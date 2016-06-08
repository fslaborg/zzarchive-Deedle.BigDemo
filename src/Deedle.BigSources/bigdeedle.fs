module BigDeedle.Core

open System
open Deedle
open Deedle.Ranges
open Deedle.Virtual 
open Deedle.Addressing
open Deedle.Vectors.Virtual

// ------------------------------------------------------------------------------------------------
// Concrete data source needs to provide the following loader
// ------------------------------------------------------------------------------------------------

type Row = 
  { RowKey : int64
    Date : DateTimeOffset
    Values : Map<string, obj> }

type Loader = string -> string -> Row[]

// ------------------------------------------------------------------------------------------------
// Addressing data in Azure tables by partition & offset
// ------------------------------------------------------------------------------------------------

/// This type represents a key used for addressing items in the table. It consists of 
/// a partition (day/month/year) together with Int32 offset in the partition.
type PartitionedAddress = 
  { Year : int16 
    Month : sbyte
    Day : sbyte
    Offset : int }

/// Helpers for working with `PartitionedAddress` - most importantly, we need functions
/// for turning `PartitionedAddress` into int64 number used by Deedle for addressing.
module PartAddr =
  /// Turns `PartitionedKey` into Deedle `Address` (which is `int64<address>`)
  let keyToAddress { Year=y; Month=m; Day=d; Offset=i } : Address = 
    (int64 y <<< 48) ||| (int64 m <<< 40) ||| (int64 d <<< 32) ||| (int64 i)
    |> LanguagePrimitives.Int64WithMeasure 
  
  /// Turns Deedle `Address` (which is `int64<address>`) into `PartitionedKey` 
  let addressToKey (addr:Address) = 
    let addr = int64 addr
    { Year = int16 ((addr &&& 0xffff000000000000L) >>> 48)
      Month = sbyte ((addr &&& 0x0000ff0000000000L) >>> 40)
      Day = sbyte ((addr &&& 0x000000ff00000000L) >>> 32)
      Offset = int (addr &&& 0x00000000ffffffffL) }

  /// Generate partitions in a range specified by two `PartitionedAddress` values
  let inRange { Year=y1; Month=m1; Day=d1 } { Year=y2; Month=m2; Day=d2 } = 
    let dt1, dt2 = DateTime(int y1, int m1, int d1), DateTime(int y2, int m2, int d2)
    let days = int (dt2 - dt1).TotalDays
    let step = if days >= 0 then +1 else -1
    [| for i in 0 .. step .. days ->
         let dt = dt1.AddDays(float i)
         int16 dt.Year, sbyte dt.Month, sbyte dt.Day |]

  /// Get partition (year/month/day) of a `PartitionedAddress` value
  let inline partOfKey { Year=y; Month=m; Day=d } = y, m, d

  /// Get the next partition (year/month/day), handling end of months correctly
  let inline next (y,m,d) =
    let d = DateTime(int y, int m, int d).AddDays(1.0)
    int16 d.Year, sbyte d.Month, sbyte d.Day

  /// Get the previous partition (year/month/day), handling end of months correctly
  let inline prev (y,m,d) =
    let d = DateTime(int y, int m, int d).AddDays(-1.0)
    int16 d.Year, sbyte d.Month, sbyte d.Day

  /// Format partition (year/month/day) into YYYY-MM-DD string 
  let inline format (y,m,d) = 
    let inline pstring n v = v.ToString().PadLeft(n, '0')
    pstring 4 y + "-" + pstring 2 m + "-" + pstring 2 d


// ------------------------------------------------------------------------------------------------
// Address operations - the type `PartitionedAddressOperations` tells Deedle how to work with
// `PartitionedAddress`. Deedle uses this e.g. to find 15 first/last addresses, to find all
// addresses between two given addresses (or the distance between them) and similar.
// ------------------------------------------------------------------------------------------------


/// Implements the `IRangeKeyOperations<'TAddress>` interface for Deedle.
/// The parameters of the constructor are:
///
///  - `maxAddress` is the largest `PartitionedAddress` for the time series
///  - `getPartitionSize` returns the size of a partition specified as a string
///    in the YYYY-MM-DD format.
///
type PartitionedAddressOperations(minAddress, maxAddress, getPartitionSize) = 
  interface IRangeKeyOperations<PartitionedAddress> with

    member x.Compare(k1, k2) = 
      // This works because the record fields are in the correct order
      compare k1 k2

    member x.Distance(k1, k2) =             
      // Add "+1" for skipping to next partition and then subtract 
      // "1" at the end, because we got distance of 1 element after
      if k1 > k2 then failwith "Distance: assume dt1 <= dt2"
      let partitions = PartAddr.inRange k1 k2
      let partSizes = partitions |> Array.mapi (fun i part ->
          let partSize = getPartitionSize(PartAddr.format part)
          let lo = if i = 0 then k1.Offset else 0
          let hi = if i = partitions.Length-1 then k2.Offset else partSize-1
          int64 (hi - lo + 1))
      (Array.sum partSizes) - 1L

    member x.Range(k1, k2) =
      // Here, we need to generate range in both directions (when
      // k1 is larger, we need to produce range in 'reversed' order)
      let step = if k2 > k1 then +1 else -1 
      let partitions = PartAddr.inRange k1 k2
      partitions |> Seq.mapi (fun i (y,m,d) ->
        let partSize = getPartitionSize(PartAddr.format (y,m,d))
        let first, last = 
          // Find first & last element on the partition. This is 
          // generally 0 .. count-1 except for boundary PartAddr.
          if k2 > k1 then
            (if i = 0 then k1.Offset else 0), 
            (if i = partitions.Length-1 then k2.Offset else partSize-1)
          else 
            (if i = 0 then k1.Offset else partSize-1),
            (if i = partitions.Length-1 then k2.Offset else 0)
        seq { for i in first .. step .. last -> 
                { Year=y; Month=m; Day=d; Offset = i } }) |> Seq.concat 

    member x.IncrementBy(k, offset) =
      let first = PartAddr.partOfKey k
      let rec loop ((y,m,d) & part) offset = 
        // Increment the starting position of 'k' by the specified 'offset' by
        // iterating over partitions and adding their sizes, until we have enough
        // (or until we run out of partitions)
        if part < (minAddress.Year, minAddress.Month, minAddress.Day) ||
           part > (maxAddress.Year,maxAddress.Month,maxAddress.Day) then 
          raise (new IndexOutOfRangeException())        
        if offset >= 0L then
          let count = getPartitionSize(PartAddr.format part)
          let start = if part = first then k.Offset else 0
          if offset < int64 (count - start) then { Year = y; Month = m; Day = d; Offset = start + (int offset) }
          else loop (PartAddr.next part) (offset - int64 (count - start))        
        else 
          let count = if part = first then k.Offset else getPartitionSize(PartAddr.format part)
          if -offset < int64 count then { Year = y; Month = m; Day = d; Offset = count - 1 + (int offset) }
          else loop (PartAddr.prev part) (offset + int64 count)        
      loop first offset

    member x.ValidateKey(k, lookup) =
      // If the key is larger than max or smaller than min address...
      if lookup = Lookup.Exact then
        let count = getPartitionSize(PartAddr.format (PartAddr.partOfKey k))
        if k.Offset >= 0 && k.Offset < count then OptionalValue(k)
        else OptionalValue.Missing
      else 
        // This would only be required if we were using 'Ranges.lookup' to
        // implement `IVirtualVectorSource<'T>.LookupValue` (which we don't)
        failwith "ValidateKey: Not supported - lookup <> LookupExact"


// ------------------------------------------------------------------------------------------------
// Implementation of `IVirtualVectorSource<'T>`. This tells Deedle how to access data in the 
// time series (or data frame) and how to perform merging and slicing of series/frames. We are 
// relying on Deedle's `Ranges<'T>` type here, which handles merging/slicing and so the code 
// is not very complex. The only complex operation is lookup, which is implemented below.
// ------------------------------------------------------------------------------------------------

/// The type `PartitionedTableSource<'T>` is used with `'T = DateTimeOffset` (for indices) and
/// `'T = float` (for data vectors). This interface is implemented below for the two options.
type ITableValueSource<'T> = 
  abstract Lookup : Ranges<PartitionedAddress> * 'T * Lookup * (Address -> bool) -> OptionalValue<'T * Address>
  abstract ValueAt : PartitionedAddress -> OptionalValue<'T>


/// Represents a data source for an index or a vector of a time series stored in partitioned
/// Azure table. This takes `ITableValueSource<'T>` for actually reading the values and 
/// `Ranges<PartitionedAddress>` representing the range that we're providing access to.
type PartitionedTableSource<'T>(id, source:ITableValueSource<'T>, ranges:Ranges<PartitionedAddress>) = 
  let kta, atk = Func<_, _>(PartAddr.keyToAddress), Func<_, _>(PartAddr.addressToKey)
  let addressing = RangesAddressOperations(ranges, kta, atk) :> IAddressOperations

  /// The range of the series (this is "ranges" because it can be a 
  /// sequence of disconnected ranges as a result of slicing & merging)
  member x.Ranges = ranges

  // Implements non-generic source (boilerplate)
  interface IVirtualVectorSource with
    member x.AddressingSchemeID = id
    member x.Length = ranges.Length

    member x.ElementType = typeof<'T>
    member x.AddressOperations = addressing
    member x.Invoke(op) = op.Invoke(x)

  // Implement generic source interface - mostly boilerplate or delegation 
  // to `Ranges`, except for LookupRange which is not supported here at all
  interface IVirtualVectorSource<'T> with
    member x.MergeWith(sources) = 
      PartitionedTableSource(id, source, ranges.MergeWith(sources |> Seq.map (function
        | :? PartitionedTableSource<'T> as t -> t.Ranges
        | _ -> failwith "MergeWith: other is not partitioned table source!"))) :> _

    member x.LookupRange(value) = 
      // This can be used if we can efficiently (without full scan) build a `RangeRestriction`
      // that describes the indices of rows that have the specified `value`. We cannot
      // do this here, but we could if we had some additional index over the data (e.g.)
      failwith "LookupRange: not supported"

    member x.LookupValue(value, lookup, check) = 
      source.Lookup(ranges, value, lookup, check.Invoke)

    member x.ValueAt(loc) = 
      loc.Address |> PartAddr.addressToKey |> source.ValueAt

    member x.GetSubVector(restriction) = 
      let newRanges = ranges.Restrict(restriction |> RangeRestriction.map PartAddr.addressToKey)
      PartitionedTableSource<'T>(id, source, newRanges) :> _

       
// ------------------------------------------------------------------------------------------------
// Concrete implementation of everything we need to create Deedle series
// ------------------------------------------------------------------------------------------------

/// Creates `IVirtualVectorSource<float>` for accessing dates of a specified ID. 
/// This takes initial range (which should cover the whole series)
let getIndexSource (loader:Loader) id ranges =
  let source = 
    { new ITableValueSource<DateTimeOffset> with
      member x.Lookup(ranges, dt, lookup, check) = 
        let rec loop ((y,m,d) & part) =
          let data = loader id (PartAddr.format part)
          // If we are out of range, return <missing>. If we are out of range for
          // the current partition, try the next/previous, depending on lookup
          if (part < PartAddr.partOfKey ranges.FirstKey && (lookup &&& Lookup.Greater) <> Lookup.Greater) ||
             (part > PartAddr.partOfKey ranges.LastKey && (lookup &&& Lookup.Smaller) <> Lookup.Smaller) then
            OptionalValue.Missing
          elif (data.Length = 0 || data.[0].Date > dt) &&
            (lookup &&& Lookup.Smaller) = Lookup.Smaller then loop (PartAddr.prev part)
          elif (data.Length = 0 || data.[data.Length-1].Date < dt) &&
            (lookup &&& Lookup.Greater) = Lookup.Greater then loop (PartAddr.next part)
          elif (data.Length = 0 || data.[0].Date = dt) && lookup = Lookup.Smaller then loop (PartAddr.prev part)
          elif (data.Length = 0 || data.[data.Length-1].Date = dt) && lookup = Lookup.Greater then loop (PartAddr.next part)
          else
            // Scan the current partition using binary search function provided by Deedle
            let pk = { Year = y; Month = m; Day = d; Offset = 0 }
            let optOffset = 
              Virtual.IndexUtilsModule.binarySearch data.LongLength 
                (Func<_, _>(fun i -> data.[int i].RowKey)) dt.UtcTicks lookup 
                (Func<_, _>(fun i -> check (PartAddr.keyToAddress { pk with Offset = int i })))
            optOffset |> OptionalValue.map (fun i -> 
              data.[int i].Date, PartAddr.keyToAddress { pk with Offset = int i })

        loop (int16 dt.Year, sbyte dt.Month, sbyte dt.Day)

      member x.ValueAt(k) = 
        let part = loader id (PartAddr.format (PartAddr.partOfKey k))
        OptionalValue(part.[k.Offset].Date) }
  PartitionedTableSource<DateTimeOffset>(id, source, ranges)

/// Creates `IVirtualVectorSource<float>` for accessing specified ID and column. 
/// This takes initial range (which should cover the whole series)
let getValueSource<'T> (loader:Loader) parser id column ranges =
  let source = 
    { new ITableValueSource<'T> with
      member x.Lookup(_, _, _, _) = 
        failwith "Lookup: Not supported on value source!"
      member x.ValueAt(k) = 
        let part = loader id (PartAddr.format (PartAddr.partOfKey k))
        OptionalValue(parser(part.[k.Offset].Values.[column])) }
  PartitionedTableSource<'T>(id, source, ranges)

/// Get `Ranges<'K>` value representing the full range for a table with the specified ID
/// (This also creates `PartitionedAddressOperations` associated with ranges)
let getCompleteRanges (y1,m1,d1) (y2,m2,d2) (loader:Loader) id =
  let getPartitionSize part = (loader id part).Length
  let maxPart = loader id (PartAddr.format (y2,m2,d2))
  let minAddress = { Year = y1; Month = m1; Day = d1; Offset = 0 }
  let maxAddress = { Year = y2; Month = m2; Day = d2; Offset = maxPart.Length-1 }
  let range = [ minAddress, maxAddress]
  Ranges.create (PartitionedAddressOperations(minAddress, maxAddress, getPartitionSize)) range  
