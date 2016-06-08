module BigDeedle.Trades

// ------------------------------------------------------------------------------------------------
// Provides acess to trades for IVE and WDC stored in Azure Table Storage
// ------------------------------------------------------------------------------------------------

open System
open BigDeedle.Core
open FSharp.Azure.StorageTypeProvider
open Deedle
open Deedle.Virtual
open Deedle.Vectors.Virtual

/// Provides access to partitions in the Azure table storage and caches the specified
/// number of partitions. The cache is indexed by `ID * Partition`. Partitions contain
/// arrays of date and values.
module private PartitionCache =
  let mutable loggingEnabled = false

  type AzureStore = AzureTypeProvider<Credentials.storageConnection>

  let private cacheSize = 20 

  let private cache = 
    System.Collections.Concurrent.ConcurrentDictionary<_, int64 * _>()
  
  let private cleanupCache () =
    if cache.Count > cacheSize then
      let drop = cache.Count - cacheSize
      let dropKeys = 
        cache 
        |> Seq.map (fun (KeyValue(k, (ts, _))) -> k, ts) 
        |> Seq.sortBy snd |> Seq.take drop |> List.ofSeq
      dropKeys |> Seq.iter (fst >> cache.TryRemove >> ignore)

  let get (id:string) part = 
    let id = id.ToLower()
    match cache.TryGetValue((id, part)) with
    | true, (_, res) -> res
    | false, _ -> 
        cleanupCache()
        if loggingEnabled then printfn "Downloading %s: %s" id part
        let inline asDate rk ro = DateTimeOffset(int64 rk, TimeSpan.FromMinutes(float ro))
        let res = 
          match id with
          | "wdc" ->
              AzureStore.Tables.WDC.Query().``Where Partition Key Is``.``Equal To``(part).Execute()  
              |> Array.map (fun row -> 
                { RowKey = int64 row.RowKey; Date = asDate row.RowKey row.Offset; Values = row.Values })
          | "ive" ->
              AzureStore.Tables.IVE.Query().``Where Partition Key Is``.``Equal To``(part).Execute()  
              |> Array.map (fun row -> 
                { RowKey = int64 row.RowKey; Date = asDate row.RowKey row.Offset; Values = row.Values })
          | _ -> failwith "Only 'ive' and 'wdc' are valid time series IDs!"
        cache.[(id, part)] <- (DateTime.UtcNow.Ticks, res)
        res

/// Enable logging of partition downloads
let EnableLogging() = PartitionCache.loggingEnabled <- true

/// Disable logging of partition downloads
let DisableLogging() = PartitionCache.loggingEnabled <- false

/// Create a time series for the specified ID and Column (Bid/Ask/Price)
let GetSeries id column = 
  let ranges = getCompleteRanges (2009s, 09y, 28y) (2015s, 07y, 1y) PartitionCache.get id
  let idxSrc = getIndexSource PartitionCache.get id ranges
  let valSrc = getValueSource PartitionCache.get Convert.ToDouble id column ranges
  Virtual.CreateSeries(idxSrc, valSrc)

/// Create frame for the specified ID with all three columns
let GetFrame id = 
  let ranges = getCompleteRanges (2009s, 09y, 28y) (2015s, 07y, 1y) PartitionCache.get id
  let idxSrc = getIndexSource PartitionCache.get id ranges
  let valSrcs = 
    [ getValueSource PartitionCache.get Convert.ToDouble id "Price" ranges :> IVirtualVectorSource
      getValueSource PartitionCache.get Convert.ToDouble id "Bid" ranges :> IVirtualVectorSource
      getValueSource PartitionCache.get Convert.ToDouble id "Ask" ranges :> IVirtualVectorSource ]
  Virtual.CreateFrame(idxSrc, ["Price";"Bid";"Ask"], valSrcs)
