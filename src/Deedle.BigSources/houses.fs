module BigDeedle.Houses

// ------------------------------------------------------------------------------------------------
// Provides acess to UK house prices accessed via simple REST API
// ------------------------------------------------------------------------------------------------

open System
open BigDeedle.Core
open Deedle
open Deedle.Virtual
open Deedle.Vectors.Virtual
open FSharp.Data

/// Provides access to partitions in the Azure table storage and caches the specified
/// number of partitions. The cache is indexed by `ID * Partition`. Partitions contain
/// arrays of date and values.
module PartitionCache =
  let inline private asDate rk ro = 
    DateTimeOffset(int64 rk, TimeSpan.FromMinutes(float ro))

  let inline private readValue k (v:string) = 
    if k = "Price" then k, box (float v) else k, box v

  let private readData (part:string) =
    let year = part.Split('-').[0]
    let data = Http.RequestString("http://houseprices-data.azurewebsites.net/" + part)
    if String.IsNullOrWhiteSpace data then [||] else
      let csv = CsvFile.Parse(data)
      [| for row in csv.Rows ->
          let date = asDate (int64 (row.Item(15))) 0L
          let values = [ for i in 0 .. 14 -> readValue (csv.Headers.Value.[i]) (row.Item(i)) ]
          { RowKey = int64 (row.Item(15)); Date = date; Values = Map.ofSeq values } |]

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
        printfn "Downloading %s: %s" id part
        let res = readData part
        cache.[(id, part)] <- (DateTime.UtcNow.Ticks, res)
        res


/// Create frame for the specified ID with all three columns
let GetFrame () = 
  let id = "housue prices"
  let columns = 
    [ "ID"; "Postcode"; "Type"; "OldNew"; "Duration"; "PAON"; "SAON"; 
      "Street"; "Locality"; "Town"; "District"; "County"; "Category"; "Status"]

  let ranges = getCompleteRanges (1995s, 01y, 01y) (2016s, 04y, 30y) PartitionCache.get id
  let idxSrc = getIndexSource PartitionCache.get id ranges
  let valSrcs = 
    [ yield getValueSource PartitionCache.get Convert.ToDouble id "Price" ranges :> IVirtualVectorSource
      for col in columns do
        yield getValueSource PartitionCache.get Convert.ToString id col ranges :> IVirtualVectorSource ]

  Virtual.CreateFrame(idxSrc, "Price"::columns, valSrcs)
