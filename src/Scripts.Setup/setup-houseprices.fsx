#nowarn "211"
#I "../../"
#r "packages/FSharp.Azure.StorageTypeProvider/lib/net40/Microsoft.WindowsAzure.Storage.dll"
#r "packages/FSharp.Azure.StorageTypeProvider/lib/net40/FSharp.Azure.StorageTypeProvider.dll"
#load "packages/FsLab/Themes/AtomChester.fsx"
#load "packages/FsLab/FsLab.fsx"
#load "utils/credentials.fsx"
#load "utils/mbrace.fsx"

open System
open Deedle
open MBrace.Core
open MBrace.Azure
open FSharp.Data
open FSharp.Azure.StorageTypeProvider
open Microsoft.WindowsAzure.Storage.Blob

// -------------------------------------------------------------------------------------------------
// Download houuse price data and store it in Azure blobs (one per day)
// The processing is all done inside Azure cluster using MBrace
// -------------------------------------------------------------------------------------------------

// Get cluster and Azure storage connection
type HousePrices = AzureTypeProvider<Credentials.storageConnection>
let cluster = Config.GetCluster()

// Recreate Azure container (run step by step)
let container = HousePrices.Containers.CloudBlobClient.GetContainerReference("data")
container.Delete()
container.CreateIfNotExists() |> ignore
let permissions = new BlobContainerPermissions()
permissions.PublicAccess <- BlobContainerPublicAccessType.Blob


/// Get directory for a given year (we store blobs per year)
let getDirectory year =
  let container = HousePrices.Containers.CloudBlobClient.GetContainerReference("data")
  container.GetDirectoryReference(year)

/// Store house prices for a given year
let storeHousePrices year = local {
  // Download data & read it into Deedle frame
  do! Cloud.Logf "Starting data download: %d" year
  let url = sprintf "http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-%d.csv" year
  let str = Http.RequestString(url)
  do! Cloud.Logf "Downloaded data: %d" year
  let reader = new System.IO.StringReader(str)
  let frame =
    Frame.ReadCsv(reader, hasHeaders=false)
    |> Frame.indexColsWith [ "ID"; "Price"; "Date"; "Postcode"; "Type"; "OldNew";
        "Duration"; "PAON"; "SAON"; "Street"; "Locality"; "Town"; "District"; "County";
        "Category"; "Status" ]

  // Write data to blobs, partitioned by month & day
  let dir = getDirectory (string year)
  for month in [1 .. 12] do
    let monthlyFrame = frame |> Frame.filterRows (fun k row ->
      let dt = row.GetAs<DateTime>("Date")
      dt.Year = year && dt.Month = month)
    for d in [1 .. DateTime.DaysInMonth(year, month)] do
      // Get frame for a given day & make dates unique
      let dailyFrame = monthlyFrame |> Frame.filterRows (fun k row ->
        row.GetAs<DateTime>("Date").Day = d)
      let dates = dailyFrame.GetColumn<DateTime>("Date")
      let uniqueDates =
        dates
        |> Series.sort
        |> Series.scanValues (fun (i, (last:DateTime)) dt ->
            if last.Date = dt.Date then (i+1, dt.AddTicks(int64 i))
            else (1, dt) ) (1, DateTime.MinValue)
        |> Series.mapValues (fun (_, dt) -> dt.Ticks)
      dailyFrame?Date <- uniqueDates

      // Write the frame to the blob
      let sb = System.Text.StringBuilder()
      let wr = new System.IO.StringWriter(sb)
      dailyFrame.SaveCsv(wr, includeRowKeys=false)
      let blob = dir.GetBlockBlobReference(sprintf "%d-%02d-%02d" year month d)
      blob.UploadText(sb.ToString())
    do! Cloud.Logf "Finished writing blobs: %d-%02d" year month
  do! Cloud.Logf "Successfully processed year: %d" year }

// Create massive process doing all the work
let storeAll =
  [ for y in 1995 .. 1995 -> local {
      try do! storeHousePrices y
      with e -> do! Cloud.Logf "Failed writing data: %d" y } ]
  |> Cloud.Parallel
  |> cluster.CreateProcess

// Check status & logs and result once it's done
cluster

for log in storeAll.GetLogs() do
  printfn "[%s] %s" (log.DateTime.ToString("T")) log.Message

storeAll.Status
storeAll.Result
