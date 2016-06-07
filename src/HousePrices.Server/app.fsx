#r "System.Xml.Linq.dll"
#r "packages/FAKE/tools/FakeLib.dll"
#r "packages/Suave/lib/net40/Suave.dll"
#r "packages/FSharp.Data/lib/net40/FSharp.Data.dll"
#load "packages/FSharp.Azure.StorageTypeProvider/StorageTypeProvider.fsx"
open Fake
open Suave
open System
open FSharp.Data
open Suave.Filters
open Suave.Writers
open Suave.Operators
open Microsoft.WindowsAzure.Storage

/// Get refernece to the 'data' container in a storage account
/// specifeid in `CUSTOMCONNSTR_COEFFECTLOGS_STORAGE` env var
let getDataReference () =
  let storageEnvVar = "CUSTOMCONNSTR_HOUSEPRICES_STORAGE"
  let connStr = Environment.GetEnvironmentVariable(storageEnvVar) 
  let account = CloudStorageAccount.Parse(connStr)
  let client = account.CreateCloudBlobClient()
  client.GetContainerReference("data")

// Get the 'data' container
let blob = getDataReference()

/// Return data for a specified year/month/day
let getBlobData year month day = async {
  let ms = new IO.MemoryStream()
  let ref = blob.GetDirectoryReference(string year).GetBlobReference(sprintf "%d-%02d-%02d" year month day)
  let! exists = ref.ExistsAsync() |> Async.AwaitTask
  if exists then
    do! Async.FromBeginEnd((fun (a, n) -> ref.BeginDownloadToStream(ms, a, n) :> IAsyncResult), ref.EndDownloadToStream)
    ms.Seek(0L, IO.SeekOrigin.Begin) |> ignore
    return ms.ToArray() 
  else return Array.empty }

// When we get POST request to /log, write the received 
// data to the log blob (on a single line)
let app =
  choose [
    path "/" >=> Successful.OK "Service is running..."
    pathScan "/%d-%d-%d" (fun (y,m,d) -> 
      setHeader "content-type" "text/csv" >=> fun ctx -> async {
        let! data = getBlobData y m d
        return! ctx |> Successful.ok data }) ]

// When port was specified, we start the app (in Azure), 
// otherwise we do nothing (it is hosted by 'build.fsx')
match Int32.TryParse(getBuildParam "port") with
| true, port ->
    let serverConfig =
      { Web.defaultConfig with
          logger = Logging.Loggers.saneDefaultsFor Logging.LogLevel.Warn
          bindings = [ HttpBinding.mkSimple HTTP "127.0.0.1" port ] }
    Web.startWebServer serverConfig app
| _ -> ()