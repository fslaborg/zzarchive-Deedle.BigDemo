#I "../../"
#r "packages/FSharp.Azure.StorageTypeProvider/lib/net40/Microsoft.WindowsAzure.Storage.dll"
#r "packages/FSharp.Azure.StorageTypeProvider/lib/net40/FSharp.Azure.StorageTypeProvider.dll"
#load "packages/FsLab/FsLab.fsx"
#load "utils/credentials.fsx"
#load "utils/mbrace.fsx"

open System
open System.IO
open Credentials
open MBrace
open MBrace.Flow
open MBrace.Azure
open MBrace.Core
open FSharp.Data
open Deedle
open XPlot.GoogleCharts
open FSharp.Azure.StorageTypeProvider
open FSharp.Azure.StorageTypeProvider.Table

type AzureStore = AzureTypeProvider<Credentials.storageConnection>

// ------------------------------------------------------------------------------------------------
// Step 1: Download WDC & IVE stock prices from kibot and store the data in CSV files in Azure 
// ------------------------------------------------------------------------------------------------

let cluster = Config.GetCluster()

cluster.AttachLogger(ConsoleLogger())
cluster.ShowProcesses()
cluster.ShowWorkers()

/// Download text file from the specified URL and save it (in 100k line chunks)
/// into file storage as "temp/<id>_<chunk>.csv"; the log shows last downloaded line
let downloadPrices id url = cloud { 
  let st = Http.RequestStream(url)
  let sr = new StreamReader(st.ResponseStream)
  
  let writeBatch index (batch:list<string>) = cloud { 
    let dt = DateTime.Now
    let! _ = CloudFile.WriteAllLines(sprintf "temp/%s_%d.csv" id index, List.rev batch)
    do! Cloud.Logf "Last line: %s" batch.Head }

  let rec loop index batch batchSize = cloud {
    match sr.ReadLine() with
    | null -> do! writeBatch index batch
    | line ->
        if batchSize > 100000 then 
          do! writeBatch index batch
          return! loop (index + 1) [line] 0
        else 
          return! loop index (line::batch) (batchSize + 1)  }

  return! loop 0 [] 0 }


// Download WDC and IVE tick bid/ask data from kibot
let wdc = "http://api.kibot.com/?action=history&symbol=WDC&interval=tickbidask&bp=1&user=guest"
let d1 = downloadPrices "wdc" wdc |> cluster.CreateProcess
d1.Status

let ive = "http://api.kibot.com/?action=history&symbol=IVE&interval=tickbidask&bp=1&user=guest"
let d2 = downloadPrices "ive" ive |> cluster.CreateProcess
d2.Status

// Print the last message written to the log
for log in d1.GetLogs() do
  printfn "[%s] %s" (log.DateTime.ToString("T")) log.Message


// ------------------------------------------------------------------------------------------------
// Step 2: Create empty tables for storing the stock prices in Azure
// ------------------------------------------------------------------------------------------------

open Microsoft.WindowsAzure.Storage

let storageAccount = CloudStorageAccount.Parse(Credentials.storageConnection)
let tableClient = storageAccount.CreateCloudTableClient()
let t1 = tableClient.GetTableReference("WDC")
t1.CreateIfNotExists()
let t2 = tableClient.GetTableReference("IVE")
t2.CreateIfNotExists()

// ------------------------------------------------------------------------------------------------
// Step 3: Write stock prices from the temp CSV files to the table storage
// ------------------------------------------------------------------------------------------------

let [<Literal>] schema = 
  "Date (date), Time (date), Price (decimal), " + 
  "Bid (decimal option), Ask (decimal option), Size (int)"
type Stocks = CsvProvider<Schema=schema, HasHeaders=false>

type Row = 
  { Offset : int; Ask : decimal; 
    Bid : decimal; Price : decimal; Count : int }

/// Read a row of prices and create a tuple with partition key, row key
/// and data of type `Row` to be inserted into Azure table storage
let parseRow (dt:DateTimeOffset) (row:Stocks.Row) =
  let partKey = sprintf "%04i-%02i-%02i" dt.Year dt.Month dt.Day
  let rowKey = string dt.UtcTicks
  let ask, bid = defaultArg row.Ask row.Price, defaultArg row.Bid row.Price
  let vals = 
    { Ask = ask; Bid = bid; Price = row.Price; 
      Count = row.Size; Offset = int dt.Offset.TotalMinutes }
  Partition partKey, Row rowKey, vals


/// Read the CSV files downloaded in Step 1 and write the prices to the specAzure
/// Azure table. The CSV often contains multiple records for the same time (because
/// the precision is just in seconds). We want to use date+time as the row key and so
/// we "correct" the time by adding ticks to rows that have the same time as previous
let writePrices (table:AzureTable) id = cloud { 

  /// Write a batch of entries parsed using `parseRow`
  let writeBatch (batch:list<_ * _ * Row>) = cloud { 
    let dt = DateTime.Now
    let res = table.Insert(batch, TableInsertMode.Upsert)    

    // Write any errors that may have happened to a log 
    // file 'temp/log.txt' (which can be read on the client)
    let parts = 
      [ for (Partition p, _, _) in batch -> p ]
      |> Seq.distinct |> String.concat ", "
    let inslog = 
      [ yield sprintf "Stored batch for '%s' of length '%d' for partitions: %s" id batch.Length parts
        for p, r in res do 
          for a in r do 
            match a with SuccessfulResponse _ -> () | a -> yield sprintf "%A" a ]
    let! lines = CloudFile.ReadAllLines("temp/log.txt")
    do! CloudFile.WriteAllLines("temp/log.txt", Seq.append lines inslog) |> Cloud.Ignore }


  /// Iterates over all lines in all temp CSV files, 
  /// corrects their time to be unique & saves them in batches
  let rec loop timeLast timeOffs batch files lines = cloud {
    match files, lines with
    | [], [] -> do! writeBatch batch
    | file::files, [] ->
        if not (List.isEmpty batch) then do! writeBatch batch
        let! lines = CloudFile.ReadAllLines(file)
        return! loop timeLast timeOffs [] files (List.ofSeq lines)
    | files, line::lines ->
        let row = Stocks.ParseRows(line).[0]
        let date = DateTimeOffset(row.Date + (row.Time - row.Time.Date))
        let date, timeLast, timeOffs = 
          if date <> timeLast then date, date, 0L
          else date.AddTicks(timeOffs + 1L), timeLast, timeOffs + 1L        
        let newRows = parseRow date row
        return! loop timeLast timeOffs (newRows::batch) files lines }

  // Read CSV files for the specified 'id' & process them!
  let! tempFiles = CloudFile.Enumerate("temp") 
  let matching = 
    [ for f in tempFiles do
        if f.Path.Contains(id) then yield f.Path ]
    |> List.sortBy (fun f -> int (f.Split('_', '.').[1]))
  return! loop DateTimeOffset.MinValue 0L [] matching [] }


// Create an empty log file before starting the download 
CloudFile.WriteAllLines("temp/log.txt", ["Starting..."]) |> cluster.Run

// Save data into Azure Tables! Note - this currently takes ages (hours) to
// complete. Start with IVE which runs faster. I'm not sure why. Perhaps 
// doing this in parallel would be better, but perhaps not.
let w1 = writePrices AzureStore.Tables.WDC "ive" |> cluster.CreateProcess
let w2 = writePrices AzureStore.Tables.IVE "wdc" |> cluster.CreateProcess
w1.Status
w1.Cancel()
w1.AwaitResult()
w2.Status

// Print the last message written to the log
CloudFile.ReadAllLines("temp/log.txt") |> cluster.Run


// ------------------------------------------------------------------------------------------------
// Query the data with Azure storage type provider
// ------------------------------------------------------------------------------------------------

// Check data in the first and the last partition for IVE
AzureStore.Tables.IVE.Query()
  .``Where Partition Key Is``.``Equal To``("2009-09-28").Execute()
AzureStore.Tables.IVE.Query()
  .``Where Partition Key Is``.``Equal To``("2015-07-01").Execute()


// Check data in the first and the last partition for WDC
AzureStore.Tables.WDC.Query()
  .``Where Partition Key Is``.``Equal To``("2009-09-28").Execute()
AzureStore.Tables.WDC.Query()
  .``Where Partition Key Is``.``Greater Than``("2015-07-01").Execute()


/// Do a quick check of the saved data in the Azure table. For every year in the 
/// data set & for every month, count data in the first working day partition
let logInfo () = 
  cloud {
    let! _ = CloudFile.WriteAllLines("temp/info.txt", ["Starting..."]) 
    for y in [2009 .. 2015] do
      for m in [1 .. 12] do
        let d = [1 .. 7] |> Seq.find (fun d -> 
          let w = int (DateTime(y, m, d).DayOfWeek) in w <> 0 && w <> 6)
        let pt = y.ToString() + "-" + m.ToString().PadLeft(2, '0') + "-" + d.ToString().PadLeft(2, '0')
        let l = AzureStore.Tables.WDC.Query().``Where Partition Key Is``.``Equal To``(pt).Execute().Length
      
        let! lines = CloudFile.ReadAllLines("temp/info.txt") 
        let newLines = Seq.append [sprintf "%s (length %d)" pt l] lines
        do! CloudFile.WriteAllLines("temp/info.txt", newLines) |> Cloud.Ignore }

// Start the process, wait until it completes & read the file
let info = logInfo () |> cluster.CreateProcess
info.Status
CloudFile.ReadAllLines("temp/info.txt") |> cluster.Run
