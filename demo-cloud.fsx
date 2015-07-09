#load "packages/FsLab/FsLab.fsx"
#load "utils/credentials.fsx"
#load "utils/mbrace.fsx"
#r "bin/Debug/Deedle.BigDemo.dll"

open System
open MBrace
open MBrace.Core
open MBrace.Azure
open MBrace.Workflows
open MBrace.Azure.Client
open Deedle
open Credentials
open FSharp.Charting

// NOTE: You may need to restart F# Interactive before running the rest of the demos.
// (otherwise MBrace will try to copy your local BigDeedle data frames to the cloud)

// ------------------------------------------------------------------------------------------------
// DEMO #3: Doing various things with BigDeedle frames and series in the cloud
// ------------------------------------------------------------------------------------------------

let cluster = Runtime.GetHandle(config)

cluster.AttachClientLogger(ConsoleLogger())
cluster.ShowProcesses()
cluster.ShowWorkers()

// Test MBrace to check it's running fine
cloud { return 40 + 2 } |> cluster.Run

// Helpers copied from the previous demo file
let dt (y, m, d) (hh, mm, ss) = 
  DateTimeOffset(DateTime(y, m, d, hh, mm, ss), TimeSpan.FromMinutes(0.0))
let firstDay = dt (2009, 9, 28) (0, 0, 0)
let lastDay = dt (2015, 7, 1) (0, 0, 0)
let one = TimeSpan.FromHours(24.0)

/// Calculates the average price for an ID on a given day
let meanPriceOnDay id day =
  let ts = BigDeedle.GetFrame(id)?Price
  ts.[dt day (0, 0, 0) .. dt day (23,59,59)] |> Stats.mean


// Calculate the average price locally and in the cloud. Depending on your internet, 
// this might already be faster in the cloud (because data stays in the same data centre).
#time
meanPriceOnDay "WDC" (2012, 6, 1)
meanPriceOnDay "WDC" (2012, 5, 1)

cloud { return meanPriceOnDay "WDC" (2012, 6, 1) } |> cluster.Run
cloud { return meanPriceOnDay "WDC" (2012, 5, 1) } |> cluster.Run

/// Calculate returns (as percentage) over one minute chunks for a given day
let meanMinuteReturns id day = 
  let prices = BigDeedle.GetFrame(id)?Price
  let range = prices.[dt day (10,0,0) .. dt day (15,0,0)]
  let byMinute = range |> Series.sampleTimeInto (TimeSpan.FromMinutes 1.0) Direction.Forward Stats.mean
  (Series.diff 1 byMinute) / byMinute * 100.0 |> Stats.mean 

// Test this locally
meanMinuteReturns "WDC" (2012, 6, 1)

open MBrace.Flow

/// Calculate returns for all days in a given year and month
/// Here, we can nicely use MBrace's "Cloud.Ballanced.map" to scale 
/// the processing of individual days across multiple threads/machines
let returnsForMonth y m = cloud {
  let lastDay = DateTime(y,m,1).AddMonths(1).AddDays(-1.0).Day
  let! returns = 
    [ 1 .. lastDay ] 
    |> Cloud.Balanced.map (fun d ->
        let res = meanMinuteReturns "WDC" (y, m, d)
        DateTime(y,m,d), res ) 
  return series returns }

// Get the returns for a single month
let may15proc = returnsForMonth 2015 5 |> cluster.CreateProcess

// Check what is going on
may15proc.Completed
cluster.ShowWorkers()

let may15 = may15proc.AwaitResult()
may15 |> Chart.Line
  
// .. or we can use 'Cloud.Parallel' to perform the calculation for June
// months in all the years for which we have data (2010 .. 2015)
let junesProc =
  [ for y in 2010 .. 2015 -> cloud {
      let! ts = returnsForMonth y 6 
      return ts |> Series.mapKeys (fun dt -> dt.Day ) }]
  |> Cloud.Parallel
  |> cluster.CreateProcess

// Wait for the result and create a combined plot!
junesProc.Completed
cluster.ShowWorkers()

junesProc.AwaitResult() 
|> Array.map Chart.Line 
|> Chart.Combine



