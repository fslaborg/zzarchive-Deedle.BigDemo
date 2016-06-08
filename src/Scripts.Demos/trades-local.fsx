#I "../.."
#load "packages/FsLab/Themes/AtomChester.fsx"
#load "packages/FsLab/FsLab.fsx"
#r "src/Deedle.BigSources/bin/Debug/Deedle.BigSources.dll"

open System
open Deedle
open XPlot.GoogleCharts
open XPlot.GoogleCharts.Deedle

// ------------------------------------------------------------------------------------------------
// DEMO #1: Accessing virtual data frames and time series
// ------------------------------------------------------------------------------------------------

/// Returns DateTimeOffset for the specified date and time in GMT
let dt (y, m, d) (hh, mm, ss) =
  DateTimeOffset(DateTime(y, m, d, hh, mm, ss), TimeSpan.FromMinutes(0.0))

// Get the WDC and IVE data frames with all values
let ive = BigDeedle.Trades.GetFrame("IVE")
let wdc = BigDeedle.Trades.GetFrame("WDC")

// We can do all sorts of lookup operations to find price at a given time
let wdcPrice = wdc?Price

// Lookup price at exact/smaller/greater time point
wdcPrice.[dt (2011,10,14) (8,30,54)]
wdcPrice.Get(dt (2011,10,14) (8,30,54), Lookup.Smaller)
wdcPrice.Get(dt (2011,10,14) (8,30,54), Lookup.Greater)

// We can also get the observation (i.e. the date found)
let dt1 = dt (2012,6,1) (0,0,0)
wdcPrice.TryGetObservation(dt1, Lookup.ExactOrGreater)
wdcPrice.TryGetObservation(dt1, Lookup.ExactOrGreater)

// Or we can get a sub-series for a specified time range
let jun1 = wdcPrice.[dt1 .. dt1.AddDays(1.0)]
jun1 |> Stats.mean


// ------------------------------------------------------------------------------------------------
// DEMO #2: Calculating with virtual frames and series locally
// ------------------------------------------------------------------------------------------------


// Calculate square of differences for the whole Ask series
let avg = (wdc?Ask.[snd wdc?Ask.KeyRange] + wdc?Ask.[fst wdc?Ask.KeyRange]) / 2.0
let squareDiffs = (wdc?Ask - avg) ** 2.0

let firstDay = dt (2009, 9, 28) (0, 0, 0)
let lastDay = dt (2015, 7, 1) (0, 0, 0)
let one = TimeSpan.FromHours(24.0)

// Get square root of average difference for the first & last day
squareDiffs.[firstDay .. firstDay + one] |> Stats.mean |> sqrt
squareDiffs.[lastDay .. lastDay + one] |> Stats.mean |> sqrt

// Calculate the difference between Bid and Ask (in two ways!)
wdc?DiffOne <- wdc.Rows |> Series.map (fun _ row -> Math.Round(row?Ask - row?Bid, 2))
wdc?DiffTwo <- round ((wdc?Ask - wdc?Bid) * 100.0) / 100.0

// Average bid/ask difference in the first and last day
let diffDay1 = wdc.Rows.[.. firstDay + one]?DiffOne
let diffDay2 = wdc.Rows.[lastDay ..]?DiffOne

diffDay1 |> Stats.mean
diffDay2 |> Stats.mean

// We can look for prices accross multiple partitions (here, get prices
// for 1 June for all the years at or before 12:00pm). This is a bit slow BTW!
wdc?DiffOne
|> Series.sample [ for y in 2009 .. 2015 -> dt (y,6,1) (12,0,0) ]

// Get the average difference in one minute chunks over the first day & draw chart
diffDay1
|> Series.sampleTimeInto (TimeSpan.FromMinutes 1.0) Direction.Forward Stats.mean
|> Series.dropMissing
|> Series.mapKeys (fun dto -> (dto.DateTime - dto.Date).TotalHours)
|> Chart.Line

// Compare the prices for the first & last day, averaged per 1 minute chunks
let dayPrices =
  [ wdc.Rows.[.. firstDay + one]?Price
    wdc.Rows.[lastDay ..]?Price ]

[ for ts in dayPrices ->
    (ts - Stats.mean ts)
    |> Series.sampleTimeInto (TimeSpan.FromMinutes 1.0) Direction.Forward Stats.mean
    |> Series.dropMissing
    |> Series.mapKeys (fun dto -> (dto.DateTime - dto.Date).TotalHours) ]
|> Chart.Line
