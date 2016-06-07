#load "../../packages/FsLab/Themes/AtomChester.fsx"
#load "../../packages/FsLab/FsLab.fsx"
#r "../Deedle.BigSources/bin/Debug/Deedle.BigSources.dll"

open System
open Deedle
open XPlot.GoogleCharts
open XPlot.GoogleCharts.Deedle

// ------------------------------------------------------------------------------------------------
// DEMO #1: 
// ------------------------------------------------------------------------------------------------

/// Returns DateTimeOffset for the specified date and time in GMT
let dt (y, m, d) =
  DateTimeOffset(DateTime(y, m, d), TimeSpan.FromMinutes(0.0))

// Get the WDC and IVE data frames with all values
let ive = BigDeedle.Houses.GetFrame()

