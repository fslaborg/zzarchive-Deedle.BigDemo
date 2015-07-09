BigDeedle: Large time series Azure demo
=======================================

<p style="float:right;margin:20px">
  <img src="https://www.bluemountaincapital.com/media/logo.gif" alt="BlueMountain Capital logo" />
</p>

**BigDeedle** is an extension of the exploratory data frame and time-series
manipulation library called [Deedle](http://bluemountaincapital.github.io/Deedle/)/.
With BigDeedle, you can load data frames and time-series from an external data
source without fully evaluating them and without fitting all data from the
source into memory. This means that you can create data frames (and time series)
that _represent_ gigabytes of data stored somewhere else.

BigDeedle lets you easily explore the data through the normal Deedle API. It
lets you perform lookups, slicing, merging, resampling and a few other exploratory
operations over the data set without actually accessing all the data. BigDeedle
works nicely in F# Interactively - you'll see the first and last few rows from
a frame or a series.

![BigDeedle in action!](https://github.com/tpetricek/Deedle.BigDemo/raw/master/img/screen.png)
