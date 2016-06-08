BigDeedle: Large time series Azure demo
=======================================

<img align="right" src="https://github.com/BlueMountainCapital/Deedle.BigDemo/raw/master/img/bmc.png" alt="BlueMountain Capital logo" />

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

How does it work
----------------

To use BigDeedle, you need to implement a couple of interfaces that tell Deedle how
to actually access the data. In this example, we use Azure Table storage as an
example and we implement the data access using the [Azure Storage type
provider](http://fsprojects.github.io/AzureStorageTypeProvider/). To run the demo
on your machine, you first need to run setup code that creates the tables and
inserts data into them.

As an example we're using [free sample data from kibot](http://www.kibot.com/),
which gives us prices for IVE and WDC tickers for 6 years with fairly high frequency
(about 100MB and 1GB data sets, respectively). We insert them into data table as
follows:

 * **Partition key** is the date, formatted as `YYYY-MM-DD`
 * **Row key** is the UTC ticks value of the data, formatted as string
 * **Columns** are `Price`, `Bid` and `Ask` with the 3 different prices

The BigDeedle interfaces are implemented so that they only download data in partitions
that are actually needed. So for example, in the above screenshot, BigDeedle only
downloaded partitions `2009-09-28` and `2015-07-01`. The client code also caches
partitions (in memory) to avoid re-downloading them. This is a demo, so downloading
the whole partition may be slow (they can be big), but this nicely shows you what is
happening under the cover!

Running the code
----------------

Before you can build and run everything, you need to setup a few things. Note that
the Visual Studio build will actually fail until you have the required tables in
your Azure storage! That's OK - you can create those in F# Interactive without
building everything.

Before running the code, you need to download dependencies. Either run build in
Visual Studio (which fails, but still triggers Paket) or just run the command
`.paket/paket.bootstraper.exe` followed by `.paket/paket.exe restore`.

### Running house prices demos

The house price demo uses a simple Suave REST server as a data source. You can find the
source code for it in `src/HousePrices.Server`. You do not need to run it on your own, there
is a live version running at [https://houseprices-data.azurewebsites.net/](https://houseprices-data.azurewebsites.net/).

 - **Local demos** (`houses-local.fsx`) does not require any additional setup. It shows
   how to load data from the BigDeedle storage, explore the data using the FsLab formatters
   in [Ionide](http://ionide.io) and how to get a subset of the data and do local processing.
   As an example, the script draws a chart of most expensive towns in the UK in April 2010
   shown above.

### Running finance demos

To run the code, you'll need to start an MBrace cluster. Follow instructions at
[www.mbrace.io](http://mbrace.io) to do this. You'll need to save your `azure.publishsettings`
file into the `utils` folder, create a storage account and copy the connection string to
`utils/credentials.fsx` (use `utils/credentials.template` as the template for the file).
Then you need to go through the `setup-trades.fsx` script, which does the following:

 * It first downloads the CSV file with the data and saves it in chunks into
   local files in Azure storage.
 * It creates WDC and IVE tables (once that's done, you need to reopen the
   script so that Azure type provider notices the new tables)
 * It writes the data into Azure Table storage (and as it does that, it also makes
   sure that the keys are unique). Note that this is very slow. There is some
   diagnostics to help you see how far you are.

Once the setup is done, build the solution. Now, you're ready to play with the
two demo files that you find in the repository:

 - **Local demos** (`demo-local.fsx`) requires only storage connection, but not
   a running MBrace cluster. This shows how to use BigDeedle and demonstrates the
   various functions and exploratory operations that you can perform on a series or
   a frame without actually accessing all data. The demos load data on demand from the
   Azure Table via the storage connection string specified in `credentaials.fsx`.

 - **MBrace cluster demos** (`demo-cloud.fsx`) requires a running MBrace cluster
   (use [www.briskengine.com](https://www.briskengine.com/) to get one running). This
   demonstrates how to use MBrace to run the computation over BigDeedle frames and
   series in Azure compute cluster. This reduces the latency (data is available in
   the same data center) and it also lets you scale your computations over large
   number of machines and CPUs.
