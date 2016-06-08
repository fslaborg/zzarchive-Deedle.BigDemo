#load "../../utils/mbrace.fsx"
open MBrace.Core
open MBrace.Azure
open MBrace.Flow
open MBrace.Azure.Management

// Create cluster & check progress until it's done
let deployment = Config.ProvisionCluster()
deployment.ShowInfo()

// Get the cluster & do some basic testing
let cluster = Config.GetCluster()

let t =
  cloud { return 1 + 1 }
  |> cluster.Run

// Display workers using Ionide formatter
cluster

// Delete the cluster for fun & profit (mostly profit)
Config.DeleteCluster()
