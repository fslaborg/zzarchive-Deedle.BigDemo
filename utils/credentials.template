#load "mbrace.fsx"

[<Literal>]
let storageConnection = "<Insert your storage connection>"
[<Literal>]
let serviceBusConnection = "<Insert your service bus connection>"

let config =
    { MBrace.Azure.Configuration.Default with
        StorageConnectionString = storageConnection
        ServiceBusConnectionString = serviceBusConnection }
