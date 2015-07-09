#load "mbrace.fsx"

[<Literal>]
let storageConnection = "DefaultEndpointsProtocol=https;AccountName=bigdeedle;AccountKey=YRqnnp5+dnDtJTthmkdkbntkeEEcTSVvzmZiO7brVsPUb3FFg3vDnt15UHazFPVSHNY7zZS5TvFyPg3fXE7dfQ==" // "<Insert your storage connection>"
[<Literal>]
let serviceBusConnection = "Endpoint=sb://brisk-eus30565217be8e.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=WxyJ0yQeF56cZd/RNIKWK/QqW9huwYzlln3kj+T+Hk8=" // "<Insert your service bus connection>"

let config =
    { MBrace.Azure.Configuration.Default with
        StorageConnectionString = storageConnection
        ServiceBusConnectionString = serviceBusConnection }
