# Azure Functions to Azure Stream Analytics samples
Sample code on how to connect Azure Stream Analytics with Azure Functions.

Currently two samples:
- InfluxDbWriter: Write event data schemaless to an InfluxDB time series database
- MySqlWriter: Insert events into a MySQL database. Schema needs to be adhered to in the ASA statement

The samples are working for their scenario but are meant only as starting points. They have not been tested for scalability and are not safe-guared against all possible exceptions. Please be aware of this before using this code in anything near production.

# Data flow
Stream analytics sends the event data as a JSON array of one or more events in the following form (`time` is case of the InfluxDbWriter a special value. Please see the code comments) is :
```
[
  {
    "key1":"value1",
    "key2":"value2",
    "time":"2017-09-04T17:51:02.7986986Z"
  },
  {
    "key1":"value3",
    "key2":"value4",
    "time":"2017-09-04T17:51:02.7986986Z"
  }
]
```

# Function deployment
The complete solution was built using the Azure Function SDK in Visual Studio 2017. The functions are compiled to .dll files and deployed into Azure Functions.
If you want to deploy only the function code (e.g. via the Azure Portal), you need to modify the `using` statments and add required nuget directives for packages that are loaded via NuGet, for instance `InfluxDb.Collector` or `MySql.Data`.
