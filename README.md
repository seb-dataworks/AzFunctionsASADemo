# Azure Functions to Azure Stream Analytics samples
Sample code on how to connect Azure Stream Analytics with Azure Functions.

The samples are working for their scenario but are meant only as starting points. They have not been tested for scalability and are not safe-guared against all possible exceptions. Please be aware of this before using this code in anything near production.

# Data flow
Stream analytics sends the event data as a JSON array of one or more events in the following form:

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
