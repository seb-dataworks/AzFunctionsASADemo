using InfluxDB.Collector;
using InfluxDB.Collector.Diagnostics;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;

namespace IoTDemoFunctions
{
    public static class InfluxDbWriter
    {
        /// <summary>
        /// The function processes events received from an Azure Stream Analytics job and writes them into an InfluxDB
        /// Expected input (POST body) structure as follows. If there is a key "time" as below, this should get automatically recognized by influxdb as the timestamp
        /// [
        ///     {
        ///         "key1":"value1",
        ///         "key2":"value2",
        ///         "time":"2017-09-04T17:51:02.7986986Z"
        ///     },
        ///     {
        ///         "key3":"value3",
        ///         "key4":"value4",
        ///         "time":"2017-09-04T17:51:02.7986986Z"
        ///     }
        /// ]
        /// </summary>
        /// <param name="req">http request</param>
        /// <param name="log">logger</param>
        /// <returns>http response</returns>
        [FunctionName("InfluxDbWriter")]
        public static async Task<HttpResponseMessage> Run([HttpTrigger(AuthorizationLevel.Function, "post", Route = null)]HttpRequestMessage req, TraceWriter log)
        {
            // It is recommended to move those settings into the AzFunction app settings. 
            // E.g.: string influxdburi = ConfigurationManager.AppSettings["InfluxDbUri"];
            // TODO: Fill in these values. You could remove username and password if you are using an open InfluxDB
            string influxdburi = "https://yourinfluxdb:8086";
            string influxdbdatabasename = "your_influx_db_name";
            string influxdbmeasurement = "measurement_name";
            string influxdbusername = "influx_username";
            string influxdbpassword = "influx_password";


            // Ignoring SSL certificate errors since for the demo we were using a self-signed cert to secure the InfluxDb
            // TODO: remove this if you are using valid SSL cert
            ServicePointManager.ServerCertificateValidationCallback += (sender, certificate, chain, sslPolicyErrors) => true;

            log.Info("InfluxDbWriter function received a HTTP POST request.");

            // Get request body
            dynamic dataArray = await req.Content.ReadAsAsync<object>();

            if (dataArray == null)
            {
                return req.CreateErrorResponse(HttpStatusCode.BadRequest, "No POST data received");
            }

            log.Info($"Received {dataArray.Count} message(s)");

            bool error = false;

            // Register InfluxDB collector error handler. Will fire if for example no connection to the db can be establised
            CollectorLog.RegisterErrorHandler((message, exception) =>
            {
                log.Error($"{message}: {exception}");
                error = true;
            });

            log.Info($"Opening connection to InfluxDb at {influxdburi} with database {influxdbdatabasename}. Data will flow into measurement {influxdbmeasurement}");

            // Create influx collector (connection to the database)
            Metrics.Collector = new CollectorConfiguration()
            .Batch.AtInterval(TimeSpan.FromSeconds(2))
            .WriteTo.InfluxDB(serverBaseAddress: influxdburi, database: influxdbdatabasename, username: influxdbusername, password: influxdbpassword)
            .CreateCollector();

            int writtenMessageCount = 0;

            // Iterate through all events
            foreach (var element in dataArray)
            {
                try
                {
                    // Dictionary to hold that data that is then being sent to InfluxDB
                    var data = new Dictionary<string, object>();
                    
                    // Iteratre thorugh all Key-Value pairs in the event
                    foreach (dynamic row in element)
                    {
                        string name = row.Name;
                        object value = row.Value;

                        // Now we try to convert the value into a float (numeric)
                        // If this is successful, we write the float value to influxdb and it will be recognized as one. 
                        // Otherwise stick with the orignal value which is normally interpreted as a string
                        float valueFloat;
                        if(float.TryParse(value?.ToString(), out valueFloat))
                        {
                            // If we can parse the value to float, we write the float value to influxdb
                            data.Add(name, valueFloat);
                        }
                        else
                        {
                            // Add to dictionary as uncasted value (should be string then)
                            data.Add(name, value);
                        }                        
                    }

                    log.Info($"Parsed message: { string.Join(", ", data.Select(kv => kv.Key + " = " + kv.Value).ToArray()) }");

                    // Write single message data to Influx
                    Metrics.Write(influxdbmeasurement, data);
                    writtenMessageCount++;
                }
                catch (Exception e)
                {
                    log.Error("Error during processing message. Moving on to next message", e);
                }
            }


            // Close influx connection
            log.Info("Closing InfluxDB connection");
            Metrics.Close();

            if (error)
                return req.CreateErrorResponse(HttpStatusCode.InternalServerError, "Could not write to InfluxDB. Please check error log. Data might have been lost.");

            return req.CreateResponse(HttpStatusCode.OK, $"{writtenMessageCount} of total {dataArray.Count} recevied messages written to InfluxDB measurement {influxdbmeasurement}");
        }
    }
}