using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;
using System;
using MySql.Data.MySqlClient;
using System.Collections.Generic;
using Newtonsoft.Json.Linq;

namespace AzFunctionMySqlWriter
{
    public static class MySqlWriter
    {
        /// <summary>
        /// The function processes events received from an Azure Stream Analytics job and writes them into an MySql database
        /// This function assumes that the columns it receives as keys (e.g. "key1", "key2") exist with the same name and compatible data types in the MySQL table
        /// If this is not given, the function will fail.
        /// Expected input (POST body) structure as follows. 
        /// [
        ///     {
        ///         "key1":"value1",
        ///         "key2":"value2",
        ///         "time":"2017-09-04T17:51:02.7986986Z"
        ///     },
        ///     {
        ///         "key1":"value3",
        ///         "key2":"value4",
        ///         "time":"2017-09-04T17:51:02.7986986Z"
        ///     }
        /// ]
        /// </summary>
        /// <param name="req">http request</param>
        /// <param name="log">logger</param>
        /// <returns>http response</returns>
        [FunctionName("MySqlWriter")]
        public static async Task<HttpResponseMessage> Run([HttpTrigger(AuthorizationLevel.Function, "post", Route = null)]HttpRequestMessage req, TraceWriter log)
        {
            // It is recommended to move those settings into the AzFunction app settings
            // TODO: Fill in these values. 
            string connStr = "<< insert your MySql connection string here >>";
            string mysqlTableName = "<< insert your destination table name here >>";

            log.Info("MySqlWriter function received a HTTP POST request.");

            // Get request body
            dynamic dataArray = await req.Content.ReadAsAsync<object>();

            if (dataArray == null)
            {
                return req.CreateErrorResponse(HttpStatusCode.BadRequest, "No POST data received");
            }

            log.Info($"Received {dataArray.Count} message(s)");

            using (MySqlConnection conn = new MySqlConnection(connStr))
            {
                try
                {
                    conn.Open();
                    log.Info("Connection opened");

                    foreach (var element in dataArray)
                    {
                        try
                        {
                            // Dictionary to hold that data that is then being sent to InfluxDB
                            var data = new Dictionary<string, string>();

                            bool firstRow = true;

                            string sqlTextColumns = $"INSERT INTO {mysqlTableName} (";
                            string sqlTextValues = " VALUES (";

                            // Iteratre thorugh all Key-Value pairs in the event
                            foreach (dynamic row in element)
                            {
                                string name = row.Name;
                                object value = row.Value;

                                string strValue = "";
                                // If the value was parsed by the JSON deserializer as Date, we try to convert to a valid MySql date format
                                if (value != null && ((JToken)value).Type == JTokenType.Date)
                                {
                                    if (DateTime.TryParse(value.ToString(), out DateTime resDate))
                                    {
                                        strValue = resDate.ToString("yyyy-MM-dd HH:mm:ss");
                                    }
                                }
                                else
                                {
                                    // Add to dictionary as uncasted value (should be string then)
                                    strValue = string.IsNullOrEmpty(value?.ToString()) ? "" : value?.ToString();
                                }
                                data.Add(name, strValue);

                                if (!firstRow)
                                {
                                    sqlTextColumns += ",";
                                    sqlTextValues += ",";
                                }

                                // Add the key as column name and the value as value to the SQL text string
                                sqlTextColumns += $"{name}";
                                sqlTextValues += $"?{name}";

                                firstRow = false;
                            }

                            log.Info($"Parsed message: { string.Join(", ", data.Select(kv => kv.Key + " = " + kv.Value).ToArray()) }");

                            string sqlCommandText = sqlTextColumns + ")" + sqlTextValues + ")";

                            MySqlCommand command = conn.CreateCommand();
                            command.CommandText = sqlCommandText;

                            // iterate over all keys and fill the parameters
                            foreach(var kv in data)
                            {
                                command.Parameters.AddWithValue($"?{kv.Key}", kv.Value);
                            }
                            // Write single message data to mysql
                            command.ExecuteNonQuery();

                        }
                        catch (Exception e)
                        {
                            log.Error("Error during processing message. Moving on to next message", e);
                        }
                    }
                }
                catch (Exception ex)
                {
                    log.Error("Error: " + ex.Message);
                    return req.CreateResponse(HttpStatusCode.InternalServerError, "Internal server error. Please check log files");
                }
            }
            return req.CreateResponse(HttpStatusCode.NoContent);
        }
    }
}
