#r "Newtonsoft.Json"

using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Text.RegularExpressions;
using Newtonsoft.Json;

public static void Run(string queueItem, dynamic inputDocument, TraceWriter log) {
    if (inputDocument != null) {
        string apiKey = System.Environment.GetEnvironmentVariable("score_category_key", EnvironmentVariableTarget.Process);
        string apiUrl = "https://ussouthcentral.services.azureml.net/workspaces/df3f6704dc804750b90088ef0629d26e/services/95a34be7261f4bedac303f2ca67f9ec2/execute?api-version=2.0&details=true";

        string[,] scoreRequestValues = new string[,] { { inputDocument.type, inputDocument.title, inputDocument.raw_text } };

        var scoreRequest = new {
            Inputs = new Dictionary<string, StringTable> () { 
                { 
                    "input1", 
                    new StringTable() 
                    {
                        ColumnNames = new string[] {"type", "title", "raw_text"},
                        Values = scoreRequestValues
                    }
                },
            },
            GlobalParameters = new Dictionary<string, string>()
        };

        string resultJson = GetJsonFromApi(apiUrl, apiKey, scoreRequest, log);
        var result = ConvertJsonStringToObject(resultJson);

        log.Info("Success!");
        log.Info(result.ToString());
        log.Info($"Predicted Category: {result["Scored Labels"]}");

        inputDocument.SetPropertyValue("predicted_category", result);
        inputDocument.date_update = DateTime.UtcNow;
    }
}

private static string GetJsonFromApi(string apiUrl, string apiKey, object scoreRequest, TraceWriter log) {
    // The Azure ML web service has a rate limit.  If we get a 429 error, then take a break and try again
    int errorCount = 0;
    int errorWait = (new Random()).Next(150, 350);

    while (errorCount < 8) {
        using (var client = new HttpClient()) {
            client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", apiKey);
            client.BaseAddress = new Uri(apiUrl);
            
            var response = client.PostAsJsonAsync("", scoreRequest).Result;

            if (response.IsSuccessStatusCode) {
                return response.Content.ReadAsStringAsync().Result;
            }
            else {
                int statusCode = (int) response.StatusCode;
                if (statusCode == 503) {
                    // If it's a 503 then we just need to pause and try again
                    errorCount++;
                    System.Threading.Thread.Sleep(errorWait);
                    errorWait *= 2;
                }
                else {
                    // Report any other errors that come up and fail
                    log.Info($"The request failed with status code: {statusCode} {response.StatusCode}");
                    log.Info(response.Headers.ToString());
                    log.Info("");

                    throw new Exception($"The request failed with status code: {response.StatusCode}");
                }
            }
        }
    }

    // If we get here, it means we re-tried a bunch of times
    // and still couldn't get a result.  So throw an exception.
    throw new Exception($"Maximum retries exceeded.  Could not get information from Azure ML web service.");
}

public static Newtonsoft.Json.Linq.JObject ConvertJsonStringToObject(string resultJson) {
    var result = JsonConvert.DeserializeObject<dynamic>(resultJson);
    var resultValue = result.Results.output1.value;
    var output = new Newtonsoft.Json.Linq.JObject();

    for (int i = 0; i < resultValue.ColumnNames.Count; i++) {
        string columnName = resultValue.ColumnNames[i];
        string typeName = resultValue.ColumnTypes[i];
        string value = resultValue.Values[0][i];

        Newtonsoft.Json.Linq.JToken token;
        if (typeName == "Double") {
            token = Newtonsoft.Json.Linq.JToken.FromObject(double.Parse(value));
        }
        else {
            token = Newtonsoft.Json.Linq.JToken.FromObject(value);
        }

        output.Add(columnName, token);
    }

    return output;
}

public class StringTable
{
    public string[] ColumnNames { get; set; }
    public string[,] Values { get; set; }
}