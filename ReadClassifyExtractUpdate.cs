using System;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Azure;
using Azure.Core;
using Azure.Identity;
using Azure.Storage.Blobs;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace FastFunction
{
    public class ReadClassifyExtractUpdate
    {
        private static readonly HttpClient Http = new HttpClient();
        private readonly ILogger _logger;

        public ReadClassifyExtractUpdate(ILoggerFactory loggerFactory)
        {
            _logger = loggerFactory.CreateLogger<ReadClassifyExtractUpdate>();
        }

        [Function("ReadClassifyExtractUpdate")]
        public async Task<HttpResponseData> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get")] HttpRequestData req,
            FunctionContext context)
        {
            var diag = new JObject
            {
                ["timestampUtc"] = DateTime.UtcNow.ToString("o")
            };

            // -------------------------------------------------------
            // CONFIG – ENVIRONMENT VARIABLES
            // -------------------------------------------------------
            string databricksHost   = Environment.GetEnvironmentVariable("DATABRICKS_HOST")   ?? "";
            string warehouseId      = Environment.GetEnvironmentVariable("DATABRICKS_WAREHOUSE_ID") ?? "";
            string databricksScope  = Environment.GetEnvironmentVariable("DATABRICKS_SCOPE") ?? "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default";

            string catalog          = Environment.GetEnvironmentVariable("CATALOG_NAME") ?? "eqdev";
            string schema           = Environment.GetEnvironmentVariable("SCHEMA_NAME")  ?? "edw";
            string table            = Environment.GetEnvironmentVariable("TABLE_NAME")   ?? "email_all_intake";
            string selectColumn     = Environment.GetEnvironmentVariable("SELECT_COLUMN") ?? "email_intake_id";
            string pathColumn       = Environment.GetEnvironmentVariable("PATH_COLUMN")   ?? "redacted_email_body_path";
            string updateColumn     = Environment.GetEnvironmentVariable("UPDATE_COLUMN") ?? "email_type";

            string aoaiEndpoint     = Environment.GetEnvironmentVariable("AOAI_ENDPOINT")   ?? "";
            string aoaiDeployment   = Environment.GetEnvironmentVariable("AOAI_DEPLOYMENT") ?? "";
            string aoaiKey          = Environment.GetEnvironmentVariable("OPENAI_API_KEY")  ?? "";

            diag["config"] = new JObject
            {
                ["databricksHost"] = databricksHost,
                ["warehouseId"]    = warehouseId,
                ["catalog"]        = catalog,
                ["schema"]         = schema,
                ["table"]          = table
            };

            // -------------------------------------------------------
            // INPUT – selectValue
            // -------------------------------------------------------
            var qs = System.Web.HttpUtility.ParseQueryString(req.Url.Query ?? "");
            string selectValueRaw = qs.Get("selectValue") ?? "";
            if (string.IsNullOrWhiteSpace(selectValueRaw))
                return CreateError(req, HttpStatusCode.BadRequest, "missing_param", "selectValue is required", diag);

            diag["selectValue"] = selectValueRaw;
            bool isNumeric = long.TryParse(selectValueRaw, out long selectNum);

            // -------------------------------------------------------
            // AUTH – DefaultAzureCredential or AzureCliCredential
            // -------------------------------------------------------
            TokenCredential credential;
            try
            {
                bool useCli = Environment.GetEnvironmentVariable("USE_CLI_CREDENTIAL") == "1";
                credential = useCli ? new AzureCliCredential() : new DefaultAzureCredential();
            }
            catch (Exception ex)
            {
                return CreateError(req, HttpStatusCode.InternalServerError, "credential_init_failed", ex.Message, diag);
            }

            // -------------------------------------------------------
            // DATABRICKS TOKEN (AAD)
            // -------------------------------------------------------
            string dbToken;
            try
            {
                var tk = await credential.GetTokenAsync(
                    new TokenRequestContext(new[] { databricksScope }),
                    CancellationToken.None);

                dbToken = tk.Token;
                diag["db_token_expires"] = tk.ExpiresOn.UtcDateTime.ToString("o");

                Http.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", dbToken);
            }
            catch (Exception ex)
            {
                return CreateError(req, HttpStatusCode.Unauthorized, "token_error", ex.Message, diag);
            }

            // -------------------------------------------------------
            // Helper – Submit SQL + Poll Status
            // -------------------------------------------------------
            async Task<JObject> SubmitAndPollSql(string sql, string tag)
            {
                var payload = new JObject
                {
                    ["statement"]   = sql,
                    ["warehouse_id"] = warehouseId
                };

                var submitUrl = databricksHost.TrimEnd('/') + "/api/2.0/sql/statements";
                diag[$"{tag}_submit_url"] = submitUrl;

                HttpResponseMessage submitResp;
                string submitBody;

                try
                {
                    submitResp = await Http.PostAsync(
                        submitUrl,
                        new StringContent(payload.ToString(Formatting.None), Encoding.UTF8, "application/json"));

                    submitBody = await submitResp.Content.ReadAsStringAsync();
                }
                catch (Exception ex)
                {
                    diag[$"{tag}_submit_exception"] = ex.Message;
                    return new JObject
                    {
                        ["status"] = new JObject
                        {
                            ["state"] = "FAILED",
                            ["error"] = new JObject { ["message"] = ex.Message }
                        }
                    };
                }

                diag[$"{tag}_submit_http"] = (int)submitResp.StatusCode;
                diag[$"{tag}_submit_body_preview"] = submitBody.Length > 3000
                    ? submitBody.Substring(0, 3000) + "..."
                    : submitBody;

                if (!submitResp.IsSuccessStatusCode)
                {
                    try { return JObject.Parse(submitBody); }
                    catch
                    {
                        return new JObject
                        {
                            ["status"] = new JObject
                            {
                                ["state"] = "FAILED",
                                ["error"] = submitBody
                            }
                        };
                    }
                }

                var submitJson = JObject.Parse(submitBody);
                string sid = submitJson.Value<string>("statement_id") ?? submitJson.Value<string>("id") ?? "";
                diag[$"{tag}_statement_id"] = sid;

                if (string.IsNullOrEmpty(sid))
                    return submitJson;

                string statusUrl = databricksHost.TrimEnd('/') + "/api/2.0/sql/statements/" + sid;

                for (int i = 0; i < 25; i++)
                {
                    await Task.Delay(500);

                    HttpResponseMessage statusResp;
                    string statusBody;
                    try
                    {
                        statusResp = await Http.GetAsync(statusUrl);
                        statusBody = await statusResp.Content.ReadAsStringAsync();
                    }
                    catch (Exception ex)
                    {
                        diag[$"{tag}_poll_exception"] = ex.Message;
                        continue;
                    }

                    diag[$"{tag}_status_http_last"] = (int)statusResp.StatusCode;
                    diag[$"{tag}_status_body_preview_last"] = statusBody.Length > 3000
                        ? statusBody.Substring(0, 3000) + "..."
                        : statusBody;

                    JObject parsed;
                    try { parsed = JObject.Parse(statusBody); }
                    catch
                    {
                        continue;
                    }

                    var state = parsed.SelectToken("status.state")?.ToString();
                    if (state == "SUCCEEDED" || state == "FAILED")
                    {
                        parsed["polled_state"] = state;
                        return parsed;
                    }
                }

                return new JObject
                {
                    ["status"] = new JObject { ["state"] = "TIMEOUT" }
                };
            }

            // -------------------------------------------------------
            // STEP 1 – SELECT ROW FROM DATABRICKS
            // -------------------------------------------------------
            string where = isNumeric
                ? $"{selectColumn} = {selectNum}"
                : $"{selectColumn} = '{EscapeSql(selectValueRaw)}'";

            string selectSql = $"SELECT * FROM {catalog}.{schema}.{table} WHERE {where} LIMIT 1";
            diag["select_sql"] = selectSql;

            JObject selectResult = await SubmitAndPollSql(selectSql, "select");
            string selectState = selectResult.SelectToken("status.state")?.ToString() ?? "UNKNOWN";

            if (selectState == "FAILED")
            {
                string em = selectResult.SelectToken("status.error.message")?.ToString() ?? selectResult.ToString();
                return CreateError(req, HttpStatusCode.BadRequest, "select_failed", em, diag);
            }

            // -------------------------------------------------------
            // Extract path column value
            // -------------------------------------------------------
            string pathValue = ExtractColumnValue(selectResult, pathColumn);
            diag["pathValue"] = pathValue ?? "<null>";

            if (string.IsNullOrWhiteSpace(pathValue))
                return CreateError(req, HttpStatusCode.BadRequest, "path_missing", $"Column {pathColumn} is empty or missing", diag);

            // -------------------------------------------------------
            // STEP 2 – PARSE BLOB PATH (FULL URL OR RELATIVE)
            // -------------------------------------------------------
            string blobAccountUrl;
            string containerName;
            string relativePath;

            if (pathValue.StartsWith("http", StringComparison.OrdinalIgnoreCase))
            {
                // Full HTTPS URL
                var uri = new Uri(pathValue);
                blobAccountUrl = $"{uri.Scheme}://{uri.Host}";

                var segments = uri.AbsolutePath.Trim('/').Split('/', 2);
                containerName = segments[0];
                relativePath = segments.Length > 1 ? segments[1] : "";
            }
            else
            {
                // account/container/relative  OR  container/relative (fallback to STORAGE_ACCOUNT_NAME)
                var parts = pathValue.Split('/', StringSplitOptions.RemoveEmptyEntries);
                if (parts.Length >= 3)
                {
                    blobAccountUrl = $"https://{parts[0]}.blob.core.windows.net";
                    containerName = parts[1];
                    relativePath = string.Join('/', parts, 2, parts.Length - 2);
                }
                else if (parts.Length >= 2)
                {
                    string accountName = Environment.GetEnvironmentVariable("STORAGE_ACCOUNT_NAME") ?? "";
                    if (string.IsNullOrEmpty(accountName))
                        return CreateError(req, HttpStatusCode.BadRequest, "storage_account_missing", "Cannot infer storage account for path", diag);

                    blobAccountUrl = $"https://{accountName}.blob.core.windows.net";
                    containerName = parts[0];
                    relativePath = string.Join('/', parts, 1, parts.Length - 1);
                }
                else
                {
                    return CreateError(req, HttpStatusCode.BadRequest, "invalid_path_format", "Blob path has too few segments", diag);
                }
            }

            diag["blobAccountUrl"] = blobAccountUrl;
            diag["container"]      = containerName;
            diag["relativeBlob"]   = relativePath;

            // -------------------------------------------------------
            // STEP 3 – READ BLOB CONTENT
            // -------------------------------------------------------
            string emailText;
            try
            {
                var containerUri = new Uri($"{blobAccountUrl}/{containerName}");
                var containerClient = new BlobContainerClient(containerUri, credential);
                await containerClient.GetPropertiesAsync(); // permission check

                var blobClient = containerClient.GetBlobClient(relativePath);
                using var stream = await blobClient.OpenReadAsync();
                using var reader = new StreamReader(stream, Encoding.UTF8);
                emailText = await reader.ReadToEndAsync();
                diag["blob_bytes"] = emailText.Length;
            }
            catch (RequestFailedException ex)
            {
                return CreateError(req, HttpStatusCode.BadRequest, "blob_read_failed", ex.Message, diag);
            }
            catch (Exception ex)
            {
                return CreateError(req, HttpStatusCode.BadGateway, "blob_exception", ex.Message, diag);
            }

            // -------------------------------------------------------
            // STEP 4 – CALL AOAI TO CLASSIFY
            // -------------------------------------------------------
            if (string.IsNullOrWhiteSpace(aoaiEndpoint) ||
                string.IsNullOrWhiteSpace(aoaiDeployment) ||
                string.IsNullOrWhiteSpace(aoaiKey))
            {
                return CreateError(req, HttpStatusCode.BadRequest, "model_config_missing", "AOAI_ENDPOINT/AOAI_DEPLOYMENT/OPENAI_API_KEY missing", diag);
            }

            string systemPrompt =
                "You are an email classifier. Classify the email into EXACTLY one of these categories:\n" +
                " - Quote\n - DirectBooking\n - NonQuote\n\n" +
                "Rules:\n" +
                " - Quote: The email is asking for a quote, proposal, or price estimate.\n" +
                " - DirectBooking: The email clearly wants to book or confirm a stay.\n" +
                " - NonQuote: Everything else (spam, FYI, generic info, etc.).\n\n" +
                "Return ONLY a JSON object in this form:\n" +
                "{ \"label\": \"Quote|DirectBooking|NonQuote\", \"confidence\": 0.0 }\n";

            string truncated = emailText.Length > 20000 ? emailText.Substring(0, 20000) : emailText;

            var messages = new JArray
            {
                new JObject { ["role"] = "system", ["content"] = systemPrompt },
                new JObject { ["role"] = "user",   ["content"] = truncated }
            };

            var modelRequest = new JObject
            {
                ["messages"]    = messages,
                ["max_tokens"]  = 256,
                ["temperature"] = 0.0
            };

            string label;
            double confidence = 0.0;

            try
            {
                string apiVersion = "2024-10-01-preview";
                string url = $"{aoaiEndpoint.TrimEnd('/')}/openai/deployments/{Uri.EscapeDataString(aoaiDeployment)}/chat/completions?api-version={apiVersion}";

                var msg = new HttpRequestMessage(HttpMethod.Post, url);
                msg.Headers.Add("api-key", aoaiKey);
                msg.Content = new StringContent(modelRequest.ToString(Formatting.None), Encoding.UTF8, "application/json");

                var resp = await Http.SendAsync(msg);
                string respText = await resp.Content.ReadAsStringAsync();
                diag["model_http"] = (int)resp.StatusCode;
                diag["model_preview"] = respText.Length > 3000 ? respText.Substring(0, 3000) + "..." : respText;

                if (!resp.IsSuccessStatusCode)
                    return CreateError(req, HttpStatusCode.BadGateway, "model_call_failed", respText, diag);

                var parsed = JObject.Parse(respText);
                string content = parsed.SelectToken("choices[0].message.content")?.ToString() ?? "";

                JObject resultJson;
                try { resultJson = JObject.Parse(content); }
                catch
                {
                    // try to extract JSON substring
                    int f = content.IndexOf('{');
                    int l = content.LastIndexOf('}');
                    if (f >= 0 && l > f)
                    {
                        string sub = content.Substring(f, l - f + 1);
                        resultJson = JObject.Parse(sub);
                    }
                    else
                    {
                        return CreateError(req, HttpStatusCode.BadGateway, "model_response_not_json", content, diag);
                    }
                }

                label = resultJson.Value<string>("label") ??
                        resultJson.SelectToken("classification.label")?.ToString() ??
                        "Unknown";

                var confTok = resultJson["confidence"] ?? resultJson.SelectToken("classification.confidence");
                if (confTok != null && double.TryParse(confTok.ToString(), out double c))
                    confidence = c;
            }
            catch (Exception ex)
            {
                return CreateError(req, HttpStatusCode.BadGateway, "model_exception", ex.Message, diag);
            }

            if (string.IsNullOrWhiteSpace(label))
                return CreateError(req, HttpStatusCode.BadRequest, "model_no_label", "Model did not return a label", diag);

            diag["classification_label"] = label;
            diag["classification_confidence"] = confidence;

            // -------------------------------------------------------
            // STEP 5 – UPDATE email_all_intake.email_type
            // -------------------------------------------------------
            string updateSql = isNumeric
                ? $"UPDATE {catalog}.{schema}.{table} SET {updateColumn} = '{EscapeSql(label)}' WHERE {selectColumn} = {selectNum}"
                : $"UPDATE {catalog}.{schema}.{table} SET {updateColumn} = '{EscapeSql(label)}' WHERE {selectColumn} = '{EscapeSql(selectValueRaw)}'";

            diag["update_sql"] = updateSql;

            JObject updateResult = await SubmitAndPollSql(updateSql, "update");
            string updateState = updateResult.SelectToken("status.state")?.ToString() ?? "UNKNOWN";
            diag["update_state"] = updateState;

            if (updateState == "FAILED")
            {
                string uErr = updateResult.SelectToken("status.error.message")?.ToString() ?? updateResult.ToString();
                return CreateError(req, HttpStatusCode.BadRequest, "update_failed", uErr, diag);
            }

            // -------------------------------------------------------
            // SUCCESS RESPONSE
            // -------------------------------------------------------
            var ok = req.CreateResponse(HttpStatusCode.OK);
            ok.Headers.Add("Content-Type", "application/json");

            var responseObj = new JObject
            {
                ["status"] = "Success",
                ["label"] = label,
                ["confidence"] = confidence,
                ["snippet"] = truncated.Substring(0, Math.Min(truncated.Length, 500)),
                ["diag"] = diag
            };

            await ok.WriteStringAsync(responseObj.ToString(Formatting.None));
            return ok;
        }

        // -----------------------------------------------------------
        // Helpers
        // -----------------------------------------------------------
        private static string EscapeSql(string s) => s.Replace("'", "''");

        private static string? ExtractColumnValue(JObject sqlResult, string colName)
        {
            var cols = sqlResult.SelectToken("manifest.schema.columns") as JArray;
            var data = sqlResult.SelectToken("result.data_array") as JArray;

            if (cols == null || data == null || data.Count == 0)
                return null;

            int idx = -1;
            for (int i = 0; i < cols.Count; i++)
            {
                if (string.Equals(cols[i]?["name"]?.ToString(), colName, StringComparison.OrdinalIgnoreCase))
                {
                    idx = i;
                    break;
                }
            }

            if (idx < 0) return null;

            var firstRow = data[0] as JArray;
            if (firstRow == null || firstRow.Count <= idx) return null;

            return firstRow[idx]?.ToString();
        }

        private HttpResponseData CreateError(HttpRequestData req, HttpStatusCode code, string err, string details, JObject diag)
        {
            var res = req.CreateResponse(code);
            res.Headers.Add("Content-Type", "application/json");

            var obj = new JObject
            {
                ["error"] = err,
                ["details"] = details,
                ["diagnostics"] = diag
            };

            res.WriteString(obj.ToString(Formatting.None));
            return res;
        }
    }
}
