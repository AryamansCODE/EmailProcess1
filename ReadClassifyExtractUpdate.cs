using System;
using System.Collections.Generic;
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
            FunctionContext ctx)
        {
            var diag = new JObject
            {
                ["timestampUtc"] = DateTime.UtcNow.ToString("o")
            };

            // ---------------- CONFIG ----------------
            string databricksHost   = Environment.GetEnvironmentVariable("DATABRICKS_HOST") ?? "";
            string warehouseId      = Environment.GetEnvironmentVariable("DATABRICKS_WAREHOUSE_ID") ?? "";
            string databricksScope  = Environment.GetEnvironmentVariable("DATABRICKS_SCOPE") ?? "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default";
            string catalog          = Environment.GetEnvironmentVariable("CATALOG_NAME") ?? "eqdev";
            string schema           = Environment.GetEnvironmentVariable("SCHEMA_NAME") ?? "edw";

            string intakeTable      = Environment.GetEnvironmentVariable("TABLE_NAME") ?? "email_all_intake";
            string quoteTable       = Environment.GetEnvironmentVariable("QUOTE_TABLE_NAME") ?? "quote_emails_stg";
            string extractedTable   = Environment.GetEnvironmentVariable("EXTRACTED_TABLE_NAME") ?? "extracted_parameters";

            string selectColumn     = Environment.GetEnvironmentVariable("SELECT_COLUMN") ?? "email_intake_id";
            string pathColumn       = Environment.GetEnvironmentVariable("PATH_COLUMN") ?? "redacted_email_body_path";
            string updateColumn     = Environment.GetEnvironmentVariable("UPDATE_COLUMN") ?? "email_type";

            string storageAccountName  = Environment.GetEnvironmentVariable("STORAGE_ACCOUNT_NAME") ?? "";
            string fallbackStorageUrl  = Environment.GetEnvironmentVariable("STORAGE_ACCOUNT_URL") ?? "";
            string fallbackContainer   = Environment.GetEnvironmentVariable("STORAGE_CONTAINER_NAME") ?? "";

            string aoaiEndpoint    = Environment.GetEnvironmentVariable("AOAI_ENDPOINT") ?? "";
            string aoaiDeployment  = Environment.GetEnvironmentVariable("AOAI_DEPLOYMENT") ?? "";
            string aoaiKey         = Environment.GetEnvironmentVariable("OPENAI_API_KEY") ?? "";

            diag["config"] = new JObject
            {
                ["databricksHost"] = databricksHost,
                ["warehouseId"]    = warehouseId,
                ["catalog"]        = catalog,
                ["schema"]         = schema,
                ["intakeTable"]    = intakeTable,
                ["quoteTable"]     = quoteTable,
                ["extractedTable"] = extractedTable
            };

            // ---------------- INPUT ----------------
            var qs = System.Web.HttpUtility.ParseQueryString(req.Url.Query);
            string selectValueRaw = qs.Get("selectValue") ?? "";
            if (string.IsNullOrWhiteSpace(selectValueRaw))
                return CreateError(req, HttpStatusCode.BadRequest, "missing_param", "selectValue query parameter is required.", diag);

            diag["selectValue"] = selectValueRaw;
            bool isNumeric = long.TryParse(selectValueRaw, out long selectNum);

            // ---------------- AUTH / TOKEN ----------------
            TokenCredential credential;
            if (Environment.GetEnvironmentVariable("USE_CLI_CREDENTIAL") == "1")
                credential = new AzureCliCredential();
            else
                credential = new DefaultAzureCredential();

            string dbToken;
            try
            {
                var tk = await credential.GetTokenAsync(
                    new TokenRequestContext(new[] { databricksScope }),
                    CancellationToken.None);

                dbToken = tk.Token;
                diag["db_token_expires"] = tk.ExpiresOn.UtcDateTime.ToString("o");
            }
            catch (Exception ex)
            {
                return CreateError(req, HttpStatusCode.Unauthorized, "token_acquire_failed", ex.Message, diag);
            }

            Http.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", dbToken);

            // Helper: submit SQL + poll
            async Task<JObject> SubmitAndPollSql(string sql, string tag)
            {
                diag[$"{tag}_submit_sql"]  = sql;
                diag[$"{tag}_submit_url"]  = $"{databricksHost.TrimEnd('/')}/api/2.0/sql/statements";

                var payload = new JObject
                {
                    ["statement"]   = sql,
                    ["warehouse_id"] = warehouseId
                };

                HttpResponseMessage submitResp;
                string submitBody;
                try
                {
                    submitResp = await Http.PostAsync(
                        $"{databricksHost.TrimEnd('/')}/api/2.0/sql/statements",
                        new StringContent(payload.ToString(), Encoding.UTF8, "application/json"));
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
                diag[$"{tag}_submit_body_preview"] = submitBody.Length > 2000
                    ? submitBody.Substring(0, 2000) + "..."
                    : submitBody;

                var submitJson = JObject.Parse(submitBody);
                string statementId = submitJson.Value<string>("statement_id") ?? submitJson.Value<string>("id");
                diag[$"{tag}_statement_id"] = statementId ?? "";

                if (string.IsNullOrEmpty(statementId))
                    return submitJson;

                for (int i = 0; i < 30; i++)
                {
                    await Task.Delay(500);
                    var statusResp = await Http.GetAsync(
                        $"{databricksHost.TrimEnd('/')}/api/2.0/sql/statements/{Uri.EscapeDataString(statementId)}");
                    var statusBody = await statusResp.Content.ReadAsStringAsync();
                    diag[$"{tag}_status_http_last"] = (int)statusResp.StatusCode;
                    diag[$"{tag}_status_body_preview_last"] = statusBody.Length > 2000
                        ? statusBody.Substring(0, 2000) + "..."
                        : statusBody;

                    var statusJson = JObject.Parse(statusBody);
                    var state = statusJson.SelectToken("status.state")?.ToString();
                    if (state == "SUCCEEDED" || state == "FAILED")
                    {
                        statusJson["polled_state"] = state;
                        return statusJson;
                    }
                }

                return new JObject
                {
                    ["status"] = new JObject { ["state"] = "TIMEOUT" }
                };
            }

            // Helper: extract column value by name
            string GetColumn(JObject sqlResult, string columnName)
            {
                var cols = sqlResult.SelectToken("manifest.schema.columns") as JArray;
                var data = sqlResult.SelectToken("result.data_array") as JArray;
                if (cols == null || data == null || data.Count == 0) return null;

                int idx = -1;
                for (int i = 0; i < cols.Count; i++)
                {
                    if (string.Equals(cols[i]!["name"]!.ToString(), columnName, StringComparison.OrdinalIgnoreCase))
                    {
                        idx = i;
                        break;
                    }
                }
                if (idx < 0) return null;

                var row = data[0] as JArray;
                if (row == null || row.Count <= idx) return null;

                return row[idx]?.ToString();
            }

            // ------------- STEP 1: SELECT INTAKE ROW -------------
            string where = isNumeric
                ? $"{selectColumn} = {selectNum}"
                : $"{selectColumn} = '{EscapeSql(selectValueRaw)}'";

            string selectSql = $"SELECT * FROM {catalog}.{schema}.{intakeTable} WHERE {where} LIMIT 1";
            diag["select_sql"] = selectSql;

            var selectStatus = await SubmitAndPollSql(selectSql, "select");
            var sState = selectStatus.SelectToken("status.state")?.ToString();
            if (sState == "FAILED")
            {
                string err = selectStatus.SelectToken("status.error.message")?.ToString() ?? selectStatus.ToString();
                return CreateError(req, HttpStatusCode.BadRequest, "select_failed", err, diag);
            }

            string pathValue      = GetColumn(selectStatus, pathColumn);
            string senderEmail    = GetColumn(selectStatus, "sender_email");
            string subject        = GetColumn(selectStatus, "subject");
            string messageId      = GetColumn(selectStatus, "message_id");
            string conversationId = GetColumn(selectStatus, "conversation_id");
            string receivedTime   = GetColumn(selectStatus, "received_time"); // TIMESTAMP
            string emailSource    = GetColumn(selectStatus, "email_source");
            string emailLanguage  = GetColumn(selectStatus, "lang_detected");

            diag["pathValue"]       = pathValue ?? "";
            diag["sender_email"]    = senderEmail ?? "";
            diag["subject"]         = subject ?? "";
            diag["message_id"]      = messageId ?? "";
            diag["conversation_id"] = conversationId ?? "";
            diag["received_time"]   = receivedTime ?? "";
            diag["email_source"]    = emailSource ?? "";
            diag["email_language"]  = emailLanguage ?? "";

            if (string.IsNullOrEmpty(pathValue))
                return CreateError(req, HttpStatusCode.BadRequest, "path_missing", "Blob path is missing in intake table.", diag);

            // ------------- STEP 2: PARSE BLOB PATH -------------
            string blobAccountUrl;
            string container;
            string blobRelative;

            if (pathValue.StartsWith("http", StringComparison.OrdinalIgnoreCase))
            {
                var uri = new Uri(pathValue);
                blobAccountUrl = $"{uri.Scheme}://{uri.Host}";
                // first segment after "/" is container
                if (uri.Segments.Length < 2)
                    return CreateError(req, HttpStatusCode.BadRequest, "invalid_blob_url", "Blob URL has no container segment.", diag);

                container = uri.Segments[1].TrimEnd('/');
                if (uri.Segments.Length > 2)
                {
                    var parts = new List<string>();
                    for (int i = 2; i < uri.Segments.Length; i++)
                        parts.Add(uri.Segments[i].Trim('/'));
                    blobRelative = string.Join("/", parts);
                }
                else
                    blobRelative = "";
            }
            else
            {
                // format: account/container/...
                var parts = pathValue.Split(new[] { '/' }, 3, StringSplitOptions.RemoveEmptyEntries);
                if (parts.Length < 3)
                    return CreateError(req, HttpStatusCode.BadRequest, "invalid_path", "Blob path does not have account/container/blob segments.", diag);

                string accountFromPath = parts[0];
                container   = parts[1];
                blobRelative = parts[2];

                blobAccountUrl = $"https://{accountFromPath}.blob.core.windows.net";
            }

            if (string.IsNullOrEmpty(blobAccountUrl))
            {
                if (!string.IsNullOrEmpty(storageAccountName))
                    blobAccountUrl = $"https://{storageAccountName}.blob.core.windows.net";
                else if (!string.IsNullOrEmpty(fallbackStorageUrl))
                    blobAccountUrl = fallbackStorageUrl.Replace(".dfs.core.windows.net", ".blob.core.windows.net").TrimEnd('/');
            }

            if (string.IsNullOrEmpty(container))
                container = fallbackContainer;

            if (string.IsNullOrEmpty(blobAccountUrl) || string.IsNullOrEmpty(container))
                return CreateError(req, HttpStatusCode.BadRequest, "storage_info_missing", "Could not resolve blob account or container.", diag);

            diag["blobAccountUrl"] = blobAccountUrl;
            diag["container"]      = container;
            diag["relativeBlob"]   = blobRelative;

            // ------------- STEP 3: READ BLOB CONTENT -------------
            string mailText;
            try
            {
                var blobClient = new BlobClient(new Uri($"{blobAccountUrl}/{container}/{blobRelative}"), credential);
                using var stream = await blobClient.OpenReadAsync();
                using var sr = new StreamReader(stream, Encoding.UTF8);
                mailText = await sr.ReadToEndAsync();
                diag["blob_bytes"] = mailText?.Length ?? 0;
            }
            catch (RequestFailedException rex)
            {
                return CreateError(req, HttpStatusCode.BadRequest, "blob_read_failed", rex.Message, diag);
            }
            catch (Exception ex)
            {
                return CreateError(req, HttpStatusCode.BadGateway, "blob_exception", ex.Message, diag);
            }

            // ------------- STEP 4: CLASSIFY EMAIL (Quote / DirectBooking / NonQuote) -------------
            if (string.IsNullOrEmpty(aoaiEndpoint) || string.IsNullOrEmpty(aoaiDeployment) || string.IsNullOrEmpty(aoaiKey))
                return CreateError(req, HttpStatusCode.BadRequest, "model_config_missing", "AOAI configuration missing.", diag);

            string classificationSystemPrompt =
                "You are an email classifier for hotel reservations.\n" +
                "Classify the email into EXACTLY one of these categories:\n" +
                "  - Quote\n" +
                "  - DirectBooking\n" +
                "  - NonQuote\n" +
                "Return ONLY JSON:\n" +
                "{ \"label\": \"Quote|DirectBooking|NonQuote\", \"confidence\": 0.0 }";

            string truncatedBody = mailText.Length > 20000 ? mailText.Substring(0, 20000) : mailText;

            var classifyReqObj = new JObject
            {
                ["messages"] = new JArray
                {
                    new JObject { ["role"] = "system", ["content"] = classificationSystemPrompt },
                    new JObject { ["role"] = "user",   ["content"] = truncatedBody }
                },
                ["max_tokens"]  = 64,
                ["temperature"] = 0.0
            };

            string label;
            double confidence = 0.0;

            try
            {
                var msg = new HttpRequestMessage(
                    HttpMethod.Post,
                    $"{aoaiEndpoint.TrimEnd('/')}/openai/deployments/{Uri.EscapeDataString(aoaiDeployment)}/chat/completions?api-version=2024-10-01-preview");
                msg.Headers.Add("api-key", aoaiKey);
                msg.Content = new StringContent(classifyReqObj.ToString(), Encoding.UTF8, "application/json");

                var resp = await Http.SendAsync(msg);
                var txt  = await resp.Content.ReadAsStringAsync();
                diag["classify_model_http"] = (int)resp.StatusCode;
                diag["classify_model_preview"] = txt.Length > 2000 ? txt.Substring(0, 2000) + "..." : txt;

                if (!resp.IsSuccessStatusCode)
                    return CreateError(req, HttpStatusCode.BadGateway, "classification_call_failed", txt, diag);

                var parsed = JObject.Parse(txt);
                var content = parsed.SelectToken("choices[0].message.content")?.ToString();
                if (string.IsNullOrEmpty(content))
                    return CreateError(req, HttpStatusCode.BadGateway, "classification_empty", txt, diag);

                JObject clsJson;
                try { clsJson = JObject.Parse(content); }
                catch
                {
                    int f = content.IndexOf('{');
                    int l = content.LastIndexOf('}');
                    if (f < 0 || l <= f)
                        return CreateError(req, HttpStatusCode.BadGateway, "classification_not_json", content, diag);
                    clsJson = JObject.Parse(content.Substring(f, l - f + 1));
                }

                label = clsJson.Value<string>("label") ?? "Unknown";
                double.TryParse(clsJson.Value<string>("confidence") ?? "0", out confidence);

                diag["classification_label"] = label;
                diag["classification_confidence"] = confidence;
            }
            catch (Exception ex)
            {
                return CreateError(req, HttpStatusCode.BadGateway, "classification_exception", ex.Message, diag);
            }

            // ------------- STEP 5: UPDATE email_all_intake.email_type -------------
            string updateIntakeSql = isNumeric
                ? $"UPDATE {catalog}.{schema}.{intakeTable} SET {updateColumn} = '{EscapeSql(label)}' WHERE {selectColumn} = {selectNum}"
                : $"UPDATE {catalog}.{schema}.{intakeTable} SET {updateColumn} = '{EscapeSql(label)}' WHERE {selectColumn} = '{EscapeSql(selectValueRaw)}'";

            var updateIntakeStatus = await SubmitAndPollSql(updateIntakeSql, "update_intake");
            var uState = updateIntakeStatus.SelectToken("status.state")?.ToString();
            diag["update_intake_state"] = uState ?? "";

            if (uState == "FAILED")
            {
                string err = updateIntakeStatus.SelectToken("status.error.message")?.ToString() ?? updateIntakeStatus.ToString();
                return CreateError(req, HttpStatusCode.BadRequest, "update_intake_failed", err, diag);
            }

            // If not Quote, we stop here
            if (!string.Equals(label, "Quote", StringComparison.OrdinalIgnoreCase))
            {
                var resp = req.CreateResponse(HttpStatusCode.OK);
                resp.Headers.Add("Content-Type", "application/json");

                var resultObj = new JObject
                {
                    ["status"]     = "Success",
                    ["label"]      = label,
                    ["confidence"] = confidence,
                    ["snippet"]    = SafeTruncate(truncatedBody, 500),
                    ["mail_raw"]   = SafeTruncate(mailText, 8000),
                    ["diag"]       = diag
                };

                await resp.WriteStringAsync(resultObj.ToString());
                return resp;
            }

            // ------------- STEP 6: UPSERT quote_emails_stg -------------
            string tsLiteral = null;
            if (!string.IsNullOrEmpty(receivedTime))
            {
                if (DateTime.TryParse(receivedTime, out var dt))
                {
                    var utc = dt.Kind == DateTimeKind.Utc ? dt : dt.ToUniversalTime();
                    tsLiteral = $"TIMESTAMP '{utc:yyyy-MM-dd HH:mm:ss}'";
                }
            }
            if (tsLiteral == null)
            {
                tsLiteral = "current_timestamp()";
            }

            string missingInfoFlag = "";
            string processingStart = "true";
            string processingEnd   = "false";
            string extractionStatusVal = "1";

            string escapedSender  = EscapeSql(senderEmail ?? "");
            string escapedSubject = EscapeSql(subject ?? "");
            string escapedMsgId   = EscapeSql(messageId ?? "");
            string escapedConvId  = EscapeSql(conversationId ?? "");
            string escapedSource  = EscapeSql(emailSource ?? "");
            string escapedPath    = EscapeSql(pathValue ?? "");
            string escapedLang    = EscapeSql(emailLanguage ?? "");

            string mergeQuoteSql = $@"
MERGE INTO {catalog}.{schema}.{quoteTable} AS target
USING (
    SELECT
        {selectValueRaw}::BIGINT           AS email_intake_id,
        '{escapedSender}'                 AS sender_email,
        '{escapedSubject}'                AS subject,
        {tsLiteral}                       AS received_time,
        '{escapedMsgId}'                  AS message_id,
        '{escapedConvId}'                 AS conversation_id,
        '{escapedSource}'                 AS source_channel,
        '{escapedPath}'                   AS raw_body_blob_path,
        '{escapedLang}'                   AS email_language,
        true                              AS processing_start,
        false                             AS processing_end,
        '{EscapeSql(extractionStatusVal)}' AS extraction_status,
        ''                                AS missing_info_flag
) AS src
ON target.email_intake_id = src.email_intake_id
WHEN MATCHED THEN
  UPDATE SET
    target.sender_email       = src.sender_email,
    target.subject            = src.subject,
    target.received_time      = src.received_time,
    target.message_id         = src.message_id,
    target.conversation_id    = src.conversation_id,
    target.source_channel     = src.source_channel,
    target.raw_body_blob_path = src.raw_body_blob_path,
    target.email_language     = src.email_language,
    target.processing_start   = src.processing_start,
    target.processing_end     = src.processing_end,
    target.extraction_status  = src.extraction_status,
    target.updated_at         = current_timestamp()
WHEN NOT MATCHED THEN
  INSERT (
    email_intake_id,
    sender_email,
    subject,
    received_time,
    message_id,
    conversation_id,
    source_channel,
    raw_body_blob_path,
    email_language,
    processing_start,
    processing_end,
    extraction_status,
    missing_info_flag,
    created_at,
    updated_at
  ) VALUES (
    src.email_intake_id,
    src.sender_email,
    src.subject,
    src.received_time,
    src.message_id,
    src.conversation_id,
    src.source_channel,
    src.raw_body_blob_path,
    src.email_language,
    src.processing_start,
    src.processing_end,
    src.extraction_status,
    src.missing_info_flag,
    current_timestamp(),
    current_timestamp()
  );
";

            var mergeQuoteStatus = await SubmitAndPollSql(mergeQuoteSql, "merge_quote");
            var mqState = mergeQuoteStatus.SelectToken("status.state")?.ToString();
            if (mqState == "FAILED")
            {
                string err = mergeQuoteStatus.SelectToken("status.error.message")?.ToString() ?? mergeQuoteStatus.ToString();
                return CreateError(req, HttpStatusCode.BadRequest, "insert_or_update_quote_failed", err, diag);
            }

            string getQuoteIdSql =
                $"SELECT quote_id FROM {catalog}.{schema}.{quoteTable} WHERE email_intake_id = {selectValueRaw} ORDER BY created_at DESC LIMIT 1";

            var getQuoteStatus = await SubmitAndPollSql(getQuoteIdSql, "get_quote_id");
            string quoteIdStr = (getQuoteStatus.SelectToken("result.data_array[0][0]") ?? getQuoteStatus.SelectToken("result.data_array[0]"))?.ToString();
            if (string.IsNullOrEmpty(quoteIdStr))
                return CreateError(req, HttpStatusCode.BadRequest, "quote_id_not_found", "Could not retrieve quote_id for this email_intake_id.", diag);

            if (!long.TryParse(quoteIdStr, out long quoteId))
                return CreateError(req, HttpStatusCode.BadRequest, "quote_id_invalid", $"Invalid quote_id value '{quoteIdStr}'.", diag);

            diag["quote_id"] = quoteId;

            // ------------- STEP 7: EXTRACT PARAMETERS (AOAI) -------------
            string extractionSystemPrompt =
                "You are an information extraction model for hotel reservation emails.\n" +
                "Return ONLY JSON with these exact fields (null if not present):\n" +
                "{\n" +
                "  \"guest_name\": string or null,\n" +
                "  \"checkin_date\": \"YYYY-MM-DD\" or null,\n" +
                "  \"checkout_date\": \"YYYY-MM-DD\" or null,\n" +
                "  \"room_type\": string or null,\n" +
                "  \"property_name\": string or null,\n" +
                "  \"rate_code\": string or null,\n" +
                "  \"signature_text\": string or null,\n" +
                "  \"number_of_adults\": integer or null,\n" +
                "  \"number_of_children\": integer or null,\n" +
                "  \"elite_mailbox_flag\": true/false or null,\n" +
                "  \"customer_status\": string or null,\n" +
                "  \"rooms_count\": integer or null,\n" +
                "  \"preferred_partner_flag\": true/false or null,\n" +
                "  \"corporate_booking_flag\": true/false or null\n" +
                "}";

            var extractReqObj = new JObject
            {
                ["messages"] = new JArray
                {
                    new JObject { ["role"] = "system", ["content"] = extractionSystemPrompt },
                    new JObject { ["role"] = "user",   ["content"] = truncatedBody }
                },
                ["max_tokens"]  = 512,
                ["temperature"] = 0.0
            };

            JObject extracted;
            try
            {
                var msg = new HttpRequestMessage(
                    HttpMethod.Post,
                    $"{aoaiEndpoint.TrimEnd('/')}/openai/deployments/{Uri.EscapeDataString(aoaiDeployment)}/chat/completions?api-version=2024-10-01-preview");
                msg.Headers.Add("api-key", aoaiKey);
                msg.Content = new StringContent(extractReqObj.ToString(), Encoding.UTF8, "application/json");

                var resp = await Http.SendAsync(msg);
                var txt  = await resp.Content.ReadAsStringAsync();
                diag["extract_model_http"] = (int)resp.StatusCode;
                diag["extract_model_preview"] = txt.Length > 2000 ? txt.Substring(0, 2000) + "..." : txt;

                if (!resp.IsSuccessStatusCode)
                    return CreateError(req, HttpStatusCode.BadGateway, "extract_call_failed", txt, diag);

                var parsed = JObject.Parse(txt);
                var content = parsed.SelectToken("choices[0].message.content")?.ToString();
                if (string.IsNullOrEmpty(content))
                    return CreateError(req, HttpStatusCode.BadGateway, "extract_empty", txt, diag);

                try { extracted = JObject.Parse(content); }
                catch
                {
                    int f = content.IndexOf('{');
                    int l = content.LastIndexOf('}');
                    if (f < 0 || l <= f)
                        return CreateError(req, HttpStatusCode.BadGateway, "extract_not_json", content, diag);
                    extracted = JObject.Parse(content.Substring(f, l - f + 1));
                }

                diag["extracted_parameters"] = extracted;
            }
            catch (Exception ex)
            {
                return CreateError(req, HttpStatusCode.BadGateway, "extract_exception", ex.Message, diag);
            }

            string guestName     = extracted.Value<string>("guest_name")     ?? "";
            string checkinDate   = extracted.Value<string>("checkin_date")   ?? "";
            string checkoutDate  = extracted.Value<string>("checkout_date")  ?? "";
            string roomType      = extracted.Value<string>("room_type")      ?? "";
            string propertyName  = extracted.Value<string>("property_name")  ?? "";
            string rateCode      = extracted.Value<string>("rate_code")      ?? "";
            string signatureText = extracted.Value<string>("signature_text") ?? "";

            int? adults   = extracted["number_of_adults"]?.Type   == JTokenType.Integer ? extracted["number_of_adults"]!.Value<int?>()   : null;
            int? children = extracted["number_of_children"]?.Type == JTokenType.Integer ? extracted["number_of_children"]!.Value<int?>() : null;
            int? rooms    = extracted["rooms_count"]?.Type        == JTokenType.Integer ? extracted["rooms_count"]!.Value<int?>()        : null;

            bool? eliteMailboxFlag       = extracted["elite_mailbox_flag"]?.Type       == JTokenType.Boolean ? extracted["elite_mailbox_flag"]!.Value<bool?>()       : null;
            bool? preferredPartnerFlag   = extracted["preferred_partner_flag"]?.Type   == JTokenType.Boolean ? extracted["preferred_partner_flag"]!.Value<bool?>()   : null;
            bool? corporateBookingFlag   = extracted["corporate_booking_flag"]?.Type   == JTokenType.Boolean ? extracted["corporate_booking_flag"]!.Value<bool?>()   : null;
            string customerStatus        = extracted.Value<string>("customer_status") ?? "";

            var missing = new List<string>();
            if (string.IsNullOrWhiteSpace(checkinDate))  missing.Add("checkin_date");
            if (string.IsNullOrWhiteSpace(checkoutDate)) missing.Add("checkout_date");
            if (string.IsNullOrWhiteSpace(propertyName)) missing.Add("property_name");

            string missingInfoJoined = string.Join(",", missing);
            diag["missing_info_flag"] = missingInfoJoined;

            string updateQuoteMissingSql = $@"
UPDATE {catalog}.{schema}.{quoteTable}
SET
  missing_info_flag = '{EscapeSql(missingInfoJoined)}',
  extraction_status = '1',
  updated_at        = current_timestamp()
WHERE quote_id = {quoteId};
";
            var updateMissingStatus = await SubmitAndPollSql(updateQuoteMissingSql, "update_quote_missing");
            var umState = updateMissingStatus.SelectToken("status.state")?.ToString();
            if (umState == "FAILED")
            {
                string err = updateMissingStatus.SelectToken("status.error.message")?.ToString() ?? updateMissingStatus.ToString();
                return CreateError(req, HttpStatusCode.BadRequest, "update_quote_missing_failed", err, diag);
            }

            string checkinLiteral  = string.IsNullOrWhiteSpace(checkinDate)  ? "NULL" : $"DATE '{EscapeSql(checkinDate)}'";
            string checkoutLiteral = string.IsNullOrWhiteSpace(checkoutDate) ? "NULL" : $"DATE '{EscapeSql(checkoutDate)}'";

            string guestEsc        = EscapeSql(guestName);
            string roomTypeEsc     = EscapeSql(roomType);
            string propNameEsc     = EscapeSql(propertyName);
            string rateCodeEsc     = EscapeSql(rateCode);
            string sigEsc          = EscapeSql(signatureText);
            string custStatusEsc   = EscapeSql(customerStatus);

            string adultsSql   = adults.HasValue   ? adults.Value.ToString()   : "NULL";
            string childrenSql = children.HasValue ? children.Value.ToString() : "NULL";
            string roomsSql    = rooms.HasValue    ? rooms.Value.ToString()    : "NULL";

            string eliteSql      = eliteMailboxFlag.HasValue     ? (eliteMailboxFlag.Value ? "true" : "false") : "NULL";
            string prefPartnerSql= preferredPartnerFlag.HasValue ? (preferredPartnerFlag.Value ? "true" : "false") : "NULL";
            string corpBookSql   = corporateBookingFlag.HasValue ? (corporateBookingFlag.Value ? "true" : "false") : "NULL";

            string mergeExtractSql = $@"
MERGE INTO {catalog}.{schema}.{extractedTable} AS target
USING (
    SELECT
        {quoteId}::BIGINT      AS quote_id,
        '{guestEsc}'           AS guest_name,
        {checkinLiteral}       AS checkin_date,
        {checkoutLiteral}      AS checkout_date,
        '{roomTypeEsc}'        AS room_type,
        '{propNameEsc}'        AS property_name,
        '{rateCodeEsc}'        AS rate_code,
        '{sigEsc}'             AS signature_text,
        {adultsSql}            AS number_of_adults,
        {childrenSql}          AS number_of_children,
        {roomsSql}             AS rooms_count,
        {eliteSql}             AS elite_mailbox_flag,
        '{custStatusEsc}'      AS customer_status,
        {prefPartnerSql}       AS preferred_partner_flag,
        {corpBookSql}          AS corporate_booking_flag
) AS src
ON target.quote_id = src.quote_id
WHEN MATCHED THEN
  UPDATE SET
    target.guest_name        = src.guest_name,
    target.checkin_date      = src.checkin_date,
    target.checkout_date     = src.checkout_date,
    target.room_type         = src.room_type,
    target.property_name     = src.property_name,
    target.rate_code         = src.rate_code,
    target.signature_text    = src.signature_text,
    target.number_of_adults  = src.number_of_adults,
    target.number_of_children= src.number_of_children,
    target.rooms_count       = src.rooms_count,
    target.updated_at        = current_timestamp()
WHEN NOT MATCHED THEN
  INSERT (
    quote_id,
    guest_name,
    checkin_date,
    checkout_date,
    room_type,
    property_name,
    rate_code,
    signature_text,
    number_of_adults,
    number_of_children,
    rooms_count,
    elite_mailbox_flag,
    customer_status,
    preferred_partner_flag,
    corporate_booking_flag,
    created_at,
    updated_at
  ) VALUES (
    src.quote_id,
    src.guest_name,
    src.checkin_date,
    src.checkout_date,
    src.room_type,
    src.property_name,
    src.rate_code,
    src.signature_text,
    src.number_of_adults,
    src.number_of_children,
    src.rooms_count,
    src.elite_mailbox_flag,
    src.customer_status,
    src.preferred_partner_flag,
    src.corporate_booking_flag,
    current_timestamp(),
    current_timestamp()
  );
";

            var mergeExtractStatus = await SubmitAndPollSql(mergeExtractSql, "merge_extracted");
            var meState = mergeExtractStatus.SelectToken("status.state")?.ToString();
            if (meState == "FAILED")
            {
                string err = mergeExtractStatus.SelectToken("status.error.message")?.ToString() ?? mergeExtractStatus.ToString();
                return CreateError(req, HttpStatusCode.BadRequest, "insert_or_update_extracted_failed", err, diag);
            }

            var ok = req.CreateResponse(HttpStatusCode.OK);
            ok.Headers.Add("Content-Type", "application/json");

            var outObj = new JObject
            {
                ["status"]          = "Success",
                ["label"]           = label,
                ["confidence"]      = confidence,
                ["quote_id"]        = quoteId,
                ["missing_info_flag"] = missingInfoJoined,
                ["snippet"]         = SafeTruncate(truncatedBody, 500),
                ["mail_raw"]        = SafeTruncate(mailText, 8000),
                ["diag"]            = diag
            };

            await ok.WriteStringAsync(outObj.ToString());
            return ok;
        }

        private static string EscapeSql(string s) => (s ?? string.Empty).Replace("'", "''");

        private static string SafeTruncate(string s, int max)
        {
            if (string.IsNullOrEmpty(s)) return s ?? "";
            return s.Length <= max ? s : s.Substring(0, max);
        }

        private HttpResponseData CreateError(HttpRequestData req, HttpStatusCode statusCode, string code, string details, JObject diag)
        {
            var res = req.CreateResponse(statusCode);
            res.Headers.Add("Content-Type", "application/json");
            var obj = new JObject
            {
                ["error"] = code,
                ["details"] = details,
                ["diagnostics"] = diag
            };
            res.WriteString(obj.ToString());
            return res;
        }
    }
}
