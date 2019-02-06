using System;
using System.IO;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using System.Diagnostics;
using System.Threading;
using NLog;
using System.Collections.Generic;
using System.Net.Http;
using System.Linq;

namespace Microsoft.Azure.DataLake.Store
{
    /// <summary>
    /// Http layer class to send htttp requests to the server.
    /// </summary>
    internal class WebTransport
    {
        private const int MinDataSizeForCompression = 1000;
        /// <summary>
        /// Logger to log VERB, Header of requests and responses headers
        /// </summary>
        private static readonly Logger WebTransportPowershellLog = LogManager.GetLogger("adls.powershell.WebTransport");
        /// <summary>
        /// Logger to log messages related to Http transport
        /// </summary>
        private static readonly Logger WebTransportLog = LogManager.GetLogger("adls.dotnet.WebTransport");
        /// <summary>
        /// Logger to log messages related to authorization token
        /// </summary>
        private static readonly Logger TokenLog = LogManager.GetLogger("adls.dotnet.WebTransport.Token");

        /// <summary>
        /// Capacity of the stringbuilder to build the Http request URL
        /// </summary>
        private const int UrlLength = 100;
        /// <summary>
        /// Thorw an error if AuthorizationHeader is less than that
        /// </summary>
        private const int AuthorizationHeaderLengthThreshold = 10;

        private static int ErrorResponseDefaultLength = 1000;

        #region Common

        private static Stream GetCompressedStream(Stream inputStream, AdlsClient client, int postRequestLength)
        {
            if (client.WrapperStream != null && postRequestLength > MinDataSizeForCompression)
            {

                return client.WrapperStream(inputStream);

            }
            return inputStream;
        }


        /// <summary>
        /// Verifies whether the arguments for MakeCall is correct. Throws exception if any argument is null or out of range.
        /// </summary>
        /// <param name="opCode">Operation Code</param>
        /// <param name="path">Path of the file or directory</param>
        /// <param name="requestData">byte array, offset and length of the data of http request</param>
        /// <param name="quer">Headers for request</param>
        /// <param name="client">ADLS Store CLient</param>
        /// <param name="req">Request options containing RetryOption, timout and requestid </param>
        /// <param name="resp">Contains the response message </param>
        /// <returns>False if there is any errors with arguments else true</returns>
        private static bool VerifyMakeCallArguments(string opCode, string path, ByteBuffer requestData, QueryParams quer, AdlsClient client, RequestOptions req, OperationResponse resp)
        {
            //Check all type of errors and exceptions
            if (resp == null) throw new ArgumentNullException(nameof(resp)); //Check if resp is not null
            if (req == null) throw new ArgumentNullException(nameof(req)); //Check if req is not null
            if (quer == null) throw new ArgumentNullException(nameof(quer)); //Check if quer is not null
            if (client == null) throw new ArgumentNullException(nameof(client));//Check for client
            if (String.IsNullOrEmpty(client.AccountFQDN) || string.IsNullOrEmpty(client.AccountFQDN))//Check the client account
            {
                resp.IsSuccessful = false;
                resp.Error = "The client account name is missing.";
                return false;
            }
            if (!Operation.Operations.ContainsKey(opCode))
            {
                resp.IsSuccessful = false;
                resp.Error = "Operation Code doesnot exist.";
                return false;
            }
            if (String.IsNullOrEmpty(path) || string.IsNullOrEmpty(path.Trim()))//Check for path
            {
                resp.IsSuccessful = false;
                resp.Error = "The file/directory path for this operation is missing.";
                return false;
            }
            //Check for request data
            if (requestData.Data != null && (requestData.Offset >= requestData.Data.Length || (requestData.Offset < 0) || (requestData.Count + requestData.Offset > requestData.Data.Length)))
            {
                throw new ArgumentOutOfRangeException(nameof(requestData.Offset));
            }
            return true;
        }
        /// <summary>
        /// After MakeSingleCall determines whether the HTTP request was succesful. Populates the error, logs messages and update4s the latency tracker.
        /// </summary>
        /// <param name="opCode">Operation Code</param>
        /// <param name="path">Path of the file or directory</param>
        /// <param name="resp">Contains the response message </param>
        /// <param name="responseLength">Length of the response returned by the server</param>
        /// <param name="requestLength">Length of the request data</param>
        /// <param name="req">Request Options</param>
        /// <param name="client">AdlsClient</param>
        /// <param name="querParams">Serialized query parameter of the Http request</param>
        /// <param name="numRetries">Number of retries</param>
        private static void HandleMakeSingleCallResponse(string opCode, string path, OperationResponse resp, int responseLength, int requestLength, RequestOptions req, AdlsClient client, string querParams, ref int numRetries)
        {
            DetermineIsSuccessful(resp);//After recieving the response from server determine whether the response is successful
            string error = "";
            // Make sure latency header error is different from error. Latencyheader error is put in http header so there is restriction on the content characters
            string latencyHeaderError = "";
            if (!resp.IsSuccessful)
            {
                if (resp.Ex != null)
                {
                    error = resp.Ex.Message;
                    latencyHeaderError = resp.Ex.GetType().Name;
                }
                else if (!string.IsNullOrEmpty(resp.RemoteExceptionName))
                {
                    error = $"HTTP {resp.HttpStatus} ( {resp.RemoteExceptionName}  {resp.RemoteExceptionMessage} JavaClassName: {resp.RemoteExceptionJavaClassName} ";
                    latencyHeaderError = $"HTTP {resp.HttpStatus} ( {resp.RemoteExceptionName} )";
                }
                else if (!string.IsNullOrEmpty(resp.Error))
                {
                    latencyHeaderError = error = resp.Error;
                }
                //This is either unexplained exception or the remote exception returned from server
                resp.ExceptionHistory = resp.ExceptionHistory == null ? error : resp.ExceptionHistory + "," + error;
                numRetries++;
            }
            if (WebTransportLog.IsDebugEnabled)
            {
                string logLine =
                    $"HTTPRequest,{(resp.IsSuccessful ? "Succeeded" : "failed")},cReqId:{req.RequestId},lat:{resp.LastCallLatency},err:{error},Reqlen:{requestLength},Resplen:{responseLength}" +
                    $"{(resp.HttpStatus == HttpStatusCode.Unauthorized ? $",Tokenlen:{resp.AuthorizationHeaderLength}" : "")},token_ns:{resp.TokenAcquisitionLatency},sReqId:{resp.RequestId}" +
                    $",path:{path},qp:{querParams}{(!req.KeepAlive ? ",keepAlive:false" : "")}{(!req.IgnoreDip && client.DipIp != null ? $",dipIp:{client.DipIp}" : "")}, connectionfailure:{resp.ConnectionFailure}";
                WebTransportLog.Debug(logLine);
            }
            LatencyTracker.AddLatency(req.RequestId, numRetries, resp.LastCallLatency, latencyHeaderError, opCode,
                requestLength + responseLength, client.ClientId);
        }
        /// <summary>
        /// Determine whether the Http request was successful based on error or Exception or remote exception
        /// </summary>
        /// <param name="resp">OperationResponse</param>
        private static void DetermineIsSuccessful(OperationResponse resp)
        {
            if (resp.Ex != null) resp.IsSuccessful = false;
            else if (!string.IsNullOrEmpty(resp.Error)) resp.IsSuccessful = false;
            else if (!string.IsNullOrEmpty(resp.RemoteExceptionName)) resp.IsSuccessful = false;
            else if (((int)resp.HttpStatus) >= 100 && ((int)resp.HttpStatus) < 300) resp.IsSuccessful = true;
            else resp.IsSuccessful = false;
        }

        private static void PostPowershellLogDetails(HttpRequestMessage request, HttpResponseMessage response)
        {
            if (WebTransportPowershellLog.IsDebugEnabled)
            {
                string message = $"VERB: {request.Method}{Environment.NewLine}{Environment.NewLine}RequestHeaders:{Environment.NewLine}";
                bool firstHeader = true;
                foreach (var requestHeader in request.Headers)
                {
                    if (requestHeader.Key.Equals("Authorization"))
                    {
                        message += (!firstHeader ? Environment.NewLine : "") +
                                   $"[AuthorizationHeaderLength:{requestHeader.Value.FirstOrDefault().Length}]";
                    }
                    else
                    {
                        message += (!firstHeader ? Environment.NewLine : "") +
                                   $"[{requestHeader.Key}:{requestHeader.Value.FirstOrDefault()}]";
                    }

                    firstHeader = false;
                }
                message += $"{Environment.NewLine}{Environment.NewLine}";
                message += $"ResponseStatus:{response.StatusCode}{Environment.NewLine}{Environment.NewLine}ResponseHeaders:{Environment.NewLine}";
                firstHeader = true;
                foreach (var responseHeader in response.Headers)
                {
                    message += (!firstHeader ? Environment.NewLine : "") + $"[{responseHeader.Key}:{responseHeader.Value.FirstOrDefault()}]";
                    firstHeader = false;
                }

                message += $"{Environment.NewLine}{Environment.NewLine}";
                WebTransportPowershellLog.Debug(message);
            }
        }

        /// <summary>
        /// Serializes the client FQDN, queryparams and token into a request URL
        /// </summary>
        /// <param name="op">Operation</param>
        /// <param name="path">Path of directory or file</param>
        /// <param name="client">AdlsClient</param>
        /// <param name="resp">OperationResponse</param>
        /// <param name="queryParams">Serialized queryparams</param>
        /// <param name="clientReqOptions">Request options</param>
        /// <returns>URL</returns>
        private static string CreateHttpRequestUrl(Operation op, string path, AdlsClient client, OperationResponse resp, string queryParams, RequestOptions clientReqOptions)
        {
            StringBuilder urlString = new StringBuilder(UrlLength);
            urlString.Append(client.GetHttpPrefix());
            urlString.Append("://");
            // If dip ip is set and ignore dip is not specified then request needs to go to dip ip
            if (client.DipIp != null && !clientReqOptions.IgnoreDip)
            {
                urlString.Append(client.DipIp);
            }
            else
            {
                urlString.Append(client.AccountFQDN);
            }

            urlString.Append(op.Namespace);
            // This is to prevent badly formed requests for uris not having a preceding /
            if (path[0] != '/')
            {
                urlString.Append(Uri.EscapeDataString("/"));
            }

            try
            {
                urlString.Append(Uri.EscapeDataString(path));
            }
            catch (UriFormatException ex)
            {
                resp.Ex = ex;
                return null;
            }
            urlString.Append("?");
            urlString.Append(queryParams);
            try
            {
                var uri = new Uri(urlString.ToString());
            }
            catch (UriFormatException ur)
            {
                resp.Ex = ur;
                return null;
            }
            return urlString.ToString();
        }

        private static CancellationTokenSource GetCancellationTokenSourceForTimeout(RequestOptions req)
        {
            var cts = new CancellationTokenSource();
            cts.CancelAfter(req.TimeOut);
            return cts;
        }

        /// <summary>
        /// Sets the WebRequest headers
        /// </summary>
        /// <param name="webReq">HttpWebRequest</param>
        /// <param name="client">AdlsClient</param>
        /// <param name="req">RequestOptions</param>
        /// <param name="token">Auth token</param>
        /// <param name="customHeaders">Custom headers</param>
        private static void AssignCommonHttpHeaders(HttpRequestMessage webReq, AdlsClient client, RequestOptions req, string token, IDictionary<string, string> customHeaders, int postRequestLength)
        {
            webReq.Headers.TryAddWithoutValidation("Authorization", token);
            string latencyHeader = LatencyTracker.GetLatency();
            if (!string.IsNullOrEmpty(latencyHeader))
            {
                webReq.Headers.TryAddWithoutValidation("x-ms-adl-client-latency", latencyHeader);
            }

            if (client.ContentEncoding != null && postRequestLength > MinDataSizeForCompression)
            {
                webReq.Headers.TryAddWithoutValidation("Content-Encoding", client.ContentEncoding);

            }

            if (client.DipIp != null && !req.IgnoreDip)
            {
                webReq.Headers.TryAddWithoutValidation("Host", client.AccountFQDN);
            }

            if (!req.KeepAlive)
            {
                webReq.Headers.TryAddWithoutValidation("Connection", "Close");
            }

            if (customHeaders != null)
            {
                string contentType;
                if (customHeaders.TryGetValue("Content-Type", out contentType))
                {
                    webReq.Content.Headers.TryAddWithoutValidation("Content-Type", contentType);
                    customHeaders.Remove("Content-Type");
                }
                foreach (var key in customHeaders.Keys)
                {
                    webReq.Headers.TryAddWithoutValidation(key, customHeaders[key]);
                }
            }
            webReq.Headers.TryAddWithoutValidation("User-Agent", client.GetUserAgent());
            webReq.Headers.TryAddWithoutValidation("x-ms-client-request-id", req.RequestId);
        }
        
        /// <summary>
        /// Verifies the responseData for the operation and initializes it if the encoding is chunked
        /// </summary>
        /// <param name="webResponse">HttpWebResponse</param>
        /// <param name="responseData">ResponseData structure</param>
        /// <param name="isResponseError">True when we are initializing error response stream else false</param>
        /// <returns>False if the response is not chunked but the content length is 0 else true</returns>
        private static bool InitializeResponseData(HttpResponseMessage webResponse, ref ByteBuffer responseData, bool isResponseError = false)
        {
            string encoding = null;
            if (webResponse.Headers.Contains("Transfer-Encoding"))
            {
                encoding = webResponse.Headers.GetValues("Transfer-Encoding").FirstOrDefault();
            }
            if (!string.IsNullOrEmpty(encoding) && encoding.Equals("chunked"))
            {
                // If the error response is from our FE, then it wont be chunked. If the error is from IIS
                // then it may be chunked. So assign a default size of the error response. Even if the remote error 
                // is not contained in that buffer size, its fine.
                if (isResponseError)
                {
                    responseData.Data = new byte[ErrorResponseDefaultLength];
                    responseData.Count = ErrorResponseDefaultLength;
                    responseData.Offset = 0;
                }
                //If it is chunked responseData should be instantiated and responseDataLength should be greater than 0, because we dont know the content length
                if (responseData.Data == null)
                {
                    throw new ArgumentNullException(nameof(responseData.Data));
                }
                if (responseData.Offset >= responseData.Data.Length || responseData.Offset < 0 ||
                    responseData.Count + responseData.Offset > responseData.Data.Length)
                {
                    throw new ArgumentOutOfRangeException(nameof(responseData.Offset));
                }
            }
            else
            {
                //Initialize the response based on content length property
                if (responseData.Data == null) //For OPEN operation the data might not be chunked
                {
                    if (webResponse.Content.Headers.ContentLength != null && webResponse.Content.Headers.ContentLength.Value > 0)
                    {
                        responseData.Data = new byte[webResponse.Content.Headers.ContentLength.Value];
                        responseData.Offset = 0;
                        responseData.Count = (int)webResponse.Content.Headers.ContentLength.Value;
                    }
                    else
                    {
                        return false;
                    }
                }
            }
            return true;
        }
        /// <summary>
        /// Handles WebException. Determines whether it is due to cancelled operation, remoteexception from server or some other webexception
        /// </summary>
        /// <param name="e">WebException instance</param>
        /// <param name="resp">OperationResponse</param>
        /// <param name="path">Path</param>
        /// <param name="requestId">Request Id</param>
        /// <param name="token">Auth token</param>
        /// <param name="webReq">Http Web request</param>
        /// <param name="cancelToken">Cancel Token</param>
        private static void HandleWebException(HttpResponseMessage e, OperationResponse resp, string path, string requestId, string token, HttpRequestMessage webReq, CancellationToken cancelToken = default(CancellationToken))
        {
            try
            {

                if (resp.HttpStatus == HttpStatusCode.Unauthorized && TokenLog.IsDebugEnabled)
                {
                    string tokenLogLine =
                        $"HTTPRequest,HTTP401,cReqId:{requestId},sReqId:{resp.RequestId},path:{path},token:{token}";
                    TokenLog.Debug(tokenLogLine);
                }

                ByteBuffer errorResponseData = default(ByteBuffer);
                if (!InitializeResponseData(e, ref errorResponseData, true))
                {
                    throw new ArgumentException("ContentLength of error response stream is not set");
                }
                using (Stream errorStream = e.Content.ReadAsStreamAsync().GetAwaiter().GetResult())
                {
                    // Reading the data from the error response into a byte array is necessary to show the actual error data as a part of the 
                    // error message in case JSON parsing does not work. We read the bytes and then pass it back to JsonTextReader using a memorystream
                    int noBytes;
                    int totalLengthToRead = errorResponseData.Count;
                    do
                    {
                        noBytes = errorStream.Read(errorResponseData.Data, errorResponseData.Offset, totalLengthToRead);
                        errorResponseData.Offset += noBytes;
                        totalLengthToRead -= noBytes;

                    } while (noBytes > 0 && totalLengthToRead > 0);
                    string contenttype = null;
                    if (e.Content.Headers.Contains("Content-Type"))
                    {
                        contenttype = e.Content.Headers.GetValues("Content-Type").FirstOrDefault();
                    }

                    ParseRemoteError(errorResponseData.Data, errorResponseData.Count, resp, contenttype);
                }

            }
            catch (Exception ex)
            {
                resp.Ex = ex;
            }
        }
        /// <summary>
        /// Parses RemoteException and populates the remote error fields in OperationResponse
        /// </summary>
        /// <param name="errorBytes">Error Response bytes</param>
        /// <param name="errorBytesLength">Error response bytes length</param>
        /// <param name="resp">Response instance</param>
        /// <param name="contentType">Content Type</param>
        private static void ParseRemoteError(byte[] errorBytes, int errorBytesLength, OperationResponse resp, string contentType)
        {
            try
            {
                using (MemoryStream errorStream = new MemoryStream(errorBytes, 0, errorBytesLength))
                {
                    using (StreamReader stReader = new StreamReader(errorStream))
                    {

                        using (var jsonReader = new JsonTextReader(stReader))
                        {

                            jsonReader.Read(); //StartObject {
                            jsonReader.Read(); //"RemoteException"
                            if (jsonReader.Value == null || !((string)jsonReader.Value).Equals("RemoteException"))
                            {
                                throw new IOException(
                                    $"Unexpected type of exception in JSON error output. Expected: RemoteException Actual: {jsonReader.Value}");
                            }

                            jsonReader.Read(); //StartObject {
                            do
                            {
                                jsonReader.Read();
                                if (jsonReader.TokenType.Equals(JsonToken.PropertyName))
                                {

                                    switch ((string)jsonReader.Value)
                                    {
                                        case "exception":
                                            jsonReader.Read();
                                            resp.RemoteExceptionName = (string)jsonReader.Value;
                                            break;
                                        case "message":
                                            jsonReader.Read();
                                            resp.RemoteExceptionMessage = (string)jsonReader.Value;
                                            break;
                                        case "javaClassName":
                                            jsonReader.Read();
                                            resp.RemoteExceptionJavaClassName = (string)jsonReader.Value;
                                            break;
                                    }
                                }

                            } while (!jsonReader.TokenType.Equals(JsonToken.EndObject));

                        }
                    }
                }
            }
            catch (Exception e)
            {
                resp.Ex = e;
                //Store the actual remote response in a separate variable, since response can have illegal charcaters which will throw exception while setting them to headers
                resp.RemoteErrorNonJsonResponse = $" Content-Type of error response: {contentType}. Error: {Encoding.UTF8.GetString(errorBytes, 0, errorBytesLength)}";
            }
        }
        #endregion

        #region Async
        /// <summary>
        /// Calls the API that makes the HTTP request to the server. Retries the HTTP request in certain cases. This is a asynchronous call.
        /// </summary>
        /// <param name="opCode">Operation Code</param>
        /// <param name="path">Path of the file or directory</param>
        /// <param name="requestData">byte array, offset and length of the data of http request</param>
        /// <param name="responseData">byte array, offset and length of the data of http response. byte array should be initialized for chunked response</param>
        /// <param name="quer">Headers for request</param>
        /// <param name="client">ADLS Store CLient</param>
        /// <param name="req">Request options containing RetryOption, timout and requestid </param>
        /// <param name="resp">Contains the response message </param>
        /// <param name="cancelToken">CancellationToken to cancel the operation</param>
        /// <param name="customHeaders">Dictionary containing the custom header that Core wants to pass</param>
        /// <returns>Tuple of Byte array containing the bytes returned from the server and number of bytes read from server</returns>
        internal static async Task<Tuple<byte[], int>> MakeCallAsync(string opCode, string path,
            ByteBuffer requestData, ByteBuffer responseData, QueryParams quer, AdlsClient client, RequestOptions req, OperationResponse resp, CancellationToken cancelToken, IDictionary<string, string> customHeaders = null)
        {
            if (!VerifyMakeCallArguments(opCode, path, requestData, quer, client, req, resp))
            {
                return null;
            }
            string uuid = req.RequestId;
            int numRetries = 0;
            Tuple<byte[], int> retVal;
            do
            {
                resp.Reset();
                req.RequestId = uuid + "." + numRetries;
                resp.Retries = numRetries;
                Stopwatch watch = Stopwatch.StartNew();
                retVal = await MakeSingleCallAsync(opCode, path, requestData, responseData, quer, client, req, resp, cancelToken, customHeaders).ConfigureAwait(false);
                watch.Stop();
                resp.LastCallLatency = watch.ElapsedMilliseconds;
                HandleMakeSingleCallResponse(opCode, path, resp, retVal?.Item2 ?? 0, requestData.Count, req, client, quer.Serialize(opCode), ref numRetries);
                if (cancelToken.IsCancellationRequested)//Operation is cancelled then no retries
                {
                    break;
                }
                // If dip is used, this request is not ignoring dip, and there is a connection failure, then reset the DIP
                if (client.DipIp != null && !req.IgnoreDip && resp.ConnectionFailure)
                {
                    await client.UpdateDipAsync(cancelToken).ConfigureAwait(false);
                }

            } while (!resp.IsSuccessful && req.RetryOption.ShouldRetry((int)resp.HttpStatus, resp.Ex));
            resp.OpCode = opCode;
            return retVal;
        }

        /// <summary>
        /// Makes a single Http call to the server, sends the request and obtains the response. This is a asynchronous call.
        /// </summary>
        /// <param name="opCode">Operation Code</param>
        /// <param name="path">Path of the file or directory</param>
        /// <param name="requestData">byte array, offset and length of the data of http request</param>
        /// <param name="responseData">byte array, offset and length of the data of http response. byte array should be initialized for chunked response</param>
        /// <param name="qp">Headers for request</param>
        /// <param name="client">ADLS Store CLient</param>
        /// <param name="req">Request options containing RetryOption, timout and requestid </param>
        /// <param name="resp">Contains the response message </param>
        /// <param name="cancelToken">CancellationToken to cancel the operation</param>
        /// <param name="customHeaders">Dictionary containing the custom header that Core wants to pass</param>
        /// <returns>Tuple of Byte array containing the bytes returned from the server and number of bytes read from server</returns>
        private static async Task<Tuple<byte[], int>> MakeSingleCallAsync(string opCode, string path,
            ByteBuffer requestData, ByteBuffer responseData, QueryParams qp, AdlsClient client, RequestOptions req, OperationResponse resp, CancellationToken cancelToken, IDictionary<string, string> customHeaders)
        {
            string token = null;
            Operation op = Operation.Operations[opCode];
            string urlString = CreateHttpRequestUrl(op, path, client, resp, qp.Serialize(opCode), req);
            if (string.IsNullOrEmpty(urlString))
            {
                return null;
            }

            if (client.WrapperStream != null && string.IsNullOrEmpty(client.ContentEncoding))
            {
                resp.Error = "WrapperStream is set, but Encoding string is not set";
                return null;
            }

            if (client.WrapperStream == null && !string.IsNullOrEmpty(client.ContentEncoding))
            {
                resp.Error = "Encoding string is set, but WrapperStream is not set";
                return null;
            }

            try
            {
                // Create does not throw WebException
                using (HttpRequestMessage webReq = new HttpRequestMessage(new HttpMethod(op.Method), urlString))
                {

                    //If operation is cancelled then stop
                    cancelToken.ThrowIfCancellationRequested();
                    // If security certificate is used then no need to pass token
                    var httpClient = client.GetHttpClient(req.UseCert);
                    Stream postStream;

                    Stopwatch watch = Stopwatch.StartNew();
                    token = await client.GetTokenAsync(cancelToken).ConfigureAwait(false);
                    watch.Stop();
                    resp.TokenAcquisitionLatency = watch.ElapsedMilliseconds;
                    if (string.IsNullOrEmpty(token))
                    {
                        resp.Ex = new ArgumentException($"Token is null or empty.");
                        return null;
                    }

                    if (token.Length <= AuthorizationHeaderLengthThreshold)
                    {
                        resp.Ex = new ArgumentException($"Token Length is {token.Length}. Token is most probably malformed.");
                        return null;
                    }

                    resp.AuthorizationHeaderLength = token.Length;
                    if (!op.Method.Equals("GET"))
                    {
                        if (op.RequiresBody && requestData.Data != null)
                        {
                            postStream = GetCompressedStream(new MemoryStream(requestData.Data, requestData.Offset, requestData.Count), client, requestData.Count);
                            webReq.Content = new StreamContent(postStream);
                        }
                        else
                        {
                            postStream = new MemoryStream();
                        }
                        webReq.Content = new StreamContent(postStream);
                    }
                    AssignCommonHttpHeaders(webReq, client, req, token, customHeaders, requestData.Count);
                    using (var timeoutCancellationTokenSource = GetCancellationTokenSourceForTimeout(req))
                    {
                        using (CancellationTokenSource linkedCts =
                      CancellationTokenSource.CreateLinkedTokenSource(cancelToken, timeoutCancellationTokenSource.Token))
                        {
                            try
                            {
                                using (var webResponse = op.ReturnsBody ? await httpClient.SendAsync(webReq, HttpCompletionOption.ResponseHeadersRead, linkedCts.Token).ConfigureAwait(false) : await httpClient.SendAsync(webReq, linkedCts.Token).ConfigureAwait(false))
                                {
                                    resp.HttpStatus = webResponse.StatusCode;
                                    resp.HttpMessage = webResponse.ReasonPhrase;
                                    if (webResponse.Headers.Contains("x-ms-request-id"))
                                    {
                                        resp.RequestId = webResponse.Headers.GetValues("x-ms-request-id").FirstOrDefault();
                                    }
                                    PostPowershellLogDetails(webReq, webResponse);
                                    if ((int)resp.HttpStatus >= 300)
                                    {
                                        HandleWebException(webResponse, resp, path, req.RequestId, token, webReq);
                                        return null;
                                    }
                                    if (op.ReturnsBody)
                                    {

                                        if (!InitializeResponseData(webResponse, ref responseData))
                                        {
                                            return null;
                                        }

                                        int totalBytes = 0;
                                        try
                                        {
                                            using (Stream opStream = await webResponse.Content.ReadAsStreamAsync().ConfigureAwait(false))
                                            {

                                                int noBytes;
                                                int totalLengthToRead = responseData.Count;
                                                //Read the required amount of data. In case of chunked it is what users requested, else it is amount of data sent
                                                // to be only used for net452 opStream.ReadTimeout = 60 * 1000;
                                                do
                                                {

                                                    noBytes = await opStream.ReadAsync(responseData.Data, responseData.Offset,
                                                        totalLengthToRead, linkedCts.Token).ConfigureAwait(false);
                                                    totalBytes += noBytes;
                                                    responseData.Offset += noBytes;
                                                    totalLengthToRead -= noBytes;

                                                } while (noBytes > 0 && totalLengthToRead > 0);



                                            }
                                        }
                                        catch (IOException e)
                                        {
                                            resp.Ex = e;
                                            resp.ConnectionFailure = true;
                                            return null;
                                        }
                                        return
                                            Tuple.Create(responseData.Data,
                                                totalBytes); //Return the total bytes read also since in case of chunked amount of data returned can be less than data returned
                                    }
                                }
                            }
                            catch (HttpRequestException e)
                            {
                                resp.Ex = e;
                                resp.ConnectionFailure = true;
                            }
                            catch (OperationCanceledException e)
                            {
                                if (timeoutCancellationTokenSource.Token.IsCancellationRequested)
                                {
                                    resp.Ex = new Exception($"Operation timed out after {req.TimeOut}");
                                    // Its good to set it true for timeout
                                    resp.ConnectionFailure = true;
                                }
                                else if (cancelToken.IsCancellationRequested)
                                {
                                    resp.Ex = e;
                                }
                            }
                        }
                    }
                }
            }// Any unhandled exception is caught here
            catch (Exception e)
            {
                resp.Ex = cancelToken.IsCancellationRequested ? new OperationCanceledException(cancelToken) : e;

            }

            return null;
        }

        #endregion
    }
}