using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using System.Diagnostics;
using System.Threading;
using NLog;
using System.Collections.Generic;

namespace Microsoft.Azure.DataLake.Store
{
    /// <summary>
    /// Http layer class to send http requests to the server using HttpClient. We have both async and sync MakeCall. The reason we have that is if the application is doing some operations using explicit threads,
    /// then using async-await internally creates unecessary tasks for worker threads in threadpool. Application can create explicit threads in cases of uploader and downloader.
    /// </summary>
    internal class WebTransport
    {
        private const int MinDataSizeForCompression = 1000;
        
        /// <summary>
        /// Shared HttpClient instance for connection pooling and performance
        /// </summary>
        private static readonly HttpClient _httpClient;
        
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
        /// Throw an error if AuthorizationHeader is less than that
        /// </summary>
        private const int AuthorizationHeaderLengthThreshold = 10;

        private static int ErrorResponseDefaultLength = 1000;
        /// <summary>
        /// This contains list of custom headers that are not directly copied
        /// </summary>
        private static HashSet<string> HeadersNotToBeCopied = new HashSet<string> {"Content-Type"};

        /// <summary>
        /// Static constructor to initialize HttpClient once
        /// </summary>
        static WebTransport()
        {
            var handler = new HttpClientHandler
            {
                AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate,
                AllowAutoRedirect = false,
                UseCookies = false
            };

            _httpClient = new HttpClient(handler)
            {
                Timeout = Timeout.InfiniteTimeSpan // We handle timeouts with CancellationTokens
            };
        }

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
                    $",path:{path},qp:{querParams}{(!req.KeepAlive ? ",keepAlive:false" : "")}{(!req.IgnoreDip && client.DipIp != null ? $",dipIp:{client.DipIp}" : "")}";
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
                        var authValue = requestHeader.Value.FirstOrDefault();
                        message += (!firstHeader ? Environment.NewLine : "") +
                                   $"[AuthorizationHeaderLength:{authValue?.Length ?? 0}]";
                    }
                    else
                    {
                        message += (!firstHeader ? Environment.NewLine : "") +
                                   $"[{requestHeader.Key}:{string.Join(",", requestHeader.Value)}]";
                    }

                    firstHeader = false;
                }
                message += $"{Environment.NewLine}{Environment.NewLine}";
                message += $"ResponseStatus:{response.StatusCode}{Environment.NewLine}{Environment.NewLine}ResponseHeaders:{Environment.NewLine}";
                firstHeader = true;
                foreach (var responseHeader in response.Headers)
                {
                    message += (!firstHeader ? Environment.NewLine : "") + $"[{responseHeader.Key}:{string.Join(",", responseHeader.Value)}]";
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
        /// <summary>
        /// Sets the WebRequest headers
        /// </summary>
        /// <param name="webReq">HttpWebRequest</param>
        /// <param name="client">AdlsClient</param>
        /// <param name="req">RequestOptions</param>
        /// <param name="token">Auth token</param>
        /// <param name="opMethod">Operation method (e.g. POST/GET)</param>
        /// <param name="customHeaders">Custom headers</param>
        private static void AssignCommonHttpHeaders(HttpRequestMessage request, AdlsClient client, RequestOptions req, string token, string opMethod, IDictionary<string, string> customHeaders, int postRequestLength)
        {
            // Add authorization
            request.Headers.TryAddWithoutValidation("Authorization", token);
            
            // Add latency tracking header
            string latencyHeader = LatencyTracker.GetLatency();
            if (!string.IsNullOrEmpty(latencyHeader))
            {
                request.Headers.TryAddWithoutValidation("x-ms-adl-client-latency", latencyHeader);
            }

            // Add content encoding if set
            if (client.ContentEncoding != null && postRequestLength > MinDataSizeForCompression)
            {
                request.Headers.TryAddWithoutValidation("Content-Encoding", client.ContentEncoding);
            }

            // Add Host header if using DIP
            if (client.DipIp != null && !req.IgnoreDip)
            {
                request.Headers.Host = client.AccountFQDN;
            }

            // Add custom headers
            if (customHeaders != null)
            {
                string contentType;
                if (customHeaders.TryGetValue("Content-Type", out contentType))
                {
                    // Content-Type handled separately on HttpContent
                }
                foreach (var key in customHeaders.Keys)
                {
                    if (!HeadersNotToBeCopied.Contains(key))
                        request.Headers.TryAddWithoutValidation(key, customHeaders[key]);
                }
            }
            
            // Add user agent
            request.Headers.TryAddWithoutValidation("User-Agent", client.GetUserAgent());
            
            // Add client request ID
            request.Headers.Add("x-ms-client-request-id", req.RequestId);
        }
        /// <summary>
        /// Helper to get HttpMethod from string
        /// </summary>
        private static HttpMethod GetHttpMethod(string method)
        {
            string upperMethod = method.ToUpperInvariant();
            if (upperMethod == "GET") return HttpMethod.Get;
            if (upperMethod == "POST") return HttpMethod.Post;
            if (upperMethod == "PUT") return HttpMethod.Put;
            if (upperMethod == "DELETE") return HttpMethod.Delete;
            if (upperMethod == "HEAD") return HttpMethod.Head;
            return new HttpMethod(method);
        }
        private static CancellationTokenSource GetCancellationTokenSourceForTimeout(RequestOptions req)
        {
            if (req.TimeOut != default(TimeSpan))
            {
                return new CancellationTokenSource(req.TimeOut);
            }
            return new CancellationTokenSource();
        }

        /// <summary>
        /// Verifies the responseData for the operation and initializes it if the encoding is chunked
        /// </summary>
        /// <param name="webResponse">HttpResponseMessage</param>
        /// <param name="responseData">ResponseData structure</param>
        /// <param name="isResponseError">True when we are initializing error response stream else false</param>
        /// <returns>False if the response is not chunked but the content length is 0 else true</returns>
        private static bool InitializeResponseData(HttpResponseMessage webResponse, ref ByteBuffer responseData, bool isResponseError = false)
        {
            bool isChunked = webResponse.Headers.TransferEncodingChunked.HasValue && webResponse.Headers.TransferEncodingChunked.Value;
            
            if (isChunked)
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
                    long? contentLength = webResponse.Content?.Headers.ContentLength;
                    if (contentLength.HasValue && contentLength.Value > 0)
                    {
                        responseData.Data = new byte[contentLength.Value];
                        responseData.Offset = 0;
                        responseData.Count = (int)contentLength.Value;
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
        /// <param name="timeoutCancelToken">Cancel Token for timeout</param>
        /// <param name="actualCancelToken">Cancel Token sent by user</param>
        /// <summary>
        /// Handles HttpRequestException (network/connection errors)
        /// </summary>
        private static void HandleHttpRequestException(HttpRequestException e, OperationResponse resp, string path, string requestId, string token)
        {
            resp.IsSuccessful = false;
            resp.Ex = e;
            resp.Error = e.Message;
            resp.ConnectionFailure = true;

            if (WebTransportLog.IsErrorEnabled)
            {
                WebTransportLog.Error($"HttpRequestException for path {path}, RequestId: {requestId}, Message: {e.Message}");
            }
        }

        /// <summary>
        /// Handles TaskCanceledException (timeout or user cancellation)
        /// </summary>
        private static void HandleTaskCanceledException(TaskCanceledException e, OperationResponse resp, CancellationToken timeoutCancelToken, CancellationToken actualCancelToken)
        {
            if (timeoutCancelToken.IsCancellationRequested && !actualCancelToken.IsCancellationRequested)
            {
                resp.Ex = new Exception("Operation timed out"); // Don't use TimeoutException to allow retries
                resp.Error = "Operation timed out";
            }
            else
            {
                resp.Ex = new OperationCanceledException(actualCancelToken);
                resp.Error = "Operation cancelled";
            }
            resp.IsSuccessful = false;
        }

        /// <summary>
        /// Handles error responses from the server
        /// </summary>
        private static async Task HandleErrorResponseAsync(HttpResponseMessage response, HttpRequestMessage request, OperationResponse resp, string path, string requestId, string token, CancellationToken cancelToken)
        {
            PostPowershellLogDetails(request, response);
            resp.HttpStatus = response.StatusCode;
            resp.HttpMessage = response.ReasonPhrase;
            
            if (response.Headers.TryGetValues("x-ms-request-id", out var requestIdValues))
            {
                resp.RequestId = requestIdValues.FirstOrDefault();
            }

            if (resp.HttpStatus == HttpStatusCode.Unauthorized && TokenLog.IsDebugEnabled)
            {
                string tokenLogLine =
                    $"HTTPRequest,HTTP401,cReqId:{requestId},sReqId:{resp.RequestId},path:{path},token:{token}";
                TokenLog.Debug(tokenLogLine);
            }

            ByteBuffer errorResponseData = default(ByteBuffer);
            try
            {
                if (!InitializeResponseData(response, ref errorResponseData, true))
                {
                    throw new ArgumentException("ContentLength of error response stream is not set");
                }

                if (response.Content != null)
                {
                    using (Stream errorStream = await response.Content.ReadAsStreamAsync().ConfigureAwait(false))
                    {
                        int noBytes;
                        int totalLengthToRead = errorResponseData.Count;
                        do
                        {
                            noBytes = await errorStream.ReadAsync(errorResponseData.Data, errorResponseData.Offset, totalLengthToRead, cancelToken).ConfigureAwait(false);
                            errorResponseData.Offset += noBytes;
                            totalLengthToRead -= noBytes;

                        } while (noBytes > 0 && totalLengthToRead > 0);
                        
                        string contentType = response.Content.Headers.ContentType?.MediaType;
                        ParseRemoteError(errorResponseData.Data, errorResponseData.Offset, resp, contentType);
                    }
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
                if (resp.Ex is OperationCanceledException)//Operation is cancelled then no retries
                {
                    break;
                }

            } while (!resp.IsSuccessful && req.RetryOption.ShouldRetry((int)resp.HttpStatus, resp.Ex));
            resp.OpCode = opCode;

            // If dip is used, this request is not ignoring dip, then reset the DIP
            if (client.DipIp != null && !req.IgnoreDip)
            {
                try
                {
                    await client.UpdateDipIfNeededAsync(resp.ConnectionFailure, default(CancellationToken)).ConfigureAwait(false);
                }
                catch (Exception)
                {
                    // This should not cause exception of the main request
                }
            }

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
                // Create HTTP request message
                HttpMethod method = GetHttpMethod(op.Method);
                using (var request = new HttpRequestMessage(method, urlString))
                {
                    // Check cancellation
                    cancelToken.ThrowIfCancellationRequested();

                    // Get authorization token
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

                // Assign headers
                AssignCommonHttpHeaders(request, client, req, token, op.Method, customHeaders, requestData.Count);

                // Handle client certificate if needed
                if (req.ClientCert != null)
                {
                    // Note: HttpClient with HttpClientHandler needs certificate added to handler, not per-request
                    // This would require modifying the static HttpClient or creating a new one
                    // For now, log a warning
                    if (WebTransportLog.IsWarnEnabled)
                    {
                        WebTransportLog.Warn("Client certificates are not supported with static HttpClient. Consider creating a separate HttpClient instance.");
                    }
                }

                using (var timeoutCancellationTokenSource = GetCancellationTokenSourceForTimeout(req))
                {
                    using (CancellationTokenSource linkedCts =
                        CancellationTokenSource.CreateLinkedTokenSource(cancelToken, timeoutCancellationTokenSource.Token))
                    {
                        try
                        {
                            // Add request body if needed
                            if (!op.Method.Equals("GET", StringComparison.OrdinalIgnoreCase))
                            {
                                if (op.RequiresBody && requestData.Data != null)
                                {
                                    // Create content
                                    HttpContent content;
                                    
                                    if (client.WrapperStream != null && requestData.Count > MinDataSizeForCompression)
                                    {
                                        // Use compressed content
                                        var memoryStream = new MemoryStream();
                                        using (var compressedStream = GetCompressedStream(memoryStream, client, requestData.Count))
                                        {
                                            await compressedStream.WriteAsync(requestData.Data, requestData.Offset, 
                                                requestData.Count, linkedCts.Token).ConfigureAwait(false);
                                        }
                                        content = new ByteArrayContent(memoryStream.ToArray());
                                    }
                                    else
                                    {
                                        content = new ByteArrayContent(requestData.Data, requestData.Offset, requestData.Count);
                                    }

                                    // Set content type from custom headers if present
                                    if (customHeaders != null && customHeaders.ContainsKey("Content-Type"))
                                    {
                                        content.Headers.ContentType = MediaTypeHeaderValue.Parse(customHeaders["Content-Type"]);
                                    }

                                    request.Content = content;
                                }
                                else
                                {
                                    // Empty body
                                    request.Content = new ByteArrayContent(Array.Empty<byte>());
                                }
                            }

                            // Send request
                            HttpResponseMessage response = await _httpClient.SendAsync(request, 
                                HttpCompletionOption.ResponseHeadersRead, linkedCts.Token).ConfigureAwait(false);

                            // Check for success
                            if (!response.IsSuccessStatusCode)
                            {
                                // Handle error response
                                await HandleErrorResponseAsync(response, request, resp, path, req.RequestId, token, linkedCts.Token).ConfigureAwait(false);
                                return null;
                            }

                            // Success response
                            resp.HttpStatus = response.StatusCode;
                            resp.HttpMessage = response.ReasonPhrase;
                            
                            if (response.Headers.TryGetValues("x-ms-request-id", out var requestIdValues))
                            {
                                resp.RequestId = requestIdValues.FirstOrDefault();
                            }
                            
                            PostPowershellLogDetails(request, response);

                            if (op.ReturnsBody)
                            {
                                if (!InitializeResponseData(response, ref responseData))
                                {
                                    return null;
                                }

                                int totalBytes = 0;
                                using (Stream opStream = await response.Content.ReadAsStreamAsync().ConfigureAwait(false))
                                {
                                    int noBytes;
                                    int totalLengthToRead = responseData.Count;
                                    do
                                    {
                                        noBytes = await opStream.ReadAsync(responseData.Data, responseData.Offset,
                                            totalLengthToRead, linkedCts.Token).ConfigureAwait(false);
                                        totalBytes += noBytes;
                                        responseData.Offset += noBytes;
                                        totalLengthToRead -= noBytes;

                                    } while (noBytes > 0 && totalLengthToRead > 0);
                                }

                                return Tuple.Create(responseData.Data, totalBytes);
                            }

                            return Tuple.Create<byte[], int>(null, 0);
                        }
                        catch (HttpRequestException e)
                        {
                            HandleHttpRequestException(e, resp, path, req.RequestId, token);
                            return null;
                        }
                        catch (TaskCanceledException e)
                        {
                            HandleTaskCanceledException(e, resp, timeoutCancellationTokenSource.Token, cancelToken);
                            return null;
                        }
                    }
                }
                } // Close using (var request = ...)
            }
            catch (Exception e)
            {
                resp.IsSuccessful = false;
                resp.Ex = e;
                resp.Error = e.Message;
                return null;
            }
        }

        #endregion

        #region Sync

        /// <summary>
        /// Calls the API that makes the HTTP request to the server. Retries the HTTP request in certain cases. This is a synchronous call.
        /// </summary>
        /// <param name="opCode">Operation Code</param>
        /// <param name="path">Path of the file or directory</param>
        /// <param name="requestData">byte array, offset and length of the data of http request</param>
        /// <param name="responseData">byte array, offset and length of the data of http response. byte array should be initialized for chunked response</param>
        /// <param name="quer">Headers for request</param>
        /// <param name="client">ADLS Store CLient</param>
        /// <param name="req">Request options containing RetryOption, timout and requestid </param>
        /// <param name="resp">Contains the response message </param>
        /// <param name="customHeaders">Dictionary containing the custom header that Core wants to pass</param>
        /// <returns>Tuple of Byte array containing the bytes returned from the server and number of bytes read from server</returns>

        internal static Tuple<byte[], int> MakeCall(string opCode, string path, ByteBuffer requestData, ByteBuffer responseData, QueryParams quer, AdlsClient client, RequestOptions req, OperationResponse resp, IDictionary<string, string> customHeaders = null)
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
                retVal = MakeSingleCall(opCode, path, requestData, responseData, quer, client, req, resp, customHeaders);
                watch.Stop();
                resp.LastCallLatency = watch.ElapsedMilliseconds;
                HandleMakeSingleCallResponse(opCode, path, resp, retVal?.Item2 ?? 0, requestData.Count, req, client, quer.Serialize(opCode), ref numRetries);

            } while (!resp.IsSuccessful && req.RetryOption.ShouldRetry((int)resp.HttpStatus, resp.Ex));
            resp.OpCode = opCode;
            // If dip is used, this request is not ignoring dip, then reset the DIP
            if (client.DipIp != null && !req.IgnoreDip)
            {
                try
                {
                    client.UpdateDipIfNeededAsync(resp.ConnectionFailure, default(CancellationToken)).GetAwaiter().GetResult();
                }
                catch (Exception)
                {
                    // This should not cause exception of the main request
                }
            }
            return retVal;
        }

        /// <summary>
        /// Makes a single Http call to the server, sends the request and obtains the response. This is a synchronous call.
        /// NOTE: This method blocks on the async version. Consider using MakeSingleCallAsync instead for better performance.
        /// </summary>
        /// <param name="opCode">Operation Code</param>
        /// <param name="path">Path of the file or directory</param>
        /// <param name="requestData">byte array, offset and length of the data of http request</param>
        /// <param name="responseData">byte array, offset and length of the data of http response. byte array should be initialized for chunked response</param>
        /// <param name="qp">Headers for request</param>
        /// <param name="client">ADLS Store CLient</param>
        /// <param name="req">Request options containing RetryOption, timout and requestid </param>
        /// <param name="resp">Contains the response message </param>
        /// <param name="customHeaders">Dictionary containing the custom header that Core wants to pass</param>
        /// <returns>Tuple of Byte array containing the bytes returned from the server and number of bytes read from server</returns>
        private static Tuple<byte[], int> MakeSingleCall(string opCode, string path, ByteBuffer requestData, ByteBuffer responseData, QueryParams qp, AdlsClient client, RequestOptions req, OperationResponse resp, IDictionary<string, string> customHeaders)
        {
            // Call the async version synchronously
            // This is not ideal but maintains backward compatibility for sync callers
            return MakeSingleCallAsync(opCode, path, requestData, responseData, qp, client, req, resp, CancellationToken.None, customHeaders).GetAwaiter().GetResult();
        }
        #endregion


    }
}
