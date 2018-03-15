using System;
using System.IO;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using System.Diagnostics;
using System.Threading;
using NLog;

namespace Microsoft.Azure.DataLake.Store
{
    /// <summary>
    /// Http layer class to send htttp requests to the server. We have both async and sync MakeCall. The reason we have that is if the application is doing some operations using explicit threads,
    /// then using async-await internally creates unecessary tasks for worker threads in threadpool. Application can create explicit threads in cases of uploader and downloader.
    /// </summary>
    internal class WebTransport
    {
        /// <summary>
        /// Logger to log messages related to Http transport
        /// </summary>
        private static readonly Logger WebTransportLog = LogManager.GetLogger("adls.dotnet.WebTransport");
        /// <summary>
        /// Logger to log messages related to authorization token
        /// </summary>
        private static readonly Logger TokenLog = LogManager.GetLogger("adls.dotnet.WebTransport.Token");
        /// <summary>
        /// Delegate that encapsulatesz the method that gets called when CancellationToken is cancelled
        /// </summary>
        private static readonly Action<object> OnCancel = OnTokenCancel;
        /// <summary>
        /// Capacity of the stringbuilder to build the Http request URL
        /// </summary>
        private const int UrlLength = 100;
        /// <summary>
        /// Method that gets called when CancellationToken is cancelled. It aborts the Http web request.
        /// </summary>
        /// <param name="state">HttpWebRequest instance</param>
        private static void OnTokenCancel(object state)
        {
            HttpWebRequest webRequest = state as HttpWebRequest;
            webRequest?.Abort();
        }

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
        /// <param name="cancelToken">CAncellationToken to cancel the operation</param>
        /// <returns>Tuple of Byte array containing the bytes returned from the server and number of bytes read from server</returns>

        internal static async Task<Tuple<byte[], int>> MakeCallAsync(string opCode, string path,
            ByteBuffer requestData, ByteBuffer responseData, QueryParams quer, AdlsClient client, RequestOptions req, OperationResponse resp, CancellationToken cancelToken)
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
                retVal = await MakeSingleCallAsync(opCode, path, requestData, responseData, quer, client, req, resp, cancelToken).ConfigureAwait(false);
                watch.Stop();
                resp.LastCallLatency = watch.ElapsedMilliseconds;
                HandleMakeSingleCallResponse(opCode, path, resp, retVal?.Item2 ?? 0, requestData.Count, req.RequestId, client.ClientId, quer.Serialize(opCode), ref numRetries);
                if (resp.Ex is OperationCanceledException)//Operation is cancelled then no retries
                {
                    break;
                }
            } while (!resp.IsSuccessful && req.RetryOption.ShouldRetry((int)resp.HttpStatus, resp.Ex));
            resp.OpCode = opCode;
            return retVal;
        }
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
        /// <returns>Tuple of Byte array containing the bytes returned from the server and number of bytes read from server</returns>

        internal static Tuple<byte[], int> MakeCall(string opCode, string path, ByteBuffer requestData, ByteBuffer responseData, QueryParams quer, AdlsClient client, RequestOptions req, OperationResponse resp)
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
                retVal = MakeSingleCall(opCode, path, requestData, responseData, quer, client, req, resp);
                watch.Stop();
                resp.LastCallLatency = watch.ElapsedMilliseconds;
                HandleMakeSingleCallResponse(opCode, path, resp, retVal?.Item2 ?? 0, requestData.Count, req.RequestId, client.ClientId, quer.Serialize(opCode), ref numRetries);
            } while (!resp.IsSuccessful && req.RetryOption.ShouldRetry((int)resp.HttpStatus, resp.Ex));
            resp.OpCode = opCode;
            return retVal;
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
        /// <param name="requestId">Request ID</param>
        /// <param name="clientId">Client Id of the application</param>
        /// <param name="querParams">Serialized query parameter of the Http request</param>
        /// <param name="numRetries">Number of retries</param>
        private static void HandleMakeSingleCallResponse(string opCode, string path, OperationResponse resp, int responseLength, int requestLength, string requestId, long clientId, string querParams, ref int numRetries)
        {
            DetermineIsSuccessful(resp);//After recieving the response from server determine whether the response is successful
            string error = "";
            if (!resp.IsSuccessful)
            {
                if (resp.Ex != null)
                {
                    error = resp.Ex.Message;
                }
                else if (!string.IsNullOrEmpty(resp.RemoteExceptionName))
                {
                    error = resp.HttpStatus + ": " + resp.RemoteExceptionName;
                }
                else if (!string.IsNullOrEmpty(resp.Error))
                {
                    error = resp.Error;
                }
                //This is either unexplained exception or the remote exception returned from server
                resp.ExceptionHistory = resp.ExceptionHistory == null ? error : resp.ExceptionHistory + "," + error;
                numRetries++;
            }
            if (WebTransportLog.IsDebugEnabled)
            {
                string logLine = $"HTTPRequest,{(resp.IsSuccessful ? "Succeeded" : "failed")},cReqId:{requestId},lat:{resp.LastCallLatency},err{error},Reqlen:{requestLength},Resplen:{responseLength}" +
                                 $",token_ns:{resp.TokenAcquisitionLatency},sReqId:{resp.RequestId},path:{path},qp:{querParams}";
                WebTransportLog.Debug(logLine);
            }
            LatencyTracker.AddLatency(requestId, numRetries, resp.LastCallLatency, error, opCode,
                requestLength + responseLength, clientId);
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
        /// <returns>Tuple of Byte array containing the bytes returned from the server and number of bytes read from server</returns>
        private static async Task<Tuple<byte[], int>> MakeSingleCallAsync(string opCode, string path,
            ByteBuffer requestData, ByteBuffer responseData, QueryParams qp, AdlsClient client, RequestOptions req, OperationResponse resp, CancellationToken cancelToken)
        {
            string token = null;
            Operation op = Operation.Operations[opCode];
            string urlString = CreateHttpRequestUrl(op, path, client, resp, qp.Serialize(opCode));
            if (string.IsNullOrEmpty(urlString))
            {
                return null;
            }
            try
            {
                //If operation is cancelled then stop
                cancelToken.ThrowIfCancellationRequested();
                HttpWebRequest webReq = (HttpWebRequest)WebRequest.Create(urlString);
                //This point onwards if operation is cancelled http request is aborted
                cancelToken.Register(OnCancel, webReq);
                Stopwatch watch = Stopwatch.StartNew();
                token = await client.GetTokenAsync(cancelToken).ConfigureAwait(false);
                watch.Stop();
                resp.TokenAcquisitionLatency = watch.ElapsedMilliseconds;
                if (string.IsNullOrEmpty(token))
                {
                    resp.Error = "Token is null or empty";
                    return null;
                }
                AssignCommonHttpHeaders(webReq, client, req, token, op.Method);
                if (!op.Method.Equals("GET"))
                {
                    if (op.RequiresBody && requestData.Data != null)
                    {
                        SetWebRequestContentLength(webReq, requestData.Count); 
                        using (Stream ipStream = await webReq.GetRequestStreamAsync().ConfigureAwait(false))
                        {
                            await ipStream.WriteAsync(requestData.Data, requestData.Offset, requestData.Count,
                                cancelToken).ConfigureAwait(false);
                        }
                    }
                    else
                    {
                        SetWebRequestContentLength(webReq, 0);
                    }
                }
                using (var webResponse = (HttpWebResponse)await webReq.GetResponseAsync().ConfigureAwait(false))
                {
                    resp.HttpStatus = webResponse.StatusCode;
                    resp.HttpMessage = webResponse.StatusDescription;
                    resp.RequestId = webResponse.Headers["x-ms-request-id"];
                    if (op.ReturnsBody)
                    {

                        if (!InitializeResponseData(webResponse, ref responseData))
                        {
                            return null;
                        }
                        int totalBytes = 0;
                        using (Stream opStream = webResponse.GetResponseStream())
                        {

                            int noBytes;
                            int totalLengthToRead = responseData.Count;
                            //Read the required amount of data. In case of chunked it is what users requested, else it is amount of data sent
                            do
                            {
                                noBytes = await opStream.ReadAsync(responseData.Data, responseData.Offset, totalLengthToRead, cancelToken).ConfigureAwait(false);
                                totalBytes += noBytes;
                                responseData.Offset += noBytes;
                                totalLengthToRead -= noBytes;

                            } while (noBytes > 0 && totalLengthToRead > 0);
                        }
                        return Tuple.Create(responseData.Data, totalBytes);//Return the total bytes read also since in case of chunked amount of data returned can be less than data returned
                    }
                }
            }
            catch (WebException e)
            {
                HandleWebException(e, resp, path, req.RequestId, token, cancelToken);
            }
            catch (Exception e)
            {
                resp.Ex = cancelToken.IsCancellationRequested ? new OperationCanceledException(cancelToken) : e;

            }
            return null;
        }
        /// <summary>
        /// Makes a single Http call to the server, sends the request and obtains the response. This is a synchronous call.
        /// </summary>
        /// <param name="opCode">Operation Code</param>
        /// <param name="path">Path of the file or directory</param>
        /// <param name="requestData">byte array, offset and length of the data of http request</param>
        /// <param name="responseData">byte array, offset and length of the data of http response. byte array should be initialized for chunked response</param>
        /// <param name="qp">Headers for request</param>
        /// <param name="client">ADLS Store CLient</param>
        /// <param name="req">Request options containing RetryOption, timout and requestid </param>
        /// <param name="resp">Contains the response message </param>
        /// <returns>Tuple of Byte array containing the bytes returned from the server and number of bytes read from server</returns>
        private static Tuple<byte[], int> MakeSingleCall(string opCode, string path, ByteBuffer requestData, ByteBuffer responseData, QueryParams qp, AdlsClient client, RequestOptions req, OperationResponse resp)
        {
            string token = null;
            Operation op = Operation.Operations[opCode];
            string urlString = CreateHttpRequestUrl(op, path, client, resp, qp.Serialize(opCode));
            if (string.IsNullOrEmpty(urlString))
            {
                return null;
            }
            try
            {
                HttpWebRequest webReq = (HttpWebRequest)WebRequest.Create(urlString);
                Stopwatch watch = Stopwatch.StartNew();
                token = client.GetTokenAsync().GetAwaiter().GetResult();
                watch.Stop();
                resp.TokenAcquisitionLatency = watch.ElapsedMilliseconds;
                if (string.IsNullOrEmpty(token))
                {
                    resp.Error = "Token is null or empty";
                    return null;
                }
                AssignCommonHttpHeaders(webReq, client, req, token, op.Method);
                if (!op.Method.Equals("GET"))
                {
                    if (op.RequiresBody && requestData.Data != null)
                    {
                        SetWebRequestContentLength(webReq, requestData.Count);
#if NET452
                        using (Stream ipStream = webReq.GetRequestStream())
#else
                        using (Stream ipStream = webReq.GetRequestStreamAsync().GetAwaiter().GetResult())
#endif
                        {
                            ipStream.Write(requestData.Data, requestData.Offset, requestData.Count);
                        }
                    }
                    else
                    {
                        SetWebRequestContentLength(webReq, 0);
                    }
                }
#if NET452
                using (var webResponse = (HttpWebResponse)webReq.GetResponse())
#else
                using (var webResponse = (HttpWebResponse)webReq.GetResponseAsync().GetAwaiter().GetResult())
#endif
                {
                    resp.HttpStatus = webResponse.StatusCode;
                    resp.HttpMessage = webResponse.StatusDescription;
                    resp.RequestId = webResponse.Headers["x-ms-request-id"];
                    if (op.ReturnsBody)
                    {

                        if (!InitializeResponseData(webResponse, ref responseData))
                        {
                            return null;
                        }
                        int totalBytes = 0;
                        using (Stream opStream = webResponse.GetResponseStream())
                        {

                            int noBytes;
                            int totalLengthToRead = responseData.Count;
                            //Read the required amount of data. In case of chunked it is what users requested, else it is amount of data sent
                            do
                            {
                                noBytes = opStream.Read(responseData.Data, responseData.Offset, totalLengthToRead);
                                totalBytes += noBytes;
                                responseData.Offset += noBytes;
                                totalLengthToRead -= noBytes;

                            } while (noBytes > 0 && totalLengthToRead > 0);
                        }
                        return Tuple.Create(responseData.Data, totalBytes);//Return the total bytes read also since in case of chunked amount of data returned can be less than data returned
                    }
                }
            }
            catch (WebException e)
            {
                HandleWebException(e, resp, path, req.RequestId, token);
            }
            catch (Exception e)
            {
                resp.Ex = e;

            }
            return null;
        }
        /// <summary>
        /// Serializes the client FQDN, queryparams and token into a request URL
        /// </summary>
        /// <param name="op">Operation</param>
        /// <param name="path">Path of directory or file</param>
        /// <param name="client">AdlsClient</param>
        /// <param name="resp">OperationResponse</param>
        /// <param name="queryParams">Serialized queryparams</param>
        /// <returns>URL</returns>
        private static string CreateHttpRequestUrl(Operation op, string path, AdlsClient client, OperationResponse resp, string queryParams)
        {
            StringBuilder urlString = new StringBuilder(UrlLength);
            urlString.Append(client.GetHttpPrefix());
            urlString.Append("://");
            urlString.Append(client.AccountFQDN);
            urlString.Append(op.Namespace);
            try
            {
                urlString.Append(Uri.EscapeDataString(path));
            }
            catch (UriFormatException ex)
            {
                resp.Error = "UriFormatException: " + ex.Message;
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
                resp.Error = "UriFormatException: " + ur.Message;
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
        private static void AssignCommonHttpHeaders(HttpWebRequest webReq, AdlsClient client, RequestOptions req, string token, string opMethod)
        {
            webReq.Headers["Authorization"] = token;
            string latencyHeader = LatencyTracker.GetLatency();
            if (!string.IsNullOrEmpty(latencyHeader))
            {
                webReq.Headers["x-ms-adl-client-latency"] = latencyHeader;
            }
#if NET452
            webReq.UserAgent = client.GetUserAgent();
            //Setting timeout is only available in NET452
            webReq.ReadWriteTimeout = (int)req.TimeOut.TotalMilliseconds;
            webReq.Timeout = (int)req.TimeOut.TotalMilliseconds;
            webReq.ServicePoint.UseNagleAlgorithm = false;
            webReq.ServicePoint.Expect100Continue = false;
#else
            webReq.Headers["User-Agent"] = client.GetUserAgent();
#endif
            webReq.Headers["x-ms-client-request-id"] = req.RequestId;
            webReq.Method = opMethod;
        }
        /// <summary>
        /// Sets the WebRequest length
        /// </summary>
        /// <param name="webReq">HttpWebRequest</param>
        /// <param name="count">Content length</param>
        private static void SetWebRequestContentLength(HttpWebRequest webReq, int count)
        {
#if NET452
            webReq.ContentLength = count;
#else
            // Set the ContentLength property of the WebRequest.  
            webReq.Headers["Content-Length"] = Convert.ToString(count);
#endif           
        }
        /// <summary>
        /// Verifies the responseData for the operation and initializes it if the encoding is chunked
        /// </summary>
        /// <param name="webResponse">HttpWebResponse</param>
        /// <param name="responseData">ResponseData structure</param>
        /// <returns>False if the response is not chunked but the content length is 0 else true</returns>
        private static bool InitializeResponseData(HttpWebResponse webResponse, ref ByteBuffer responseData)
        {
            string encoding = webResponse.Headers["Transfer-Encoding"];
            if (!string.IsNullOrEmpty(encoding) && encoding.Equals("chunked"))
            {
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
                    if (webResponse.ContentLength > 0)
                    {
                        responseData.Data = new byte[webResponse.ContentLength];
                        responseData.Offset = 0;
                        responseData.Count = (int) webResponse.ContentLength;
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
        /// <param name="cancelToken">Cance</param>
        private static void HandleWebException(WebException e, OperationResponse resp, string path, string requestId, string token, CancellationToken cancelToken = default(CancellationToken))
        {
            if (cancelToken.IsCancellationRequested)
            {
                resp.Ex = new OperationCanceledException(cancelToken);
            }
            //This the case where some exception occured in server but server returned a response
            else if (e.Status == WebExceptionStatus.ProtocolError)
            {
                try
                {
                    using (var errorResponse = (HttpWebResponse) e.Response)
                    {
                        resp.HttpStatus = errorResponse.StatusCode;
                        resp.RequestId = errorResponse.Headers["x-ms-request-id"];
                        if (resp.HttpStatus == HttpStatusCode.Unauthorized && TokenLog.IsDebugEnabled)
                        {
                            string tokenLogLine =
                                $"HTTPRequest,HTTP401,cReqId:{requestId},sReqId:{resp.RequestId},path:{path},token:{token}";
                            TokenLog.Debug(tokenLogLine);
                        }
                        resp.HttpMessage = errorResponse.StatusDescription;
                        using (Stream errorStream = errorResponse.GetResponseStream())
                        {
                            ParseRemoteError(errorStream, resp);
                        }
                    }
                }
                catch (Exception)
                {
                    resp.Error = "Unexpected error in JSON parsing";
                }

            }
            else//No response stream is returned, Dont know what to do, so just store the exception
            {
                resp.Ex = e;

            }
        }
        /// <summary>
        /// Parses RemoteException and populates the remote error fields in OperationResponse
        /// </summary>
        /// <param name="errorStream">Error Response stream</param>
        /// <param name="resp">Response instance</param>
        /// <returns></returns>
        private static void ParseRemoteError(Stream errorStream, OperationResponse resp)
        {

            using (StreamReader stReader = new StreamReader(errorStream))
            {
                using (var jsonReader = new JsonTextReader(stReader))
                {
                    jsonReader.Read(); //StartObject {
                    jsonReader.Read(); //"RemoteException"
                    if (jsonReader.Value == null || !((string)jsonReader.Value).Equals("RemoteException"))
                    {
                        throw new IOException("There is some different type of exception");
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
}
