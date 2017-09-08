using System;
using System.Net;

namespace Microsoft.Azure.DataLake.Store
{
    /// <summary>
    /// Class encapsulates the response of one operation. Available operations are in Operation.cs
    /// For one operation, Http request can sent more than one time (More than one retry).
    /// </summary>
    public class OperationResponse
    {
        /// <summary>
        /// Operation Code
        /// </summary>
        public string OpCode { get; internal set; }
        /// <summary>
        /// Number of retries for the operation
        /// </summary>
        public int Retries { get; internal set; }
        /// <summary>
        /// HttpStatus Code for the last Http request for this operation
        /// </summary>
        public HttpStatusCode HttpStatus { get; internal set; }
        /// <summary>
        /// Http message/ output for the last Http request for this operation
        /// </summary>
        public string HttpMessage { get; internal set; }
        /// <summary>
        /// Remote exception name returned from the server for the last Http request for this operation
        /// </summary>
        public string RemoteExceptionName { get; internal set; }

        /// <summary>
        /// Remote exception message returned from the server for the last Http request for this operation
        /// </summary>
        public string RemoteExceptionMessage { get; internal set; }
        /// <summary>
        /// Remote exception java classname returned from the server for the last Http request for this operation
        /// </summary>
        public string RemoteExceptionJavaClassName { get; internal set; }
        /// <summary>
        /// Exception history of all the retries for this operation. This should not be reset for every retry
        /// </summary>
        public string ExceptionHistory { get; internal set; }
        /// <summary>
        /// Any other exception thrown while sending the last Http request for the operation
        /// </summary>
        public Exception Ex { get; internal set; }
        /// <summary>
        /// Whether the last Http request was successful for the operation
        /// </summary>
        public bool IsSuccessful { get; internal set; }

        /// <summary>
        /// Any other error caught by the code while sending the last Http request for the operation
        /// </summary>
        public string Error { get; internal set; }
        /// <summary>
        /// Request Id or the trace ID returned from the server for the last request for the operation
        /// </summary>
        public string RequestId { get; internal set; }
        /// <summary>
        /// Total latency for the last request for the operation
        /// </summary>
        public long LastCallLatency { get; internal set; }
        /// <summary>
        /// Total latency for token acquisition for the last request for the operation
        /// </summary>
        public long TokenAcquisitionLatency { get; internal set; }
        /// <summary>
        /// Resets all memebers exception the ExceptionHistory
        /// </summary>
        public void Reset()
        {
            OpCode = "";
            Retries = 0;
            HttpStatus = 0;
            HttpMessage = "";
            IsSuccessful = false;
            RemoteExceptionName = RemoteExceptionMessage = RemoteExceptionJavaClassName = "";
            Ex = null;
            Error = "";
            RequestId = "";
            LastCallLatency = TokenAcquisitionLatency = 0;
        }
    }
}
