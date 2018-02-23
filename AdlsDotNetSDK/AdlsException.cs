using System;
using System.IO;
using System.Net;

namespace Microsoft.Azure.DataLake.Store
{
    /// <summary>
    /// Exception returned by client. It encapsulates any remote excepotion returned from server, Unhandled and handled exception.
    /// </summary>
    public class AdlsException : IOException
    {
        /// <summary>
        /// Constructor that sets the message for inner exception
        /// </summary>
        /// <param name="message">Message</param>
        public AdlsException(string message) : base(message)
        { }
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
        /// Unhandled exception
        /// </summary>
        public Exception Ex { get; internal set; }
        /// <summary>
        /// Handled exception errors
        /// </summary>
        public string Error { get; internal set; }
        /// <summary>
        /// Total number of retries
        /// </summary>
        public int Retries { get; internal set; }
        /// <summary>
        /// HttpStatus code
        /// </summary>
        public HttpStatusCode HttpStatus { get; internal set; }
        /// <summary>
        /// Http message
        /// </summary>
        public string HttpMessage { get; internal set; }
        /// <summary>
        /// Trace Id as returned by server
        /// </summary>
        public string TraceId { get; internal set; }
        /// <summary>
        /// Last call latency
        /// </summary>
        public long LastCallLatency { get; internal set; }

    }
}
