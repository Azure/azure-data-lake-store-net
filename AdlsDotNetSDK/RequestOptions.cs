using System;
using System.Security.Cryptography.X509Certificates;
using Microsoft.Azure.DataLake.Store.RetryPolicies;

namespace Microsoft.Azure.DataLake.Store
{
    /// <summary>
    /// This class contains the request parameters like timeout, Retry Policy and unique requestId
    /// </summary>
    public class RequestOptions
    {
        internal bool KeepAlive = true;
        internal bool IgnoreDip = false;
        internal bool UseCert = false;

        private string _requestId;
        /// <summary>
        /// Unique request Id of the Http Request
        /// </summary>
        public string RequestId
        {
            get { return _requestId; }
            set { _requestId = string.IsNullOrEmpty(value) ? Guid.NewGuid().ToString() : value; }
        }

        private TimeSpan _timeOut;
        /// <summary>
        /// Timeout for Http request and stream read and write
        /// </summary>
        public TimeSpan TimeOut
        {
            get { return _timeOut; }
            set { _timeOut = value == TimeSpan.Zero ? new TimeSpan(60000 * 10000) : value; }//By default it should be 60000 milliseconds
        }
        private RetryPolicy _retryOption;
        /// <summary>
        /// Type of retry policy to use
        /// </summary>
        public RetryPolicy RetryOption
        {
            get { return _retryOption; }
            private set
            {
                _retryOption = value ?? new NonIdempotentRetryPolicy();
            }
        }
        /// <summary>
        /// Initializes request Id as GUID, Timeout as 60 seconds, and the request option as No retry
        /// </summary>
        public RequestOptions() : this(null, TimeSpan.Zero, null)
        { }
        /// <summary>
        /// Initializes request Id as GUID (default), Timeout as 60 seconds (default), and the request option
        /// </summary>
        /// <param name="rp">Retry option</param>
        public RequestOptions(RetryPolicy rp) : this(null, TimeSpan.Zero, rp)
        {
        }
        /// <summary>
        /// Initializes request Id, Timeout, and the request option
        /// </summary>
        /// <param name="requestId">request Id</param>
        /// <param name="timeOut">Time out</param>
        /// <param name="rp">Retry policy</param>
        public RequestOptions(string requestId, TimeSpan timeOut, RetryPolicy rp)
        {

            RequestId = requestId;
            TimeOut = timeOut;
            RetryOption = rp;
        }
    }
}
