using System;
using System.Threading;

namespace Microsoft.Azure.DataLake.Store.RetryPolicies
{
    /// <summary>
    /// 
    /// </summary>
    internal class LinearRetryPolicy : RetryPolicy
    {
        
        /// <summary>
        /// Tracks the current number of retries
        /// </summary>
        private int NumberOfRetries { get; set; }
        /// <summary>
        /// Maximum number of retries
        /// </summary>
        private int MaxRetries { get; }
        /// <summary>
        /// Wait time
        /// </summary>
        private int LinearInterval { get; }
        /// <summary>
        /// Default settings of Exponential retry policies
        /// </summary>
        internal LinearRetryPolicy()
        {
            NumberOfRetries = 0;
            MaxRetries = 4;
            LinearInterval = DefaultRetryInterval;
        }
        /// <summary>
        /// Exponential retry policies with specified maximum retries and interval
        /// </summary>
        /// <param name="maxRetries">Maximum retries</param>
        /// <param name="interval">Exponential time interval</param>
        internal LinearRetryPolicy(int maxRetries, int interval)
        {
            NumberOfRetries = 0;
            MaxRetries = maxRetries;
            LinearInterval = interval;
        }

        /// <summary>
        /// Determines whether to retry exponentially. 
        /// </summary>
        /// <param name="httpCode">Http status code</param>
        /// <param name="ex">Last exception that we saw during Httprequest</param>
        /// <returns>True if it should be retried else false</returns>
        public override bool ShouldRetry(int httpCode, Exception ex)
        {
            if (ShouldRetryBasedOnHttpOutput(httpCode, ex))
            {
                if (NumberOfRetries < MaxRetries)
                {
                    Thread.Sleep(LinearInterval);
                    NumberOfRetries++;
                    return true;
                }
            }
            return false;
        }
    }
}