using System;
using System.Threading;

namespace Microsoft.Azure.DataLake.Store.RetryPolicies
{
    /// <summary>
    /// Exponential retry policy
    /// </summary>
    public class ExponentialRetryPolicy : RetryPolicy
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
        /// Factor by which we will increase the interval
        /// </summary>
        private int ExponentialFactor { get; }
        /// <summary>
        /// Wait time
        /// </summary>
        private int ExponentialInterval { get; set; }

        public ExponentialRetryPolicy()
        {
            NumberOfRetries = 0;
            MaxRetries = 4;
            ExponentialFactor = 4;
            ExponentialInterval = 1000;
        }

        public ExponentialRetryPolicy(int maxRetries, int interval)
        {
            NumberOfRetries = 0;
            MaxRetries = maxRetries;
            ExponentialFactor = 4;
            ExponentialInterval = interval;
        }
        /// <summary>
        /// Determines whether to retry exponentially
        /// </summary>
        /// <param name="httpCode">Http status code</param>
        /// <param name="ex">Last exception that we saw during Httprequest</param>
        /// <returns></returns>
        public override bool ShouldRetry(int httpCode, Exception ex)
        {
            //HTTP CODE 1xx and 2xx are not errors and 3xx are redirection status which shouldnt be retied
            //501 is not immplemented, 505 http version not supported
            if ((httpCode >= 300 && httpCode < 500 && httpCode != 408 && httpCode != 429) ||
                httpCode == 501 || httpCode == 505)
            {
                return false;
            }
            if (ex != null || httpCode >= 500 || httpCode == 408 || httpCode == 429 || httpCode == 401)
            {
                //For 408-timed out and 429-too many responses and 5xx server except the above ones we need retries
                if (NumberOfRetries < MaxRetries)
                {
                    Thread.Sleep(ExponentialInterval);
                    ExponentialInterval = ExponentialFactor * ExponentialInterval;
                    NumberOfRetries++;
                    return true;
                }
            }
            return false;
        }
    }
}
