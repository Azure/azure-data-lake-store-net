using System;

namespace Microsoft.Azure.DataLake.Store.RetryPolicies
{
    /// <summary>
    /// Abstract class immplemented by different types of retry policies
    /// </summary>
    public abstract class RetryPolicy
    {
        /// <summary>
        /// Default retry interval
        /// </summary>
        protected const int DefaultRetryInterval = 1000;

        internal bool ShouldRetryBasedOnHttpOutput(int httpCode, Exception ex)
        {
            //HTTP CODE 1xx and 2xx are not errors and 3xx are redirection status which shouldnt be retied
            //501 is not immplemented, 505 http version not supported
            if ((httpCode >= 300 && httpCode < 500 && httpCode != 408 && httpCode != 429) ||
                httpCode == 501 || httpCode == 505)
            {
                return false;
            }
            //For 408-timed out and 429-too many responses and 5xx server except the above ones we need retries
            if (ex != null || httpCode >= 500 || httpCode == 408 || httpCode == 429)
            {
                return true;
            }
            return false;
        }

        /// <summary>
        /// Determines whether the HttpRequest should be retried
        /// </summary>
        /// <param name="httpCode"></param>
        /// <param name="ex">Last exception that we saw during Httprequest</param>
        /// <returns></returns>
        public abstract bool ShouldRetry(int httpCode, Exception ex);
    }

}
