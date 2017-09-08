using System;

namespace Microsoft.Azure.DataLake.Store.RetryPolicies
{
    /// <summary>
    /// Abstract class immplemented by different types of retry policies
    /// </summary>
    public abstract class RetryPolicy
    {
        /// <summary>
        /// Determines whether the HttpRequest should be retried
        /// </summary>
        /// <param name="httpCode"></param>
        /// <param name="ex">Last exception that we saw during Httprequest</param>
        /// <returns></returns>
        public abstract bool ShouldRetry(int httpCode, Exception ex);
    }

}
