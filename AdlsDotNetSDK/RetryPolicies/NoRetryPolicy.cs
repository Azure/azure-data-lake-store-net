using System;

namespace Microsoft.Azure.DataLake.Store.RetryPolicies
{
    /// <summary>
    /// No retry policy
    /// </summary>
    public class NoRetryPolicy : RetryPolicy
    {
        /// <summary>
        /// Returns false always
        /// </summary>
        /// <param name="httpCode">Http Code</param>
        /// <param name="ex">Last exception that we saw during Httprequest</param>
        /// <returns></returns>
        public override bool ShouldRetry(int httpCode, Exception ex)
        {
            return false;
        }
    }
}
