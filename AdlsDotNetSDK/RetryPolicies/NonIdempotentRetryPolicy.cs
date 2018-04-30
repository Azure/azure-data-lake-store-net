using System;
using System.Threading;

namespace Microsoft.Azure.DataLake.Store.RetryPolicies
{
    /// <summary>
    /// RetryPolicy to retry exponentially only for 429 (Too many requests).
    /// This is for non-idempotent operations
    /// </summary>
    public class NonIdempotentRetryPolicy : RetryPolicy
    {
        /// <summary>
        /// Tracks the current number of retries
        /// </summary>
        private int _numberOfRetries;

        /// <summary>
        /// Maximum number of retries
        /// </summary>
        private readonly int _maxRetries;

        /// <summary>
        /// Factor by which we will increase the interval
        /// </summary>
        private readonly int _exponentialFactor;

        /// <summary>
        /// Wait time
        /// </summary>
        private int _exponentialInterval;

        /// <summary>
        /// Default settings of NonIdempotent retry policy
        /// </summary>
        public NonIdempotentRetryPolicy()
        {
            _numberOfRetries = 0;
            _maxRetries = 4;
            _exponentialFactor = 4;
            _exponentialInterval = DefaultRetryInterval;
        }

        /// <summary>
        /// Returns true when http status is 429 - too many requests
        /// </summary>
        /// <param name="httpCode">HttpStatus</param>
        /// <param name="ex">Exception</param>
        /// <returns>True if request needs to retry else false</returns>
        public override bool ShouldRetry(int httpCode, Exception ex)
        {
            if (httpCode == 429)
            {
                if (_numberOfRetries < _maxRetries)
                {
                    Thread.Sleep(_exponentialInterval);
                    _exponentialInterval = _exponentialFactor * _exponentialInterval;
                    _numberOfRetries++;
                    return true;
                }
            }

            return false;
        }
    }
}