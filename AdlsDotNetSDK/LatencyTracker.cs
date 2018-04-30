using System;
using System.Collections.Generic;
using System.Text;

namespace Microsoft.Azure.DataLake.Store
{
    /// <summary>
    /// Single entry, comma separated:
    ///      1. Client Request ID
    ///      2. latency in milliseconds
    ///      3. error code(if request failed)
    ///      4. Operation
    ///      5. Request+response body Size(if available, zero otherwise)
    ///      6. Instance of ADLStoreClient(a unique number per instance in this VM)
    ///
    ///     Multiple entries can be on a single request.Entries will be separated by semicolons
    ///     Limit max entries on a single request to three, to limit increase in HTTP request size.
    /// 
    /// All apis are threadsafe.
    /// </summary>
    public class LatencyTracker
    {
        /// <summary>
        /// Queue containing the latency entries
        /// </summary>
        private static readonly Queue<string> LatencyQueue = new Queue<string>();
        /// <summary>
        /// Maximum size of the latency queue
        /// </summary>
        private const int Maxsize = 256;
        /// <summary>
        /// Maximum number of latency entries in one request
        /// </summary>
        private const int Maxperline = 3;
        /// <summary>
        /// Tracks whether latencyTracker is disabled
        /// </summary>
        private static bool _disabled;
        /// <summary>
        /// Disables the Latency tracker
        /// </summary>
        public static void Disable()
        {
            lock (LatencyQueue)
            {
                _disabled = true;
                LatencyQueue.Clear();
            }
        }
        /// <summary>
        /// Adds a latency entry to the back of queue
        /// </summary>
        /// <param name="value">Value to add</param>
        private static void Add(string value)
        {
            //Need a lock operation because doing AddIf
            lock (LatencyQueue)
            {
                if (LatencyQueue.Count < Maxsize)
                {
                    LatencyQueue.Enqueue(value);
                }
            }
        }
        /// <summary>
        /// Returns the front element of queue if there is any else returns null
        /// </summary>
        /// <returns>Front element of queue if there is else null</returns>
        private static string Dequeue()
        {
            lock (LatencyQueue)
            {
                if (LatencyQueue.Count > 0)
                {
                    return LatencyQueue.Dequeue();
                }
                return null;
            }
        }
        /// <summary>
        /// Add error/latency details of last http request to the queue
        /// </summary>
        /// <param name="clientRequestId">Client request GUID</param>
        /// <param name="retry">Retry number</param>
        /// <param name="latency">Total latency</param>
        /// <param name="error">Error</param>
        /// <param name="opCode">OpCode of the Http request</param>
        /// <param name="length">Data length+Response length</param>
        /// <param name="clientId">ADLS Client Id</param>
        internal static void AddLatency(string clientRequestId, int retry, long latency, string error, string opCode, long length, long clientId)
        {
            if (_disabled) return;
            Add(clientRequestId + "." + retry + "," + latency + "," + (string.IsNullOrEmpty(error) ? "" : error) + "," + opCode + "," + length + "," + clientId);
        }
        /// <summary>
        /// Retrieves the latency/error entries for upto maximum last 3 requests
        /// </summary>
        /// <returns></returns>
        internal static string GetLatency()
        {
            lock (LatencyQueue)
            {
                if (_disabled) return null;
            }
            int line = 0;
            StringBuilder builder = new StringBuilder(2 * Maxperline);
            do
            {
                string entry = Dequeue();
                if (entry == null)
                {
                    break;
                }
                builder.Append(builder.Length == 0 ? entry : ";" + entry);
            } while (++line < Maxperline);
            return builder.ToString();
        }
    }
}
