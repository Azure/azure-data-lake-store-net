using System.Collections.Generic;
using System.Threading;

namespace Microsoft.Azure.DataLake.Store
{
    /// <summary>
    /// Custom queue class used by thread worker. Each thread worker polls the queue. If it doesn't find an entry
    /// it waits. And after enumeration each worker pushes the directory entry in the queue and notifies the waiting threads.
    /// </summary>
    internal class ContentProcessQueue
    {
        /// <summary>
        /// Internal queue from which the worker thread picks the next directory to process
        /// </summary>
        private readonly Queue<DirectoryEntry> _threadQueue;
        /// <summary>
        /// Tracks number of threads waiting currently
        /// </summary>
        private static int _isWaiting;
        internal ContentProcessQueue()
        {
            _threadQueue = new Queue<DirectoryEntry>();
        }
        /// <summary>
        /// If no directory entries are available current thread waits, else returns the front directory entry in the queue
        /// </summary>
        /// <returns></returns>
        internal DirectoryEntry Poll()
        {
            lock (_threadQueue)
            {
                if (_threadQueue.Count == 0)
                {
                    _isWaiting++;
                    if (_isWaiting == ContentProcessor.NoThreads)//All threads are waiting
                    {
                        return null;
                    }
                    while (_threadQueue.Count == 0)
                    {
                        Monitor.Wait(_threadQueue);
                    }
                    _isWaiting--;
                }
                return _threadQueue.Dequeue();
            }
        }
        /// <summary>
        /// Pushes the directory entry to the back of the queue and notifies any waiting queue
        /// </summary>
        /// <param name="der">DirectoryEntry</param>
        internal void Enqueue(DirectoryEntry der)
        {
            lock (_threadQueue)
            {
                _threadQueue.Enqueue(der);
                Monitor.Pulse(_threadQueue);
            }

        }
        /// <summary>
        /// Returns the queue size
        /// </summary>
        /// <returns></returns>
        internal int QueueSize()
        {
            lock (_threadQueue)
            {
                return _threadQueue.Count;
            }
        }
    }
}
