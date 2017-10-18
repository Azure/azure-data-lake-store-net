using System;
using System.Threading;

namespace Microsoft.Azure.DataLake.Store.QueueTools
{
    internal class PriorityQueueWrapper<T> where T : IComparable
    {
        private readonly PriorityQueue<T> _queue;

        internal PriorityQueueWrapper()
        {
            _queue = new PriorityQueue<T>();
        }
        internal PriorityQueueWrapper(int capacity)
        {
            _queue = new PriorityQueue<T>(capacity);
        }
        internal int Size()
        {
            lock (_queue)
            {
                return _queue.HeapSize;
            }
        }
        internal void Add(T job)
        {
            lock (_queue)
            {
                _queue.Add(job);
                Monitor.Pulse(_queue);
            }
        }

        internal T Poll()
        {
            lock (_queue)
            {
                while (_queue.HeapSize <= 0)
                {
                    Monitor.Wait(_queue);
                }
                return _queue.GetMax();
            }
        }

    }
}
