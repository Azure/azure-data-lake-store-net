using System;
using System.Threading;

namespace Microsoft.Azure.DataLake.Store.QueueTools
{
    internal class PriorityQueueWrapper<T> where T : IComparable
    {
        private readonly PriorityQueue<T> _queue;
        private readonly int _totalThreads;
        private int _waitingThreads = 0;
        internal PriorityQueueWrapper(int numThreads):this()
        {
            _totalThreads = numThreads;
        }
        internal PriorityQueueWrapper()
        {
            _queue = new PriorityQueue<T>();
            _totalThreads = -1;
        }
        internal PriorityQueueWrapper(int capacity,int numThreads)
        {
            _queue = new PriorityQueue<T>(capacity);
            _totalThreads = numThreads;
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
                if (_queue.HeapSize <= 0)
                {
                    _waitingThreads++;
                    if (_waitingThreads == _totalThreads) //All threads are waiting
                    {
                        return default(T);
                    }
                    while (_queue.HeapSize <= 0)
                    {
                        Monitor.Wait(_queue);
                    }
                    _waitingThreads--;
                }
                return _queue.GetMax();
            }
        }

    }
}
