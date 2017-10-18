using System.Collections.Generic;
using System.Threading;

namespace Microsoft.Azure.DataLake.Store.QueueTools
{
    internal class QueueWrapper<T> where T:class
    {
        private readonly Queue<T> _dirList;
        private const int InitialCapacity = 10240;
        private int _isWaiting;
        private int TotalThreads { get; }
        internal QueueWrapper(int numThreads)
        {
            TotalThreads = numThreads;
            _dirList = new Queue<T>(InitialCapacity);
        }
        internal void Add(T dir)
        {
            lock (_dirList)
            {
                _dirList.Enqueue(dir);
                Monitor.Pulse(_dirList);
            }
        }

        internal T Poll()
        {
            lock (_dirList)
            {
                if (_dirList.Count == 0)
                {
                    _isWaiting++;
                    if (_isWaiting == TotalThreads) //All threads are waiting
                    {
                        return null;
                    }
                    while (_dirList.Count == 0)
                    {
                        Monitor.Wait(_dirList);
                    }
                    _isWaiting--;
                }
                return _dirList.Dequeue();
            }
        }
    }
}
