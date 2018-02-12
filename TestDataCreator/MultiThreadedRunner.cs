using System.Threading;
using Microsoft.Azure.DataLake.Store.QueueTools;

namespace TestDataCreator
{
    internal delegate void Run(object state);
    internal class MultiThreadedRunner<T> where T : class
    {

        private readonly Run _runMethod;
        private readonly QueueWrapper<T> _queue;
        private Thread[] _threads;
        private bool _isStarted = false;
        internal MultiThreadedRunner(int numThreads, Run r)
        {
            _queue = new QueueWrapper<T>(numThreads);
            _threads = new Thread[numThreads];
            _runMethod = r;
            for (int i = 0; i < numThreads; i++)
            {
                _threads[i] = new Thread(RunProcess);
            }
        }

        internal void AddToQueue(T a)
        {
            _queue.Add(a);
        }
        internal void RunMultiThreaded()
        {
            if (!_isStarted)
            {
                _isStarted = true;
                for (int i = 0; i < _threads.Length; i++)
                {
                    _threads[i].Start();
                }
            }

        }
        internal void StopMultiThreaded()
        {
            if (_isStarted)
            {
                _queue.Add(null);
                for (int i = 0; i < _threads.Length; i++)
                {
                    _threads[i].Join();
                }
            }

        }
        private void RunProcess()
        {
            while (true)
            {
                var job = _queue.Poll();
                if (job == null)
                {
                    _queue.Add(null);
                    return;
                }
                _runMethod(job);
            }
        }
    }
}
