using Microsoft.Azure.DataLake.Store.QueueTools;
using System;
using System.Net;
using System.Threading;

namespace Microsoft.Azure.DataLake.Store
{
    /// <summary>
    /// Computes the content summary of the directory
    /// </summary>
    internal class ContentProcessor
    {
        /// <summary>
        /// Mutex Lock object for doing synchronized setting and getting the clientexception
        /// </summary>
        private readonly Object _thisLock = new Object();
        /// <summary>
        /// Total number of thread workers
        /// </summary>
        internal int NumThreads;
        /// <summary>
        /// Array of thread workers
        /// </summary>
        private readonly Thread[] _threadWorker;
        /// <summary>
        /// Queue for containing the directory entries picked by the thread worker
        /// </summary>
        private readonly QueueWrapper<DirectoryEntry> _queue;
        /// <summary>
        /// Client exception if any raised by any thread
        /// </summary>
        private AdlsException _clientException;
        /// <summary>
        /// Cancellation Token
        /// </summary>
        private CancellationToken CancelToken { get; }
        /// <summary>
        /// ADLS client
        /// </summary>
        private AdlsClient Client { get; }
        /// <summary>
        /// Root path whose content summary we need
        /// </summary>
        private string RootPath { get; }
        //Tracks total directory count
        private long _directoryCount;
        /// <summary>
        /// Tracks total file count
        /// </summary>
        private long _fileCount;
        /// <summary>
        /// Tracks total size
        /// </summary>
        private long _totalBytes;
        /// <summary>
        /// internal API that gets the content summary for a path
        /// </summary>
        /// <param name="client">ADLS Client</param>
        /// <param name="path">Path of the directory or file</param>
        /// <param name="numThreads"> Number of threads</param>
        /// <param name="cancelToken">Cacellation Token</param>
        /// <returns>Content summary</returns>
        internal static ContentSummary GetContentSummary(AdlsClient client, string path, int numThreads=-1,
            CancellationToken cancelToken = default(CancellationToken))
        {
            return new ContentProcessor(client, path,numThreads, cancelToken).GetContentSummary();
        }

        private ContentProcessor(AdlsClient client, string path,int numThreads, CancellationToken cancelToken = default(CancellationToken))
        {
            Client = client;
            CancelToken = cancelToken;
            NumThreads = numThreads < 0 ? AdlsClient.DefaultNumThreads : numThreads;
            _threadWorker = new Thread[NumThreads];
            for (int i = 0; i < NumThreads; i++)
            {
                _threadWorker[i] = new Thread(Run)
                {
                    Name = "Thread-" + i
                };
            }
            _queue = new QueueWrapper<DirectoryEntry>(NumThreads);
            RootPath = path;
        }
        /// <summary>
        /// Starts each thread worker. Waits for each thread worker to finish. If there was an exception throws it.
        /// Else returns a contentsummary
        /// </summary>
        /// <returns>Content summary-Total file count, directory count, total size</returns>
        private ContentSummary GetContentSummary()
        {
            _queue.Add(new DirectoryEntry(RootPath));
            for (int i = 0; i < NumThreads; i++)
            {
                _threadWorker[i].Start();
            }
            for (int i = 0; i < NumThreads; i++)
            {
                _threadWorker[i].Join();
            }
            if (_clientException != null)//No need to lock here
            {
                throw _clientException;
            }
            return new ContentSummary(_directoryCount, _fileCount, _totalBytes, _totalBytes);
        }
        /// <summary>
        /// Atomically sets the client exception
        /// </summary>
        /// <param name="ex"></param>
        private void SetException(AdlsException ex)
        {
            lock (_thisLock)
            {
                _clientException = ex;
            }
        }
        /// <summary>
        /// Atomically gets the client exception
        /// </summary>
        /// <returns></returns>
        private AdlsException GetException()
        {
            lock (_thisLock)
            {
                return _clientException;
            }
        }
        /// <summary>
        /// The run method of each thread worker. It polls for a directory from the queue. Then calls listStatus for that directory.
        /// If it gets any sub-directory it adds it to the queue so that it can be processed again later.For each file/sub-directory it updates the file/directory/size variables
        /// </summary>
        private void Run()
        {
            while (true)
            {
                DirectoryEntry der = _queue.Poll();
                //GetException should be put here because some threads might be in waiting state and come back and see exception
                if (GetException() != null || der == null)//der==null: Time to finish as all other threads have no entries
                {
                    _queue.Add(null);//Poison block to notify other threads to close
                    return;
                }
                if (CancelToken.IsCancellationRequested)//Check if operation is cancelled
                {
                    AdlsException excep = new AdlsException("Content summary processing cancelled")
                    {
                        Ex = new OperationCanceledException()
                    };
                    SetException(excep);
                    _queue.Add(null);
                    return;
                }
                try
                {
                    foreach (var dir in Client.EnumerateDirectory(der.FullName))
                    {
                        if (dir.Type == DirectoryEntryType.DIRECTORY)
                        {
                            Interlocked.Increment(ref _directoryCount);
                            _queue.Add(dir);
                        }
                        else
                        {
                            Interlocked.Increment(ref _fileCount);
                            Interlocked.Add(ref _totalBytes, dir.Length);
                        }
                    }
                }
                catch (AdlsException ex)
                {
                    if (ex.HttpStatus != HttpStatusCode.NotFound)//Do not stop summary if the file is deleted
                    {
                        SetException(ex);//Sets the global exception to signal other threads to close
                        _queue.Add(null);//Handle corner cases like when exception is raised other threads can be in wait state
                        return;
                    }
                }
            }
        }
    }
}
