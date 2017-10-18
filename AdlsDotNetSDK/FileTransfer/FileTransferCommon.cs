using System;
using System.IO;
using System.Net;
using System.Threading;
using System.Runtime.CompilerServices;
using NLog;
using Microsoft.Azure.DataLake.Store.FileTransfer.Jobs;
using Microsoft.Azure.DataLake.Store.QueueTools;

[assembly: InternalsVisibleTo("Microsoft.Azure.DataLake.Store.UnitTest")]
[assembly: InternalsVisibleTo("TestDataCreator")]
namespace Microsoft.Azure.DataLake.Store.FileTransfer
{
    /// <summary>
    /// Class that immplements the generic logic for upload and download
    /// </summary>
    internal abstract class FileTransferCommon
    {
        protected static readonly Logger FileTransferLog = LogManager.GetLogger("adls.dotnet.FileTransfer");
        /// <summary>
        /// If true then will not recurse under sub directories
        /// </summary>
        protected bool NotRecurse;
        /// <summary>
        /// Number of producer threads
        /// </summary>
        protected int NumProducerThreads;
        /// <summary>
        /// Total number of consumer threads
        /// </summary>
        protected readonly int NumConsumerThreads;
        /// <summary>
        /// Default chunk size
        /// </summary>
        internal const long ChunkSizeDefault = 240 * 1024 * 1024L;
        /// <summary>
        /// Chunk size either default or set by user
        /// </summary>
        internal long ChunkSize { get; }
        /// <summary>
        /// Priority queue containing jobs for Consumer threads
        /// </summary>
        internal PriorityQueueWrapper<Job> ConsumerQueue { get; }

        protected AdlsClient Client { get; }
        /// <summary>
        /// Results of File transfer
        /// </summary>
        protected TransferStatus Status { get; }
        /// <summary>
        /// Source Path
        /// </summary>
        protected string SourcePath { get; set; }
        /// <summary>
        /// Destination path
        /// </summary>
        protected string DestPath { get; set; }
        /// <summary>
        /// Whether to overwrite the file in destination if it exists. If it is false and the file exists then upload or download will be skipped for the file
        /// </summary>
        internal IfExists DoOverwrite { get; }
        /// <summary>
        /// Whether to collect stats
        /// </summary>
        internal IProgress<TransferStatus> ProgressTracker;
        /// <summary>
        /// True if we just want to test ingress or egress of ADL
        /// </summary>
        internal bool IngressOrEgressTest { get; }
        /// <summary>
        /// Producer threads
        /// </summary>
        private Thread[] _threadProducer;
        /// <summary>
        /// Consumer threads
        /// </summary>
        private Thread[] _threadConsumer;
        /// <summary>
        /// Stat collecting threads
        /// </summary>
        private Thread _threadStats;
        /// <summary>
        /// Flag that represents consumer is done- used by stat collection thread
        /// </summary>
        private long _consumerDone;

        private bool _shouldRunProducer;
        
        protected FileTransferCommon(string srcPath, string destPath, AdlsClient client, int numThreads, IfExists doOverwrite, IProgress<TransferStatus> progressTracker,bool notRecurse, bool ingressOrEgressTest, long chunkSize)

        {
            if (string.IsNullOrWhiteSpace(srcPath))
            {
                throw new ArgumentException(nameof(srcPath));
            }
            SourcePath = srcPath;
            DestPath = destPath;
            Client = client;
            // If ingress or egress test then simply overwrite destination
            DoOverwrite = ingressOrEgressTest ? IfExists.Overwrite : doOverwrite;
            ProgressTracker = progressTracker;
            IngressOrEgressTest = ingressOrEgressTest;
            NumConsumerThreads = numThreads < 0 ? AdlsClient.DefaultNumThreads : numThreads;
            ChunkSize = chunkSize;
            NotRecurse = notRecurse;
#if NET452
            ServicePointManager.DefaultConnectionLimit = NumConsumerThreads;
            ServicePointManager.Expect100Continue = false;
            ServicePointManager.UseNagleAlgorithm = false;
            WebRequest.DefaultWebProxy = null;
#endif
            Status = new TransferStatus();
            ConsumerQueue = new PriorityQueueWrapper<Job>();
        }
        /// Adds jobs for concat
        internal abstract void AddConcatJobToQueue(string chunkSegmentFolder, string dest, long totSize,
            long totalChunks);
        /// <summary>
        /// Initializes the producer threads, consumer threads- This is not done in constructor because it has separate immplementation for start enumeration
        /// </summary>
        private void SetupThreads()
        {
            _shouldRunProducer = StartEnumeration();
            _threadConsumer = new Thread[NumConsumerThreads];
            for (int i = 0; i < NumConsumerThreads; i++)
            {
                _threadConsumer[i] = new Thread(ConsumerRun)
                {
                    Name = "ConsumerThread-" + i
                };
            }
            if (ProgressTracker != null)
            {
                _threadStats = new Thread(StatsRun)
                {
                    Name = "StatsThread"
                };
            }
            InitializesProducerThreads(FirstPassProducerRun);
        }
        /// <summary>
        /// Initializes the producer threads with the delegate method
        /// </summary>
        /// <param name="ts">Method that the threads run</param>
        protected void InitializesProducerThreads(ThreadStart ts)
        {
            if (_shouldRunProducer)
            {
                _threadProducer = new Thread[NumProducerThreads];
                for (int i = 0; i < NumProducerThreads; i++)
                {
                    _threadProducer[i] = new Thread(ts)
                    {
                        Name = "ProducerThread-" + i
                    };
                }
            }
        }
        /// <summary>
        /// Starts the producer threads
        /// </summary>
        protected void StartProducer()
        {
            if (_shouldRunProducer)
            {
                for (int i = 0; i < NumProducerThreads; i++)
                {
                    _threadProducer[i].Start();
                }
            }
        }
        /// <summary>
        /// Wait for producer threads to finish
        /// </summary>
        protected void WaitForProducerToFinish()
        {
            if (_shouldRunProducer)
            {
                for (int i = 0; i < NumProducerThreads; i++)
                {
                    _threadProducer[i].Join();
                }
            }
        }
        /// <summary>
        /// Starts the consumer
        /// </summary>
        protected void StartConsumer()
        {
            for (int i = 0; i < NumConsumerThreads; i++)
            {
                _threadConsumer[i].Start();
            }
        }
        /// Main method that does the upload or download
        protected TransferStatus RunTransfer()
        {
            //Sets up the producer threads for first pass producer run and conumer threads
            SetupThreads();
            //Runs the enumeration first and waits for it to end
            StartProducer();
            WaitForProducerToFinish();
            NumProducerThreads = 1;
            // After enumeration we have a better idea the sample distribution of file sizes
            // In second pass we can determine how the chunking will be done for uploader or downloader
            InitializesProducerThreads(FinalPassProducerRun);
            StartProducer();
            //Now the consumer can run in parallel to producer
            StartConsumer();
            WaitForProducerToFinish();

            //Stats thread should only start after enumeration is done
            if (ProgressTracker != null)
            {
                _threadStats.Start();
            }
            //After thread producer ends then only stats thread should start
            ConsumerQueue.Add(new PoisonJob());
            for (int i = 0; i < NumConsumerThreads; i++)
            {
                _threadConsumer[i].Join();
            }
            if (ProgressTracker != null)
            {
                Interlocked.Increment(ref _consumerDone);
                _threadStats.Join();
            }
            return Status;
        }
        /// <summary>
        /// Verifies whether input is a directory or a file. If it is a file then there is no need to start the producer
        /// </summary>
        /// <returns>True if we need to start the producer threads</returns>
        protected abstract bool StartEnumeration();
        /// <summary>
        /// Delegate that runs the producer logic for the first pass. It will do the enumeration first and determine the sample distribution of file sizes. It might add some files directly to job queue
        /// </summary>
        protected abstract void FirstPassProducerRun();
        /// <summary>
        /// Delegate that runs the producer logic for the final pass. It will go over already enumerated list, chunks each file based on a criteria and adds to job queue.
        /// </summary>
        protected abstract void FinalPassProducerRun();
        /// Gets the destination path in correct format
        protected abstract string GetDestDirectoryFormatted(string relPath);
        /// Gets the destination directory separator
        protected abstract char GetDestDirectorySeparator();
        /// Creates the MetaData for uploader or downloader
        internal abstract FileMetaData AssignMetaData(string fileFullName, string chunkSegmentFolder, string destPath,
            long fileLength, long numChunks);

        protected void AddDirectoryToConsumerQueue(string dirFullName, bool isUpload)
        {
            string relativePath = GetDestDirectoryFormatted(dirFullName.Substring(SourcePath.Length));
            string destPath = DestPath + GetDestDirectorySeparator() + relativePath;
            ConsumerQueue.Add(new MakeDirJob(destPath, Client, isUpload));
            if (FileTransferLog.IsDebugEnabled)
            {
                FileTransferLog.Debug($"FileTransfer.DirectoryProduced, {destPath}");
            }
        }
        /// Adds file or its chunks to Consumer queue
        protected long AddFileToConsumerQueue(string fileFullName, long fileLength, bool isToBeChunked)
        {
            string relativePath;
            if (SourcePath.Equals(fileFullName)) //This is the case when the input path is a file
            {
                relativePath = "";
            }
            else
            {
                relativePath = fileFullName.Substring(SourcePath.Length);
                relativePath = GetDestDirectoryFormatted(relativePath);
            }
            long numChunks = 0;
            string tempGuid = null;
            if (isToBeChunked)
            {
                //Create the metadata
                numChunks = fileLength / ChunkSize + (fileLength % ChunkSize == 0 ? 0 : 1);
                tempGuid = Guid.NewGuid().ToString();
                FileMetaData bulk = AssignMetaData(fileFullName, DestPath + relativePath + tempGuid + "Segments", DestPath + relativePath, fileLength, numChunks);
                for (int i = 0; i < numChunks; i++)
                {
                    ConsumerQueue.Add(new CopyFileJob(i, bulk, Client));
                }
            }
            else
            {
                FileMetaData bulk = AssignMetaData(fileFullName, null, DestPath + relativePath, fileLength, numChunks);
                ConsumerQueue.Add(new CopyFileJob(-1, bulk, Client));
            }
            if (FileTransferLog.IsDebugEnabled)
            {
                FileTransferLog.Debug($"FileTransfer.FileProduced, Name: {fileFullName}, Dest: {DestPath + relativePath + (tempGuid != null ? tempGuid + "Segments" : "")}, Length: {fileLength}, Chunks: {numChunks}");
            }
            return numChunks;
        }
        /// Calculates the status update
        protected void StatusUpdate(long files, long unchunkedFile, long chunks, long size, long numDirectories)
        {
            if (files > 0)
            {
                Interlocked.Add(ref Status.TotalFilesToTransfer, files);
            }
            if (size > 0)
            {
                Interlocked.Add(ref Status.TotalSizeToTransfer, size);
            }
            if (unchunkedFile > 0)
            {
                Interlocked.Add(ref Status.TotalNonChunkedFileToTransfer, size);
            }
            if (chunks > 0)
            {
                Interlocked.Add(ref Status.TotalChunksToTransfer, chunks);
            }
            if (numDirectories > 0)
            {
                Interlocked.Add(ref Status.TotalDirectoriesToTransfer,numDirectories);
            }
        }
        /// <summary>
        /// Delegate method run by stats thread
        /// </summary>
        private void StatsRun()
        {
            while (Interlocked.Read(ref _consumerDone) == 0)
            {
                ProgressTracker.Report(Status);
                Thread.Sleep(5000);
            }
        }
        /// <summary>
        /// Consumer method- Polls each job from the job queue, runs the job and stores the transfer result if failed or skipped
        /// </summary>
        private void ConsumerRun()
        {
            while (true)
            {
                var job = ConsumerQueue.Poll();

                if (job is PoisonJob)
                {
                    ConsumerQueue.Add(new PoisonJob());
                    return;
                }
                SingleEntryTransferStatus res = job.DoRun();
                if (res.Status == SingleChunkStatus.Successful)
                {
                    if (res.Type == EntryType.Chunk)
                    {
                        Interlocked.Increment(ref Status.ChunksTransfered);
                    }
                    else if (res.Type == EntryType.File)
                    {
                        if (res.EntrySize != 0)
                        {
                            Interlocked.Increment(ref Status.NonChunkedFileTransferred);
                        }
                        Interlocked.Increment(ref Status.FilesTransfered);
                    }
                    else
                    {
                        Interlocked.Increment(ref Status.DirectoriesTransferred);
                    }
                    if (res.EntrySize > 0)
                    {
                        Interlocked.Add(ref Status.SizeTransfered, res.EntrySize);
                    }
                }
                else if (res.Status == SingleChunkStatus.Failed)
                {
                    Status.AddFailedEntries(res);
                }
                else
                {
                    Status.AddSkippedEntries(res.EntryName);
                }
            }
        }
    }
}