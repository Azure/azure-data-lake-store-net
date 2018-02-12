using System;
using System.Net;
using System.Threading;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using NLog;
using Microsoft.Azure.DataLake.Store.FileTransfer.Jobs;
using Microsoft.Azure.DataLake.Store.QueueTools;

namespace Microsoft.Azure.DataLake.Store.FileTransfer
{
    /// <summary>
    /// Class that immplements the generic logic for upload and download
    /// </summary>
    internal abstract class FileTransferCommon
    {
        protected static readonly Logger FileTransferLog = LogManager.GetLogger("adls.dotnet.FileTransfer");
        private static readonly Logger JobLog = LogManager.GetLogger("adls.dotnet.FileTransfer.Job");
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

        internal const int HashStringBuilderLength = 40;
        /// <summary>
        /// Chunk size either default or set by user
        /// </summary>
        internal long ChunkSize { get; }
        /// <summary>
        /// Priority queue containing jobs for Consumer threads
        /// </summary>
        internal PriorityQueueWrapper<BaseJob> ConsumerQueue { get; }

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
        protected readonly TransferLog RecordedMetadata;
        protected const string TransferLogFileSeparator = "-";
        protected CancellationToken CancelToken;
        protected FileTransferCommon(string srcPath, string destPath, AdlsClient client, int numThreads, IfExists doOverwrite, IProgress<TransferStatus> progressTracker, bool notRecurse, bool resume, bool ingressOrEgressTest, long chunkSize, string metaDataPath, CancellationToken cancelToken, string metaDataInfo = null)
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
            metaDataInfo = string.IsNullOrEmpty(metaDataInfo) ? ChunkSize.ToString() : metaDataInfo + $",ChunkSize:{chunkSize},{(NotRecurse ? "NotRecurse" : "Recurse")}";
            RecordedMetadata = new TransferLog(resume, metaDataPath, metaDataInfo);
            Status = new TransferStatus();
            ConsumerQueue = new PriorityQueueWrapper<BaseJob>();
            CancelToken = cancelToken;
        }
        // Gets the metadata path where the metadata is stored for transfer to resume. For upload, it will be the source path appended with the destination directory or file.
        // For download it will be the destination path appended with source directory or file. Hash the filename so that it does not cross MAX_CHAR limit
        internal static string GetTransferLogFileName(string sourcePath, string destPath, char sourceSeparator, char destSeparator)
        {
            string separator = Regex.Escape($"{sourceSeparator}{destSeparator}");
            var regex = new Regex($"[:{separator}]");
            return Sha1HashString($"{regex.Replace(sourcePath, TransferLogFileSeparator)}{TransferLogFileSeparator}{regex.Replace(destPath, TransferLogFileSeparator)}-transfer.dat");
        }
        // Uses SHA1 to hash the filename
        private static string Sha1HashString(string fileName)
        {
            var sha1 = SHA1.Create();
            byte[] hashBytes = sha1.ComputeHash(Encoding.UTF8.GetBytes(fileName));
            var sb = new StringBuilder(HashStringBuilderLength);
            foreach (byte b in hashBytes)
            {
                var hex = b.ToString("x2");
                sb.Append(hex);
            }
            return sb.ToString();
        }
        /// Adds jobs for concat
        internal abstract void AddConcatJobToQueue(string source, string chunkSegmentFolder, string dest, long totSize, long totalChunks, bool doUploadRenameOnly = false);
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
            if (CancelToken.IsCancellationRequested)
            {
                return Status;
            }
            //Runs the enumeration first and waits for it to end
            StartProducer();
            WaitForProducerToFinish();
            NumProducerThreads = 1;
            // After enumeration we have a better idea the sample distribution of file sizes
            // In second pass we can determine how the chunking will be done for uploader or downloader
            InitializesProducerThreads(FinalPassProducerRun);
            if (CancelToken.IsCancellationRequested)
            {
                return Status;
            }
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
            RecordedMetadata.EndRecording(CancelToken.IsCancellationRequested);
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
        /// Creates the MetaData for uploader or downloader, alreadyChunksTransferred will be greater than -1 only if a chunked file is being resumed after it is incomplete last time
        internal abstract FileMetaData AssignMetaData(string fileFullName, string chunkSegmentFolder, string destPath, long fileLength, long numChunks, long alreadyChunksTransferred = -1);
        internal void AddBeginRecord(string source, string segmentFolder)
        {
            RecordedMetadata.AddRecord($"BEGIN{TransferLog.MetaDataDelimiter}{source}{TransferLog.MetaDataDelimiter}{segmentFolder}");
        }

        internal void AddCompleteRecord(string source, bool autoFlush = false)
        {
            RecordedMetadata.AddRecord($"COMPLETE{TransferLog.MetaDataDelimiter}{source}", autoFlush);
        }
        // If the transfer process is being resumed and it was successfully transfered before then we return false- meaning 
        // it does not need to be transfered
        protected bool AddDirectoryToConsumerQueue(string dirFullName, bool isUpload)
        {
            if (RecordedMetadata.EntryTransferredSuccessfulLastTime(dirFullName))
            {
                return false;
            }
            string relativePath = GetDestDirectoryFormatted(dirFullName.Substring(SourcePath.Length));
            string destPath = DestPath + GetDestDirectorySeparator() + relativePath;
            ConsumerQueue.Add(new MakeDirJob(dirFullName, destPath, Client, isUpload));
            if (FileTransferLog.IsDebugEnabled)
            {
                FileTransferLog.Debug($"FileTransfer.DirectoryProduced, {destPath}");
            }
            return true;
        }
        /// Adds file or its chunks to Consumer queue
        protected long AddFileToConsumerQueue(string fileFullName, long fileLength, bool isToBeChunked, out long fileSizeToTransfer)
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
            string tempChunkFolder = null;
            fileSizeToTransfer = 0;
            // We will never be here if the log has detected that the file is done
            if (isToBeChunked)
            {
                //If RecordedMetadata.EntryTransferAttemptedLastTime(fileFullName) is true then the file was attempted and incomplete. Because if it is successful then we will not be here
                int numChunksAlreadyTransferred = RecordedMetadata.EntryTransferAttemptedLastTime(fileFullName) ? RecordedMetadata.LoadedMetaData[fileFullName].Chunks.Count : -1;
                // numChunksAlreadyTransferred is -1 means this file was not attempted before so effectively 0 chunks were done
                numChunks = fileLength / ChunkSize + (fileLength % ChunkSize == 0 ? 0 : 1);
                tempChunkFolder = RecordedMetadata.EntryTransferAttemptedLastTime(fileFullName) ? RecordedMetadata.LoadedMetaData[fileFullName].SegmentFolder : DestPath + relativePath + Guid.NewGuid() + "Segments";
                var bulk = AssignMetaData(fileFullName, tempChunkFolder, DestPath + relativePath, fileLength, numChunks, numChunksAlreadyTransferred);
                if (numChunksAlreadyTransferred == numChunks)
                {// If all chunks were transferred correctly then add a CopyFileJob only. CopyFileJob will make the necessary checks and determine which state we are in
                    ConsumerQueue.Add(new CopyFileJob(0, bulk, Client));
                }
                else
                {
                    // If this file was attempted in last transfer and it is being resumed, at the enumeration time we just add the jobs for the chunks which are not 
                    // reported in the log file. In reality there can be different states 1) Only those reported in resume file are done 2) More than reported are done however concat wasn't started yet
                    // 3) All chunks are actually done and concat is half done 4) All chunks are done and concat is done. The discrepancy between the resume file and actual events is because the
                    // log file is written in a separate producer-consumer queue (not serialized) for perf reasons. All these cases checked and are taken care in FileMetaData.
                    // If we are in state 1 and 2 then the jobs are done as expected. If we are in states beyond 2 then we do the required concat job and chunks
                    // are reported done without any actual transfer.
                    for (int i = 0; i < numChunks; i++)
                    {
                        if (RecordedMetadata.EntryTransferAttemptedLastTime(fileFullName) && RecordedMetadata.LoadedMetaData[fileFullName].Chunks.Contains(i))
                        {
                            continue;
                        }
                        ConsumerQueue.Add(new CopyFileJob(i, bulk, Client));
                        fileSizeToTransfer += i == numChunks - 1 ? fileLength - i * ChunkSize : ChunkSize;
                    }
                }
                // Number of chunks to be uploaded for this file will be the total chunks minus the chunks already transferred
                numChunks -= numChunksAlreadyTransferred < 0 ? 0 : numChunksAlreadyTransferred;
            }
            else
            {
                FileMetaData bulk = AssignMetaData(fileFullName, null, DestPath + relativePath, fileLength, numChunks);
                ConsumerQueue.Add(new CopyFileJob(-1, bulk, Client));
                fileSizeToTransfer = fileLength;
            }
            if (FileTransferLog.IsDebugEnabled)
            {
                FileTransferLog.Debug($"FileTransfer.FileProduced, Name: {fileFullName}, Dest: {tempChunkFolder ?? DestPath + relativePath}, Length: {fileLength}, Chunks: {numChunks}");
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
                Interlocked.Add(ref Status.TotalNonChunkedFileToTransfer, unchunkedFile);
            }
            if (chunks > 0)
            {
                Interlocked.Add(ref Status.TotalChunksToTransfer, chunks);
            }
            if (numDirectories > 0)
            {
                Interlocked.Add(ref Status.TotalDirectoriesToTransfer, numDirectories);
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
                Thread.Sleep(50);
            }
        }
        /// <summary>
        /// Consumer method- Polls each job from the job queue, runs the job and stores the transfer result if failed or skipped
        /// </summary>
        private void ConsumerRun()
        {
            while (true)
            {
                if (CancelToken.IsCancellationRequested)
                {
                    return;
                }
                var job = ConsumerQueue.Poll();

                if (job is PoisonJob)
                {
                    ConsumerQueue.Add(new PoisonJob());
                    return;
                }
                var res = job.DoRun(JobLog) as SingleEntryTransferStatus;
                if (res == null)
                {
                    continue;
                }
                if (res.Status == SingleChunkStatus.Successful)
                {
                    if (res.Type == EntryType.Chunk)
                    {
                        Interlocked.Increment(ref Status.ChunksTransfered);
                        RecordedMetadata.AddRecord($"CHUNK{TransferLog.MetaDataDelimiter}{res.Source}{TransferLog.MetaDataDelimiter}{res.ChunkId}");
                    }
                    else if (res.Type == EntryType.File)
                    {
                        // Entry size is zero for concat
                        if (res.EntrySize != 0)
                        {
                            Interlocked.Increment(ref Status.NonChunkedFileTransferred);
                        }
                        Interlocked.Increment(ref Status.FilesTransfered);
                        // For successful concat we want to flush the metadata records
                        AddCompleteRecord(res.Source, res.EntrySize == 0);
                    }
                    else
                    {
                        Interlocked.Increment(ref Status.DirectoriesTransferred);
                        AddCompleteRecord(res.Source);
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