using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using Microsoft.Azure.DataLake.Store.FileTransfer.Jobs;
using Microsoft.Azure.DataLake.Store.QueueTools;

namespace Microsoft.Azure.DataLake.Store.FileTransfer
{
    /// <summary>
    /// Class that immplements specific logic for Downloader
    /// </summary>
    internal sealed class FileDownloader : FileTransferCommon
    {
        /// <summary>
        /// Threshold to determine this is a large file for which we may need chunking for download
        /// </summary>
        private static long ChunkWeightThreshold = 5 * 1024 * 1024 * 1024L;
        /// <summary>
        /// If number of large files is less than this number then only we should chunk large files. Say we have 100 large files then we do not need to do chunking
        /// because anyways all 100 threads will be active during the download
        /// </summary>
        private static long _numLargeFileThreshold = 20;
        /// <summary>
        /// Files with sizes less than this limit will never be chunked
        /// </summary>
        private static long _skipChunkingWeightThreshold = 2 * 1024 * 1024 * 1024L;
        // Number of large files to download- we know this after enumeration
        private int _numLargeFiles;
        // Capacity of the internal list that stores the enumerated files temporarily
        private const int DownloaderListCapacity = 10240;
        // Internal list that stores the enumerated files temporarily
        private List<DirectoryEntry> DownloaderList { get; }

        private const int NumProducerThreadsFirstPass = 20;
        internal int EgressBufferCapacity { get; }
        /// <summary>
        /// FIFO queue containing directories for producer 
        /// </summary>
        private QueueWrapper<DirectoryEntry> DownloaderProducerQueue { get; }

        private FileDownloader(string srcPath, string destPath, AdlsClient client, int numThreads,
            IfExists doOverwrite, IProgress<TransferStatus> progressTracker,bool notRecurse, bool egressTest, int egressBufferCapacity, long chunkSize) : base(srcPath, destPath, client, numThreads, doOverwrite, progressTracker,notRecurse, egressTest, chunkSize)
        {
            if (!egressTest && string.IsNullOrWhiteSpace(DestPath))
            {
                throw new ArgumentException(nameof(DestPath));
            }
            EgressBufferCapacity = egressBufferCapacity;
            if (SourcePath.EndsWith("/"))
            {
                SourcePath = SourcePath.Substring(0, SourcePath.Length - 1);
            }
            if (DestPath.EndsWith("\\"))
            {
                DestPath = DestPath.Substring(0, DestPath.Length - 1);
            }
            // If not recurse then we will have one thread and ProducerFirstPass logic loop will run only once
            NumProducerThreads = notRecurse?1:NumProducerThreadsFirstPass;
            DownloaderProducerQueue = new QueueWrapper<DirectoryEntry>(NumProducerThreads);
            DownloaderList = new List<DirectoryEntry>(DownloaderListCapacity);
            if (FileTransferLog.IsDebugEnabled)
            {
                FileTransferLog.Debug($"FileTransfer.Downloader, Src: {SourcePath}, Dest: {DestPath}, Threads: {NumConsumerThreads}, TrackingProgress: {ProgressTracker != null}, OverwriteIfExist: {DoOverwrite == IfExists.Overwrite}");
            }
        }
        /// <summary>
        /// Verifies whether input is a directory or a file. If it is a file then there is no need to start the producer
        /// </summary>
        /// <returns>True if we need tos tart the producer threads</returns>
        protected override bool StartEnumeration()
        {
            DirectoryEntry dir = Client.GetDirectoryEntry(SourcePath);
            if (dir.Type == DirectoryEntryType.FILE)
            {
                long chunks = AddFileToConsumerQueue(dir.FullName, dir.Length, dir.Length < ChunkWeightThreshold);
                StatusUpdate(1, chunks == 0 ? 1 : 0, chunks, dir.Length, 0);
                return false;
            }
            if (!IngressOrEgressTest && File.Exists(DestPath))
            {
                throw new IOException("The destination path is an existing file. It should be a directory");
            }
            if (!IngressOrEgressTest)
            {
                Directory.CreateDirectory(DestPath);
            }
            DownloaderProducerQueue.Add(dir);
            return true;
        }

        internal static TransferStatus Download(string srcPath, string destPath, AdlsClient client, bool forceChunking, bool forceNotChunking, int numThreads = -1,
            IProgress<TransferStatus> progressTracker = null, IfExists shouldOverwrite = IfExists.Overwrite,bool notRecurse=false, bool egressTest = false, int egressBufferCapacity = 4 * 1024 * 1024, long chunkSize = ChunkSizeDefault)
        {
            if (forceChunking && forceNotChunking)
            {
                throw new ArgumentException("Both of them cant be true");
            }
            if (forceChunking)
            {
                _skipChunkingWeightThreshold = ChunkSizeDefault;
                _numLargeFileThreshold = Int64.MaxValue;
            }
            else if (forceNotChunking)
            {
                _skipChunkingWeightThreshold = Int64.MaxValue;
            }
            return new FileDownloader(srcPath, destPath, client, numThreads, shouldOverwrite, progressTracker,notRecurse, egressTest, egressBufferCapacity, chunkSize).RunTransfer();
        }
        /// <summary>
        /// Download directory or file from remote server to local
        /// </summary>
        /// <param name="srcPath">Remote source path</param>
        /// <param name="destPath">Local destination path</param>
        /// <param name="client">ADLS client</param>
        /// <param name="numThreads">Number of threads- if not passed will take default number of threads</param>
        /// <param name="shouldOverwrite">Whether to overwrite or skip if the destination </param>
        /// <param name="progressTracker">Progresstracker to track progress of file transfer</param>
        /// <param name="notRecurse">If true then does enumeration till one level only, else will do recursive enumeration</param>
        /// <param name="egressTest">Egress test when we do not write file to local file system</param>
        /// <param name="egressBufferCapacity">Egress buffer size - Size of the read reuest from server</param>
        /// <param name="chunkSize">Chunk Size used for chunking</param>
        /// <returns>Transfer status of the download</returns>
        internal static TransferStatus Download(string srcPath, string destPath, AdlsClient client, int numThreads = -1, IfExists shouldOverwrite = IfExists.Overwrite, IProgress<TransferStatus> progressTracker = null,bool notRecurse=false,  bool egressTest = false, int egressBufferCapacity = 4 * 1024 * 1024, long chunkSize = ChunkSizeDefault)
        {
            return new FileDownloader(srcPath, destPath, client, numThreads, shouldOverwrite, progressTracker, notRecurse, egressTest, egressBufferCapacity, chunkSize).RunTransfer();
        }
        /// <summary>
        /// Replaces the remote directory separator in the input path by the directory separator for local file system
        /// </summary>
        /// <param name="relativePath">Input path</param>
        /// <returns></returns>
        protected override string GetDestDirectoryFormatted(string relativePath)
        {
            return relativePath.Replace('/', GetDestDirectorySeparator());
        }
        /// <summary>
        /// Gets the directory separator for local file system
        /// </summary>
        /// <returns></returns>
        protected override char GetDestDirectorySeparator()
        {
            return Path.DirectorySeparatorChar;
        }
        /// <summary>
        /// Adds the concat jobs for downloader
        /// </summary>
        /// <param name="chunkSegmentFolder">Temporary destination file name</param>
        /// <param name="dest">Destination file</param>
        /// <param name="totSize">Total size of the file- needed for verification of the copy</param>
        /// <param name="totalChunks">Total number of chunks</param>
        internal override void AddConcatJobToQueue(string chunkSegmentFolder, string dest, long totSize,
            long totalChunks)
        {
            ConsumerQueue.Add(new ConcatenateJob(chunkSegmentFolder, dest, Client, totSize, totalChunks, false));
        }

        /// <summary>
        /// Adds the directory entry to internal list
        /// </summary>
        /// <param name="dir">DirectoryEntry</param>
        private void AddDirectoryEntryToList(DirectoryEntry dir)
        {
            lock (DownloaderList)
            {
                DownloaderList.Add(dir);
            }
        }
        /// <summary>
        /// Chunking always during Download is not a effecient choice. Because multiple threads writing to different offsets of file on local file system is very slow.
        /// Chunking only makes sense when there are less number of very large files. File sizes greater than ChunkWeightThreshold is defined as large file
        /// If number of files with sizes greater than ChunkWeightThreshold is less than NumLargeFileThreshold then we will do chunking. Also for files whose size is less than DefaultSkipChunkingWeightThreshold there is
        /// no need of chunking. If we have large number of large files then we also can do without chunking.
        /// In first pass producer threads which will traverse directory tree and store the entries in a internal list or add them as non-chunked jobs to job queue depending on it's size
        /// Run on multiple threads
        /// </summary>
        protected override void FirstPassProducerRun()
        {
            do
            {
                var der = DownloaderProducerQueue.Poll();
                if (der == null) //Means end of Producer
                {
                    DownloaderProducerQueue.Add(null); //Notify if any other threads are waiting
                    return;
                }
                try
                {
                    long numDirs = 0, numFiles = 0, totChunks = 0, unchunkedFiles = 0, totSize = 0;
                    var fop = Client.EnumerateDirectory(der.FullName);
                    foreach (var dir in fop)
                    {
                        if (dir.Type == DirectoryEntryType.DIRECTORY)
                        {
                            if (NotRecurse)//Directly add the directories to be created since we won't go in recursively
                            {
                                AddDirectoryToConsumerQueue(dir.FullName, false);
                            }
                            else
                            {
                                DownloaderProducerQueue.Add(dir);
                            }
                            numDirs++;
                        }
                        else
                        {
                            numFiles++;
                            totSize += dir.Length;
                            // If the length is less than skip chunkimg weight threshold then we will add them directly to job queue as non-chunked jobs
                            if (dir.Length <= _skipChunkingWeightThreshold)
                            {
                                long chunks = AddFileToConsumerQueue(dir.FullName, dir.Length, false);
                                totChunks += chunks;
                                if (chunks == 0)
                                {
                                    unchunkedFiles++;
                                }
                            }
                            else // We are not sure, so we will store them in internal list
                            {
                                if (dir.Length > ChunkWeightThreshold)
                                {
                                    Interlocked.Increment(ref _numLargeFiles);
                                }
                                AddDirectoryEntryToList(dir);
                            }
                        }
                    }
                    if (numDirs + numFiles == 0)
                    {
                        AddDirectoryToConsumerQueue(der.FullName, false);
                    }
                    // If there are any sub directories and it is not recurse update the number of directories
                    StatusUpdate(numFiles, unchunkedFiles, totChunks, totSize, (numDirs + numFiles) > 0 ? (NotRecurse
                        ? numDirs : 0) : 1);
                }
                catch (AdlsException ex)
                {
                    Status.EntriesFailed.Add(new SingleEntryTransferStatus(der.FullName, ex.Message,
                        EntryType.Directory, SingleChunkStatus.Failed));
                }
            } while (!NotRecurse);
        }
        /// <summary>
        /// Run by one thread only. Traverse the internal list and add chunked or non-chunked jobs depending on the criteria
        /// </summary>
        protected override void FinalPassProducerRun()
        {
            bool isToBeChunked = _numLargeFiles < _numLargeFileThreshold;
            long totChunks = 0, unchunkedFiles = 0;
            foreach (var dir in DownloaderList)
            {
                long chunks = AddFileToConsumerQueue(dir.FullName, dir.Length, isToBeChunked);
                totChunks += chunks;
                if (chunks == 0)
                {
                    unchunkedFiles++;
                }
            }
            StatusUpdate(0, unchunkedFiles, totChunks, 0, 0);
        }
        // Creates MetaData for downloader
        internal override FileMetaData AssignMetaData(string fileFullName, string chunkSegmentFolder, string destPath,
            long fileLength, long numChunks)
        {
            return new FileMetaData(fileFullName, chunkSegmentFolder, destPath, fileLength, this, numChunks, false, IngressOrEgressTest, EgressBufferCapacity);
        }
    }
}
