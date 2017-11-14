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
        internal static long ChunkWeightThreshold = 5 * 1024 * 1024 * 1024L;
        /// <summary>
        /// If number of large files is less than this number then only we should chunk large files. Say we have 100 large files then we do not need to do chunking
        /// because anyways all 100 threads will be active during the download
        /// </summary>
        internal static long NumLargeFileThreshold = 20;
        /// <summary>
        /// Files with sizes less than this limit will never be chunked
        /// </summary>
        internal static long SkipChunkingWeightThreshold = 1 * 1024 * 1024 * 1024L;
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
            IfExists doOverwrite, IProgress<TransferStatus> progressTracker, bool notRecurse, bool resume, CancellationToken cancelToken, bool egressTest, int egressBufferCapacity, long chunkSize) : base(srcPath, destPath, client, numThreads, doOverwrite, progressTracker, notRecurse, resume, egressTest, chunkSize, Path.Combine(Path.GetTempPath(), ".adl", "Download", GetTransferLogFileName(srcPath, destPath, '/', Path.DirectorySeparatorChar)), cancelToken)
        {
            
            EgressBufferCapacity = egressBufferCapacity;
            
            // If not recurse then we will have one thread and ProducerFirstPass logic loop will run only once
            NumProducerThreads = notRecurse ? 1 : NumProducerThreadsFirstPass;
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
                long fileSizeToTransfer;
                long chunks = AddFileToConsumerQueue(dir.FullName, dir.Length, dir.Length > SkipChunkingWeightThreshold, out fileSizeToTransfer);
                StatusUpdate(1, dir.Length <= SkipChunkingWeightThreshold ? 1 : 0, chunks, fileSizeToTransfer, 0);
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
        #region UnitTestMethods
        // Testing purpose
        internal static TransferStatus Download(string srcPath, string destPath, AdlsClient client, bool forceChunking, bool forceNotChunking, int numThreads = -1,
            IProgress<TransferStatus> progressTracker = null, IfExists shouldOverwrite = IfExists.Overwrite, bool notRecurse = false, bool resume = false, bool egressTest = false, int egressBufferCapacity = 4 * 1024 * 1024, long chunkSize = ChunkSizeDefault)
        {
            if (forceChunking && forceNotChunking)
            {
                throw new ArgumentException("Both of them cant be true");
            }
            if (forceChunking)
            {
                SkipChunkingWeightThreshold = ChunkSizeDefault;
                NumLargeFileThreshold = Int64.MaxValue;
            }
            else if (forceNotChunking)
            {
                SkipChunkingWeightThreshold = Int64.MaxValue;
            }
            return new FileDownloader(srcPath, destPath, client, numThreads, shouldOverwrite, progressTracker, notRecurse, resume, default(CancellationToken), egressTest, egressBufferCapacity, chunkSize).RunTransfer();
        }
        #endregion
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
        /// <param name="resume">If true we are resuming a previously interrupted upload process</param>
        /// <param name="cancelToken">Cancellation Token</param>
        /// <param name="egressTest">Egress test when we do not write file to local file system</param>
        /// <param name="egressBufferCapacity">Egress buffer size - Size of the read reuest from server</param>
        /// <param name="chunkSize">Chunk Size used for chunking</param>
        /// <returns>Transfer status of the download</returns>
        internal static TransferStatus Download(string srcPath, string destPath, AdlsClient client, int numThreads = -1, IfExists shouldOverwrite = IfExists.Overwrite, IProgress<TransferStatus> progressTracker = null, bool notRecurse = false, bool resume = false, CancellationToken cancelToken=default(CancellationToken), bool egressTest = false, int egressBufferCapacity = 4 * 1024 * 1024, long chunkSize = ChunkSizeDefault)
        {
            if (!egressTest && string.IsNullOrWhiteSpace(destPath))
            {
                throw new ArgumentException(nameof(DestPath));
            }
            if (srcPath.EndsWith("/"))
            {
                srcPath = srcPath.Substring(0, srcPath.Length - 1);
            }
            if (destPath.EndsWith($"{Path.DirectorySeparatorChar}"))
            {
                destPath = destPath.Substring(0, destPath.Length - 1);
            }
            var downloader = new FileDownloader(srcPath, destPath, client, numThreads, shouldOverwrite, progressTracker,
                notRecurse, resume, cancelToken, egressTest, egressBufferCapacity, chunkSize);
            return downloader.RunTransfer();
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
        /// <param name="source">Source file path</param>
        /// <param name="chunkSegmentFolder">Temporary destination file name</param>
        /// <param name="dest">Destination file</param>
        /// <param name="totSize">Total size of the file- needed for verification of the copy</param>
        /// <param name="totalChunks">Total number of chunks</param>
        /// <param name="doUploadRenameOnly"></param>
        internal override void AddConcatJobToQueue(string source, string chunkSegmentFolder, string dest, long totSize,
            long totalChunks, bool doUploadRenameOnly=false)
        {
            ConsumerQueue.Add(new ConcatenateJob(source, chunkSegmentFolder, dest, Client, totSize, totalChunks, false));
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
                if (CancelToken.IsCancellationRequested)
                {
                    return;
                }
                var der = DownloaderProducerQueue.Poll();
                if (der == null) //Means end of Producer
                {
                    DownloaderProducerQueue.Add(null); //Notify if any other threads are waiting
                    return;
                }
                try
                {
                    long numDirs = 0, numFiles = 0, totChunks = 0, unchunkedFiles = 0, totSize = 0, isEmpty =0;
                    var fop = Client.EnumerateDirectory(der.FullName);
                    foreach (var dir in fop)
                    {
                        isEmpty = 1;
                        if (dir.Type == DirectoryEntryType.DIRECTORY)
                        {
                            if (NotRecurse)//Directly add the directories to be created since we won't go in recursively
                            {
                                if (!AddDirectoryToConsumerQueue(dir.FullName, false))
                                {
                                    continue;
                                }
                            }
                            else
                            {
                                DownloaderProducerQueue.Add(dir);
                            }
                            numDirs++;
                        }
                        else
                        {
                            if (RecordedMetadata.EntryTransferredSuccessfulLastTime(dir.FullName))
                            {
                                continue;
                            }
                            // We calculate the total files here only even though some files are chunked or non-chunked in the final producer pass
                            numFiles++;
                            long fileSizeToTransfer = 0;
                            // If we are resuming and last time we chunked this file and it is incomplete so we want to chunk it this time also
                            if (RecordedMetadata.EntryTransferredIncompleteLastTime(dir.FullName))
                            {
                                long chunks = AddFileToConsumerQueue(dir.FullName, dir.Length, true, out fileSizeToTransfer);
                                totChunks += chunks;
                            }
                            // If the length is less than skip chunking weight threshold then we will add them directly to job queue as non-chunked jobs
                            else if (dir.Length <= SkipChunkingWeightThreshold)
                            {
                                AddFileToConsumerQueue(dir.FullName, dir.Length, false, out fileSizeToTransfer);
                                unchunkedFiles++;
                            }// We will only update the totSize based on number of chunks or unchunked files that will get transfered this turn
                            else // We are not sure, so we will store them in internal list
                            {
                                if (dir.Length > ChunkWeightThreshold)
                                {
                                    Interlocked.Increment(ref _numLargeFiles);
                                }
                                AddDirectoryEntryToList(dir);
                            }
                            totSize += fileSizeToTransfer;
                        }
                    }
                    bool isDirectoryEmptyAndNotDownloadedYet = false;
                    if (isEmpty == 0)
                    {
                        isDirectoryEmptyAndNotDownloadedYet= AddDirectoryToConsumerQueue(der.FullName, false);
                        
                    }
                    // If there are any sub directories and it is not recurse update the number of directories
                    StatusUpdate(numFiles, unchunkedFiles, totChunks, totSize, NotRecurse ? numDirs : (isDirectoryEmptyAndNotDownloadedYet ? 1 : 0));
                }
                catch (AdlsException ex)
                {
                    Status.EntriesFailed.Add(new SingleEntryTransferStatus(der.FullName, null, ex.Message,
                        EntryType.Directory, SingleChunkStatus.Failed));
                }
            } while (!NotRecurse);
        }
        /// <summary>
        /// Run by one thread only. Traverse the internal list and add chunked or non-chunked jobs depending on the criteria
        /// </summary>
        protected override void FinalPassProducerRun()
        {
            bool isToBeChunked = _numLargeFiles < NumLargeFileThreshold;
            long totChunks = 0, unchunkedFiles = 0, totSize = 0;
            foreach (var dir in DownloaderList)
            {
                if (CancelToken.IsCancellationRequested)
                {
                    return;
                }
                long fileSizeToTransfer;
                long chunks = AddFileToConsumerQueue(dir.FullName, dir.Length, isToBeChunked, out fileSizeToTransfer);
                totChunks += chunks;
                totSize += fileSizeToTransfer;
                if (!isToBeChunked)
                {
                    unchunkedFiles++;
                }
            }
            StatusUpdate(0, unchunkedFiles, totChunks, totSize, 0);
        }
        // Creates MetaData for downloader, alreadyChunksTransferred will be greater than -1 only if a chunked file is being resumed after it is incomplete last time
        internal override FileMetaData AssignMetaData(string fileFullName, string chunkSegmentFolder, string destPath, long fileLength, long numChunks, long alreadyChunksTransfered = -1)
        {
            return new FileMetaData(fileFullName, chunkSegmentFolder, destPath, fileLength, this, numChunks, alreadyChunksTransfered, IngressOrEgressTest, EgressBufferCapacity);
        }
    }
}
