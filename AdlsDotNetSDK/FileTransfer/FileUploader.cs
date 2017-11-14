using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Text;
using System.Threading;
using Microsoft.Azure.DataLake.Store.QueueTools;
using Microsoft.Azure.DataLake.Store.FileTransfer.Jobs;

namespace Microsoft.Azure.DataLake.Store.FileTransfer
{
    /// <summary>
    /// Class that immplements specific logic for Uploader
    /// </summary>
    internal sealed class FileUploader : FileTransferCommon
    {
        /// <summary>
        /// FIFO queue containing directories for producer queue in case of Uploader
        /// </summary>
        private QueueWrapper<DirectoryInfo> UploaderProducerQueue { get; }
        private const int NumProducerThreadsFirstPass = 1;
        private readonly bool _isBinary;
        private readonly Encoding _encodeType;
        internal const string DestTempGuidForConcat = "ConcatGuid";
        private FileUploader(string srcPath, string destPath, AdlsClient client, int numThreads,
            IfExists doOverwrite, IProgress<TransferStatus> progressTracker, bool notRecurse, bool resume, bool isBinary, CancellationToken cancelToken, bool ingressTest, long chunkSize) : base(srcPath, destPath, client, numThreads, doOverwrite, progressTracker, notRecurse, resume, ingressTest, chunkSize, Path.Combine(Path.GetTempPath(), ".adl", "Upload", GetTransferLogFileName(srcPath, destPath,Path.DirectorySeparatorChar,'/')), cancelToken, $"binary:{isBinary}")
        {
            // If not recurse then we will have one thread and ProducerFirstPass logic loop will run only once
            NumProducerThreads = NotRecurse ? 1 : NumProducerThreadsFirstPass;
            UploaderProducerQueue = new QueueWrapper<DirectoryInfo>(NumProducerThreads);
            if (FileTransferLog.IsDebugEnabled)
            {
                FileTransferLog.Debug($"FileTransfer.Uploader, Src: {SourcePath}, Dest: {DestPath}, Threads: {NumConsumerThreads}, TrackingProgress: {ProgressTracker != null}, OverwriteIfExist: {DoOverwrite == IfExists.Overwrite}");
            }
            _isBinary = isBinary;
            _encodeType = Encoding.UTF8;
        }
        /// Replaces the local directory separator in the input path by the directory separator for remote file system
        protected override string GetDestDirectoryFormatted(string ipPath)
        {
            return ipPath.Replace(Path.DirectorySeparatorChar, GetDestDirectorySeparator());
        }
        /// <summary>
        /// Gets the directory separator for remote file system-ADLS
        /// </summary>
        /// <returns></returns>
        protected override char GetDestDirectorySeparator()
        {
            return '/';
        }
        /// <summary>
        /// Upload directory or file from local to remote
        /// </summary>
        /// <param name="srcPath">Local source path</param>
        /// <param name="destPath">Remote destination path</param>
        /// <param name="client">ADLS client</param>
        /// <param name="numThreads">Number of threads- if not passed will take default number of threads</param>
        /// <param name="shouldOverwrite">Whether to overwrite or skip if the destination </param>
        /// <param name="progressTracker">Progresstracker to track progress of file transfer</param>
        /// <param name="notRecurse"> If true then just does a enumeration in first level</param>
        /// <param name="resume">If true we are resuming a previously interrupted upload process</param>
        /// <param name="isBinary">If false then we want to upload at new line boundaries</param>
        /// <param name="cancelToken">Cancellation Token</param>
        /// <param name="ingressTest">True if we just want to test ingress</param>
        /// <param name="chunkSize">Chunk Size used for chunking</param>
        /// <returns>Transfer Status of the upload</returns>
        internal static TransferStatus Upload(string srcPath, string destPath, AdlsClient client, int numThreads = -1, IfExists shouldOverwrite = IfExists.Overwrite, IProgress<TransferStatus> progressTracker = null, bool notRecurse = false, bool resume = false, bool isBinary = false, CancellationToken cancelToken = default(CancellationToken), bool ingressTest = false, long chunkSize = ChunkSizeDefault)
        {
            if (string.IsNullOrWhiteSpace(destPath))
            {
                throw new ArgumentException(nameof(DestPath));
            }
            if (srcPath.EndsWith($"{Path.DirectorySeparatorChar}"))
            {
                srcPath = srcPath.Substring(0, srcPath.Length - 1);
            }
            if (destPath.EndsWith("/"))
            {
                destPath = destPath.Substring(0, destPath.Length - 1);
            }
            var uploader= new FileUploader(srcPath, destPath, client, numThreads, shouldOverwrite, progressTracker, notRecurse, resume, isBinary, cancelToken, ingressTest, chunkSize);
            return uploader.RunTransfer();
        }
        /// Verifies whether input is a directory or a file. If it is a file then there is no need to start the producer
        protected override bool StartEnumeration()
        {
            if (File.Exists(SourcePath))
            {
                var file = new FileInfo(SourcePath);
                long fileSizeToTransfer;
                long chunks = AddFileToConsumerQueue(file.FullName, file.Length, file.Length > ChunkSize, out fileSizeToTransfer);
                StatusUpdate(1, file.Length <= ChunkSize ? 1 : 0, chunks, fileSizeToTransfer, 0);
                return false;
            }
            //Check if the destination is file
            DirectoryEntry entry = null;
            try
            {
                entry = Client.GetDirectoryEntry(DestPath);
            }
            catch (AdlsException e)
            {
                if (e.HttpStatus != HttpStatusCode.NotFound)
                {
                    throw e;
                }
            }
            if (entry?.Type == DirectoryEntryType.FILE)
            {
                throw new IOException("The destination path is an existing file. It should be a directory");
            }
            if (Directory.Exists(SourcePath))
            {
                DirectoryInfo dir = new DirectoryInfo(SourcePath);
                UploaderProducerQueue.Add(dir);
                return true;
            }
            throw new FileNotFoundException(SourcePath);
        }
        /// <summary>
        /// Adds the concat jobs for uploader
        /// </summary>
        /// <param name="source">Source file path</param>
        /// <param name="chunkSegmentFolder">Temporary folder where chunks are located</param>
        /// <param name="dest">Destination file</param>
        /// <param name="totSize">Total size of the file- needed for verification of the copy</param>
        /// <param name="totalChunks">Total number of chunks</param>
        /// <param name="doUploadRenameOnly"></param>
        internal override void AddConcatJobToQueue(string source, string chunkSegmentFolder, string dest, long totSize,
            long totalChunks, bool doUploadRenameOnly =false)
        {
            ConsumerQueue.Add(new ConcatenateJob(source, chunkSegmentFolder, dest, Client, totSize, totalChunks, true, doUploadRenameOnly));
        }
        /// <summary>
        /// Producer code which traverses local directory tree and add them as chunked or non-chunked jobs to job queue depending on it's size. Currently this directly adds 
        /// jobs to job queue but in future we will try to add files to an internal list and add them as jobs in FinalPassProducerRun.
        /// </summary>
        protected override void FirstPassProducerRun()
        {
            do
            {
                if (CancelToken.IsCancellationRequested)
                {
                    return;
                }
                var dir = UploaderProducerQueue.Poll();

                if (dir == null) //Means end of Producer
                {
                    UploaderProducerQueue.Add(null); //Notify if any other threads are waiting
                    return;
                }
                try
                {
                    long numSubDirs = 0, isEmpty=0;
                    IEnumerable<DirectoryInfo> enumDir = dir.EnumerateDirectories();
                    foreach (var subDir in enumDir)
                    {
                        isEmpty = 1;
                        if (NotRecurse) //Directly add the directories to be created since we won't go in recursively
                        {
                            if (!AddDirectoryToConsumerQueue(subDir.FullName, true))
                            {
                                continue;
                            }
                        }
                        else
                        {
                            UploaderProducerQueue.Add(subDir);
                        }
                        numSubDirs++;
                    }

                    IEnumerable<FileInfo> enumFiles = dir.EnumerateFiles();
                    long numFiles = 0, totChunks = 0, unchunkedFiles = 0, totSize = 0;
                    foreach (var file in enumFiles)
                    {
                        isEmpty = 1;
                        if (RecordedMetadata.EntryTransferredSuccessfulLastTime(file.FullName))
                        {
                            continue;
                        }
                        long fileSizeToTransfer;
                        long chunks = AddFileToConsumerQueue(file.FullName, file.Length, file.Length > ChunkSize, out fileSizeToTransfer);
                        totChunks += chunks;
                        if (file.Length <= ChunkSize)
                        {
                            unchunkedFiles++;
                        }
                        numFiles++;
                        totSize += fileSizeToTransfer;
                    }
                    bool isDirectoryEmptyAndNotUploadedYet = false;
                    if (isEmpty == 0)
                    {
                        isDirectoryEmptyAndNotUploadedYet = AddDirectoryToConsumerQueue(dir.FullName, true);
                    }
                    // If there are any directories and it is not recurse update the number of directories
                    StatusUpdate(numFiles, unchunkedFiles, totChunks, totSize, NotRecurse ? numSubDirs : (isDirectoryEmptyAndNotUploadedYet ? 1 : 0));
                }
                catch (Exception ex)
                {
                    Status.EntriesFailed.Add(new SingleEntryTransferStatus(dir.FullName, null, ex.StackTrace,
                        EntryType.Directory, SingleChunkStatus.Failed));
                }
            } while (!NotRecurse);
        }
        /// Creates the MetaData for uploader, alreadyChunksTransferred will be greater than -1 only if a chunked file is being resumed after it is incomplete last time
        internal override FileMetaData AssignMetaData(string fileFullName, string chunkSegmentFolder, string destPath,
            long fileLength, long numChunks, long alreadyChunksTransfered = -1)
        {
            return new FileMetaData(fileFullName, chunkSegmentFolder, destPath, fileLength, this, numChunks, _isBinary, _encodeType, alreadyChunksTransfered);
        }
        /// <summary>
        /// Currently this is not immplemented. We do a constant chunking size. But for future we would like to increase chunking size for 
        /// very large files so do some kind of adaptive chunking.
        /// </summary>
        protected override void FinalPassProducerRun()
        {
            return;
        }
    }
}
