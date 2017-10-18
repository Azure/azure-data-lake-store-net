using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
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
        private FileUploader(string srcPath, string destPath, AdlsClient client, int numThreads,
            IfExists doOverwrite, IProgress<TransferStatus> progressTracker,bool notRecurse, bool ingressTest, long chunkSize) : base(srcPath, destPath, client, numThreads, doOverwrite, progressTracker,notRecurse, ingressTest, chunkSize)
        {
            if (string.IsNullOrWhiteSpace(DestPath))
            {
                throw new ArgumentException(nameof(DestPath));
            }
            if (SourcePath.EndsWith("\\"))
            {
                SourcePath = SourcePath.Substring(0, SourcePath.Length - 1);
            }
            if (DestPath.EndsWith("/"))
            {
                DestPath = DestPath.Substring(0, DestPath.Length - 1);
            }
            // If not recurse then we will have one thread and ProducerFirstPass logic loop will run only once
            NumProducerThreads = NotRecurse?1:NumProducerThreadsFirstPass;
            UploaderProducerQueue = new QueueWrapper<DirectoryInfo>(NumProducerThreads);
            if (FileTransferLog.IsDebugEnabled)
            {
                FileTransferLog.Debug($"FileTransfer.Uploader, Src: {SourcePath}, Dest: {DestPath}, Threads: {NumConsumerThreads}, TrackingProgress: {ProgressTracker != null}, OverwriteIfExist: {DoOverwrite == IfExists.Overwrite}");
            }
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
        /// <param name="ingressTest">True if we just want to test ingress</param>
        /// <param name="chunkSize">Chunk Size used for chunking</param>
        /// <returns>Transfer Status of the upload</returns>
        internal static TransferStatus Upload(string srcPath, string destPath, AdlsClient client, int numThreads = -1, IfExists shouldOverwrite = IfExists.Overwrite, IProgress<TransferStatus> progressTracker = null,bool notRecurse=false, bool ingressTest = false, long chunkSize = ChunkSizeDefault)
        {
            return new FileUploader(srcPath, destPath, client, numThreads, shouldOverwrite, progressTracker,notRecurse, ingressTest, chunkSize).RunTransfer();
        }
        /// Verifies whether input is a directory or a file. If it is a file then there is no need to start the producer
        protected override bool StartEnumeration()
        {
            if (File.Exists(SourcePath))
            {
                var file = new FileInfo(SourcePath);
                long chunks = AddFileToConsumerQueue(file.FullName, file.Length, file.Length > ChunkSize);
                StatusUpdate(1, chunks == 0 ? 1 : 0, chunks, file.Length, 0);
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
        /// <param name="chunkSegmentFolder">Temporary folder where chunks are located</param>
        /// <param name="dest">Destination file</param>
        /// <param name="totSize">Total size of the file- needed for verification of the copy</param>
        /// <param name="totalChunks">Total number of chunks</param>
        internal override void AddConcatJobToQueue(string chunkSegmentFolder, string dest, long totSize,
            long totalChunks)
        {
            ConsumerQueue.Add(new ConcatenateJob(chunkSegmentFolder, dest, Client, totSize, totalChunks, true));
        }
        /// <summary>
        /// Producer code which traverses local directory tree and add them as chunked or non-chunked jobs to job queue depending on it's size. Currently this directly adds 
        /// jobs to job queue but in future we will try to add files to an internal list and add them as jobs in FinalPassProducerRun.
        /// </summary>
        protected override void FirstPassProducerRun()
        {
            do
            {
                var dir = UploaderProducerQueue.Poll();

                if (dir == null) //Means end of Producer
                {
                    UploaderProducerQueue.Add(null); //Notify if any other threads are waiting
                    return;
                }
                try
                {
                    long isEmpty = 0;
                    IEnumerable<DirectoryInfo> enumDir = dir.EnumerateDirectories();
                    foreach (var subDir in enumDir)
                    {
                        if (NotRecurse) //Directly add the directories to be created since we won't go in recursively
                        {
                            AddDirectoryToConsumerQueue(subDir.FullName, true);
                        }
                        else
                        {
                            UploaderProducerQueue.Add(subDir);
                        }
                        isEmpty++;
                    }

                    IEnumerable<FileInfo> enumFiles = dir.EnumerateFiles();
                    long numDirs = isEmpty, totChunks = 0, unchunkedFiles = 0, totSize = 0;
                    foreach (var file in enumFiles)
                    {
                        long chunks = AddFileToConsumerQueue(file.FullName, file.Length, file.Length > ChunkSize);
                        totChunks += chunks;
                        if (chunks == 0)
                        {
                            unchunkedFiles++;
                        }
                        isEmpty++;
                        totSize += file.Length;
                    }
                    if (isEmpty == 0)
                    {
                        AddDirectoryToConsumerQueue(dir.FullName, true);
                    }
                    // If there are any directories and it is not recurse update the number of directories
                    StatusUpdate(isEmpty - numDirs, unchunkedFiles, totChunks, totSize, isEmpty>0?(NotRecurse
                        ?numDirs:0):1);
                }
                catch (Exception ex)
                {
                    Status.EntriesFailed.Add(new SingleEntryTransferStatus(dir.FullName, ex.StackTrace,
                        EntryType.Directory, SingleChunkStatus.Failed));
                }
            } while (!NotRecurse);
        }
        /// Creates the MetaData for uploader
        internal override FileMetaData AssignMetaData(string fileFullName, string chunkSegmentFolder, string destPath,
            long fileLength, long numChunks)
        {
            return new FileMetaData(fileFullName, chunkSegmentFolder, destPath, fileLength, this, numChunks, true);
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
