using System.Collections.Generic;
using System;
using System.IO;
using System.Net;
using Microsoft.Azure.DataLake.Store.RetryPolicies;

namespace Microsoft.Azure.DataLake.Store.FileTransfer.Jobs
{
    /// <summary>
    /// For uploader concatenates the chunks and for downloader renames the temporary filename.
    /// </summary>
    internal class ConcatenateJob : BaseJob
    {
        private string Source { get; }

        /// <summary>
        /// For uploader:
        ///      1)when files are chunked this is the directory name where chunks are saved. 
        /// For downloader:
        ///     1) When files are chunked, temporary GUID name
        /// When not chunked, Concat job is not created
        /// </summary>
        private string ChunkSegmentFolder { get; }
        /// <summary>
        /// Destination path
        /// </summary>
        private string Destination { get; }
        /// <summary>
        /// Adls client
        /// </summary>
        private AdlsClient Client { get; }
        /// <summary>
        /// Total file size- use for uploader or downloader verification
        /// </summary>
        private long FileSize { get; }
        /// <summary>
        /// Whether it is upload or download
        /// </summary>
        private bool IsUpload { get; }
        /// <summary>
        /// Total number of chunks
        /// </summary>
        private readonly long _totalChunks;

        private readonly bool _performUploadRenameOnly;

        private const int UploadRetryTime = 1;
        private const string DestTempGuidForConcat = "ConcatGuid";

        internal ConcatenateJob(string source, string chunkSegmentFolder, string dest, AdlsClient client, long size, long totalChunks, bool isUpload, bool doUploadRenameOnly = false) : base(size)
        {
            Source = source;
            ChunkSegmentFolder = chunkSegmentFolder;
            Destination = dest;
            Client = client;
            FileSize = size;
            _totalChunks = totalChunks;
            IsUpload = isUpload;
            _performUploadRenameOnly = doUploadRenameOnly;
        }

        protected override object DoJob()
        {
            try
            {
                //Note: We will never reach here if the destination exists and we have elected to not overwrite the destination if it exists
                if (IsUpload)
                {
                    return PerformUploadJob();
                }
                // DOWNLOAD CASE
                //Deletes the destination and renames the file from a temporaray guid name to the destination
                File.Delete(Destination);
                File.Move(ChunkSegmentFolder, Destination);
                if (VerifyLocalExist())
                {
                    return new SingleEntryTransferStatus(Source, Destination, "", EntryType.File, SingleChunkStatus.Successful);
                }
                return new SingleEntryTransferStatus(Source, Destination, "Size did not match", EntryType.File, SingleChunkStatus.Failed);
            }
            catch (Exception e)
            {
                return new SingleEntryTransferStatus(Source, Destination, e.Message, EntryType.File, SingleChunkStatus.Failed);
            }
        }
        /// <summary>
        /// Verify whether the input file's length and file length after upload or download are same
        /// </summary>
        /// <returns>True if it matches else false</returns>
        private bool VerifyLocalExist()
        {
            long length = new FileInfo(Destination).Length;
            if (length == FileSize)
            {
                return true;
            }
            return false;
        }

        private bool VerifyAdlExists(string destination)
        {
            try
            {
                long length = Client.GetDirectoryEntry(destination).Length;
                if (length == FileSize)
                {
                    return true;
                }
                return false;
            }
            catch (AdlsException excep)
            {
                if (excep.HttpStatus == HttpStatusCode.NotFound)
                {
                    return false;
                }
                throw excep;
            }
        }
        //Upload: Concats all the chunks into a temporary guid name. Then renames it to the destination to overwrite it
        private SingleEntryTransferStatus PerformUploadJob()
        {
            AdlsException adlsExcep;
            // If only do rename then go do that- This will only happen in resume cases
            if (_performUploadRenameOnly || PerformConcatWithRetries(out adlsExcep))
            {
                string destGuid = ChunkSegmentFolder + FileUploader.DestTempGuidForConcat;
                try
                {
                    // This call is with retries
                    Client.Rename(destGuid, Destination, true);
                    if (VerifyAdlExists(Destination))
                    {
                        return new SingleEntryTransferStatus(Source, Destination, "", EntryType.File,
                            SingleChunkStatus.Successful);
                    }
                    return new SingleEntryTransferStatus(Source, Destination, "Size did not match", EntryType.File,
                        SingleChunkStatus.Failed);
                }
                catch (AdlsException excep)
                {
                    adlsExcep = excep;
                }
            }
            return new SingleEntryTransferStatus(Source, Destination, adlsExcep.Message, EntryType.File, SingleChunkStatus.Failed);
        }
        // Perform concat with retries. Currently retries only once. If the concat fails, checks whether it can be retried based on HttpStatuscode,
        // If true then check whether the destiantion already exists and the source is deleted. If there is no intermediate state then returns true.
        private bool PerformConcatWithRetries(out AdlsException excep)
        {
            var retryPolicy = new ExponentialRetryPolicy();
            string destGuid = ChunkSegmentFolder + FileUploader.DestTempGuidForConcat;
            var chunkList = new List<string>((int)_totalChunks);
            for (int i = 0; i < _totalChunks; i++)
            {
                chunkList.Add(ChunkSegmentFolder + "/" + i);
            }
            int retries = 0;
            do
            {
                excep = PerformConcatSingle(chunkList, destGuid);
                if (excep == null)
                {
                    return true;
                }
                if (!retryPolicy.ShouldRetryBasedOnHttpOutput((int)excep.HttpStatus, excep.Ex))
                {
                    return false;
                }
                if (VerifyAdlExists(destGuid))
                {
                    if (Client.CheckExists(ChunkSegmentFolder))
                    {
                        // If both destination and source folder exist then end-no way to recover
                        return false;
                    }
                    return true;
                }
            } while (retries++ < UploadRetryTime);
            return false;
        }

        private AdlsException PerformConcatSingle(List<string> fileList, string destGuid)
        {
            AdlsException exception=null;
            try
            {
                Client.ConcatenateFiles(destGuid, fileList);
            }
            catch (AdlsException ex)
            {
                exception = ex;
            }
            return exception;
        }
        protected override string JobType()
        {
            return "FileTransfer.ConcatJob";
        }

        protected override string JobDetails()
        {
            return $"Source: {ChunkSegmentFolder}, Destination: {Destination}, Length: {FileSize}";
        }
    }
}
