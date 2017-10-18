using System.Collections.Generic;
using System;
using System.IO;

namespace Microsoft.Azure.DataLake.Store.FileTransfer.Jobs
{
    /// <summary>
    /// For uploader concatenates the chunks and for downloader renames the temporary filename.
    /// </summary>
    internal class ConcatenateJob : Job
    {
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

        internal ConcatenateJob(string chunkSegmentFolder,string dest,AdlsClient client,long size,long totalChunks,bool isUpload) : base(size)
        {
            ChunkSegmentFolder = chunkSegmentFolder;
            Destination = dest;
            Client = client;
            FileSize = size;
            _totalChunks = totalChunks;
            IsUpload = isUpload;
        }

        protected override SingleEntryTransferStatus DoJob()
        {
            try
            {
                //Note: We will never reach here if the destination exists and we have elected to not overwrite the destination if it exists
                //Upload: Concats all the chunks into a temporary guid name. Then renames it to the destination to overwrite it
                if (IsUpload)
                {
                    string destGuid = Destination + Guid.NewGuid();
                    List<string> chunkList = new List<string>((int)_totalChunks);
                    for (int i = 0; i < _totalChunks; i++)
                    {
                        chunkList.Add(ChunkSegmentFolder + "/" + i);
                    }
                    Client.ConcatenateFiles(destGuid, chunkList);
                    Client.Rename(destGuid, Destination, true);
                }
                else
                {
                    //Deletes the destination and renames the file from a temporaray guid name to the destination
                    File.Delete(Destination);
                    File.Move(ChunkSegmentFolder,Destination);
                }
                if (VerifyConcat())
                {
                    return new SingleEntryTransferStatus(Destination, "", EntryType.File, SingleChunkStatus.Successful);
                }
                return new SingleEntryTransferStatus(Destination, "Size did not match", EntryType.File, SingleChunkStatus.Failed);
            }
            catch (Exception e)
            {
                return new SingleEntryTransferStatus(Destination, e.Message, EntryType.File, SingleChunkStatus.Failed);
            }
        }
        /// <summary>
        /// Verify whether the input file's length and file length after upload or download are same
        /// </summary>
        /// <returns>True if it matches else false</returns>
        private bool VerifyConcat()
        {
            long length=IsUpload?Client.GetDirectoryEntry(Destination).Length:new FileInfo(Destination).Length;
            if (length == FileSize)
            {
                return true;
            }
            return false;
        }
        
        protected override string JobType()
        {
            return "ConcatJob";
        }

        protected override string JobDetails()
        {
            return $"Source: {ChunkSegmentFolder}, Destination: {Destination}, Length: {FileSize}";
        }
    }
}
