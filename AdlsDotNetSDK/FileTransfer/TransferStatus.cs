using System;
using System.Collections.Generic;

namespace Microsoft.Azure.DataLake.Store.FileTransfer
{
    /// <summary>
    /// Contains information regarding Transfer status
    /// </summary>
    public class TransferStatus
    {
        /// <summary>
        /// Total number of chunks that needs to be transferred after enumeration
        /// </summary>
        public long TotalChunksToTransfer;
        /// <summary>
        /// Total number of unchunked files that needs to be transferred after enumeration
        /// </summary>
        public long TotalNonChunkedFileToTransfer;
        /// <summary>
        /// Total number of files (includes chunked and unchunked) that need to be transferred after enumeration
        /// </summary>
        public long TotalFilesToTransfer;
        /// <summary>
        /// Total size of files or chunks that need to be transferred after enumeration
        /// </summary>
        public long TotalSizeToTransfer;
        /// <summary>
        /// Total number of empty directories that needs to be transferred after enumeration
        /// </summary>
        public long TotalDirectoriesToTransfer;
        /// <summary>
        /// Tracks number of files (includes chunked and unchunked) transferred correctly
        /// </summary>
        public long FilesTransfered;
        /// <summary>
        /// Tracks number of chunks transferred correctly
        /// </summary>
        public long ChunksTransfered;
        /// <summary>
        /// Tracks number of unchunked files transferred
        /// </summary>
        public long NonChunkedFileTransferred;
        /// <summary>
        /// Tracks number of chunks transferred correctly
        /// </summary>
        public long SizeTransfered;
        /// <summary>
        /// Tracks number of empty directories transferred correctly
        /// </summary>
        public long DirectoriesTransferred;
        private readonly Object _lock = new Object(); 
        /// <summary>
        /// List of directories or chunks or unchunked files that did not get transferred correctly
        /// </summary>
        public List<SingleEntryTransferStatus> EntriesFailed = new List<SingleEntryTransferStatus>();
        /// <summary>
        /// List of name of files that are skipped because we did not want to overwrite existing files
        /// </summary>
        public HashSet<string> EntriesSkipped = new HashSet<string>();
        internal void AddFailedEntries(SingleEntryTransferStatus entry)
        {
            lock (_lock)
            {
                EntriesFailed.Add(entry);
            }
        }
        internal void AddSkippedEntries(string entry)
        {
            lock (_lock)
            {
                EntriesSkipped.Add(entry);
            }
        }
    }
    /// <summary>
    /// Contains the transfer result of each file or chunk or directory transfer
    /// </summary>
    public class SingleEntryTransferStatus
    {
        /// <summary>
        /// Name of the hunk or file or directory
        /// </summary>
        public string EntryName { get; }
        /// <summary>
        /// Size of the chunk or file
        /// </summary>
        public long EntrySize { get; }
        /// <summary>
        /// Any errors if the transfer was not successful
        /// </summary>
        public string Errors { get; }
        /// <summary>
        /// Type of entry: File, chunk or directory
        /// </summary>
        public EntryType Type { get; }
        /// <summary>
        /// Status of the transfer: Successful or failed or skipped if we didnt want to overwrite the destination if it exists
        /// </summary>
        public SingleChunkStatus Status { get; }
        internal SingleEntryTransferStatus(string entry,string err,EntryType type,SingleChunkStatus status,long entrySize=0)
        {
            EntryName = entry;
            Errors = err;
            Type = type;
            Status = status;
            EntrySize = entrySize;
        }
    }
    /// <summary>
    /// Enum for different types of entries transferred
    /// </summary>
    public enum EntryType
    {
        Directory,
        File,
        Chunk
    }
    /// <summary>
    /// Status of a transfer of single entry
    /// </summary>
    public enum SingleChunkStatus
    {
        /// <summary>
        /// Transfer is successful
        /// </summary>
        Successful,
        /// <summary>
        /// Transfer failed
        /// </summary>
        Failed,
        /// <summary>
        /// If the destination file exists and the user has selected to fail if the destination exists then it is skipped
        /// </summary>
        Skipped
    }

}
