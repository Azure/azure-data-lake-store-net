using System;
using System.IO;
using System.Text;
#if NET452
using Microsoft.Win32.SafeHandles;
using System.Runtime.InteropServices;
using System.Threading;
using System.ComponentModel;
#endif

namespace Microsoft.Azure.DataLake.Store.FileTransfer
{
    /// <summary>
    /// It contains the metadata of the file: Source path, Destination path, ChunkSegmentFolder, Total file length
    /// </summary>
    internal class FileMetaData
    {
        /// <summary>
        /// For uploader:
        ///      1)when files are chunked this is the directory name where chunks are saved. 
        ///      2)when files are not chunked this is null
        /// For downloader:
        ///     1) When files are chunked temporary GUID name
        ///     2) when not chunked same as uploader
        /// </summary>
        internal string ChunkSegmentFolder { get; }
        /// <summary>
        /// Source full path
        /// </summary>
        internal string SrcFile { get; }
        /// <summary>
        /// Destination Path
        /// </summary>
        internal string Dest { get; }
        /// <summary>
        /// Total size of the file
        /// </summary>
        internal long TotSize { get; }
        /// <summary>
        /// Total number of chunks of the file
        /// </summary>
        internal readonly long TotalChunks;
        /// <summary>
        /// Number of chunks transferred. When all the chunks are transferred then concat job is queued
        /// </summary>
        private long _numChunksTransfered;
        // Number of chunks that are transfered already
        internal readonly long StartChunksAlreadyTransfered;
        /// <summary>
        /// Flag that stores wether file exists. It is null when no information is known. 
        /// Once it is not null, we do not need to make FileSystem calls to know wehther the file exists
        /// </summary>
        private bool? _fileExists;
        /// <summary>
        /// Download specific flag. For download if it is true, then the file is already created on local file system. If it is false then file needs to be created.
        /// This is necessary because when we do chunked downloads more than one thread writes at different offsets to same file locally 
        /// </summary>
        private bool _downloadTempFileExists;
        private readonly Object _lock = new Object();

        internal FileTransferCommon Transfer { get; }
        /// <summary>
        /// Whether this is upload or download
        /// </summary>
        internal bool IsUpload { get; }
        /// <summary>
        /// If true for download we do not want to write file to local filesystem, for upload we want to read from a random stream
        /// </summary>
        internal bool IngressOrEgressTest { get; }
        /// <summary>
        /// Download specific. The buffer size of 
        /// </summary>
        internal long? EgressBufferSize { get; }

        internal Encoding EncodeType;
        internal bool IsBinary;
        // This will be true if a chunked file is being resumed after it is incomplete last time
        internal bool IsFileHalfDone;
        private bool? _shouldFileBeResumed;
        // This is irrecoverable error encountered during resume
        internal string UnexpectedTransferErrorResume;
        internal string GetChunkFileName(int index)
        {
            return ChunkSegmentFolder != null ? (IsUpload ? ChunkSegmentFolder + "/" + index : ChunkSegmentFolder) : Dest;
        }

        internal FileMetaData(string src, string chunkSegmentFolder, string dest, long totSize, FileTransferCommon transfer, long totChunks, long startChunkIndx, bool ingressOrEgressTest = false, long? egressBuffer = null) : this(src, chunkSegmentFolder, dest, totSize, transfer, totChunks, false, startChunkIndx, ingressOrEgressTest)
        {
            EgressBufferSize = egressBuffer;
        }
        internal FileMetaData(string src, string chunkSegmentFolder, string dest, long totSize, FileTransferCommon transfer, long totChunks, bool isBinary, Encoding encodeType, long startChunkIndx, bool ingressOrEgressTest = false) : this(src, chunkSegmentFolder, dest, totSize, transfer, totChunks, true, startChunkIndx, ingressOrEgressTest)
        {
            IsBinary = isBinary;
            EncodeType = encodeType;
        }
        private FileMetaData(string src, string chunkSegmentFolder, string dest, long totSize, FileTransferCommon transfer, long totChunks, bool isUpload, long startChunkIndx, bool ingressOrEgressTest)
        {
            ChunkSegmentFolder = chunkSegmentFolder;
            Dest = dest;
            Transfer = transfer;
            TotSize = totSize;
            SrcFile = src;
            TotalChunks = totChunks;
            IsUpload = isUpload;
            IngressOrEgressTest = ingressOrEgressTest;
            // startChunkIndx = -1 means this file was not attempted before so effectively 0 chunks were done, if it is >=0 then it was attempted before
            if (startChunkIndx >= 0)
            {
                IsFileHalfDone = true;
            }
            _numChunksTransfered = StartChunksAlreadyTransfered = startChunkIndx < 0 ? 0 : startChunkIndx;
        }
        /// If overwrite then no need to skip. If not overwrite and the file exists then skip. This method is only necessary for chunked file transfers because:
        /// For uploader we do not want to create the temorary 240MB chunks if the file exists and user wants to IfExists.Fail For downloader we do not want different threads 
        /// to write to the temp file if the destination file exists and user wants IfExists.Fail.
        internal bool ShouldSkipForChunkedFile(AdlsClient client)
        {
            if (Transfer.DoOverwrite == IfExists.Overwrite)
            {
                return false;
            }
            lock (_lock)
            {
                //This is to prevent unecessary server requests of directory access for the same file
                if (_fileExists != null)
                {
                    return _fileExists.Value;
                }
                _fileExists = IsUpload ? client.CheckExists(Dest) : File.Exists(Dest);
                return _fileExists.Value;
            }
        }
#if NET452
        // https://msdn.microsoft.com/en-us/library/windows/desktop/aa364596(v=vs.85).aspx
        // Pinvoke to set the file as parse
        [DllImport("Kernel32.dll", SetLastError = true, CharSet = CharSet.Auto)]
        private static extern bool DeviceIoControl(
            SafeFileHandle hDevice, int dwIoControlCode, IntPtr inBuffer, int nInBufferSize, IntPtr outBuffer, int nOutBufferSize, ref int pBytesReturned, [In] ref NativeOverlapped lpOverlapped);

        // Marks the file sparse when we create the local file during download. Otherwise if we have threads writing to the same file at different offset we have backfilling of zeros
        // meaning we are writing the file twicedue to which we get half the performance
        internal void MarkFileSparse(SafeFileHandle fileHandle)
        {
            int bytesReturned = 0;
            NativeOverlapped lpOverlapped = new NativeOverlapped();
            bool result = DeviceIoControl(fileHandle, 590020, //FSCTL_SET_SPARSE,
                    IntPtr.Zero, 0, IntPtr.Zero, 0, ref bytesReturned, ref lpOverlapped);
            if (result == false)
            {
                throw new Win32Exception();
            }
        }
#endif
        // For downloader. For chunked downloads: Creates the file if it does not exist. Else returns a write stream. This is necessary since multiple threads will write 
        // to the same file at different offset. First thread to get the lock will create the file. Rest threads will open the file for read. For non chunked downloads:  
        // Creates the directory and the file
        internal Stream CreateOrOpenDownloadFile()
        {
            string dest = GetChunkFileName(0);
            // Non chunked file download
            if (TotalChunks == 0)
            {
                Directory.CreateDirectory(Path.GetDirectoryName(dest));
                return new FileStream(dest, Transfer.DoOverwrite == IfExists.Overwrite ? FileMode.Create : FileMode.CreateNew, FileAccess.Write);
            }
            lock (_lock)
            {
                // If the file was reported (in the log file) to be attempted last download, then the destination must exist, unless it was explicitly deleted by user
                if (IsFileHalfDone || _downloadTempFileExists)
                {
                    return new FileStream(dest, FileMode.Open, FileAccess.Write, FileShare.ReadWrite);
                }
                Directory.CreateDirectory(Path.GetDirectoryName(dest));
                var fs = new FileStream(dest, FileMode.Create, FileAccess.Write, FileShare.ReadWrite);
#if NET452
                MarkFileSparse(fs.SafeFileHandle);
#endif
                _downloadTempFileExists = true;
                return fs;
            }
        }
        // Updates the number of chunks uploaded or downloaded. Once all the chunks are done, then add the concat job
        internal void UpdateChunk()
        {
            if (!IsUpload && IngressOrEgressTest)
            {
                return;
            }
            lock (_lock)
            {
                if (_numChunksTransfered == StartChunksAlreadyTransfered) // First chunk has been transferred so put a record in the metadata log file
                {
                    Transfer.AddBeginRecord(SrcFile, ChunkSegmentFolder);
                }
                _numChunksTransfered++;
                if (_numChunksTransfered >= TotalChunks)
                {
                    //Add concatenatejob to priorityqueue of transfer
                    Transfer.AddConcatJobToQueue(SrcFile, ChunkSegmentFolder, Dest, TotSize, TotalChunks);
                }
            }
        }

        internal bool ResumeUpload(AdlsClient client)
        {
            lock (_lock)
            {
                if (_shouldFileBeResumed != null)
                {
                    return _shouldFileBeResumed.Value;
                }
                bool chunkTempDestExists = client.CheckExists(ChunkSegmentFolder);
                bool chunkConcatTempDestExists =
                    client.CheckExists(ChunkSegmentFolder + FileUploader.DestTempGuidForConcat);
                _shouldFileBeResumed = false;
                // Chunks of a file will be resumed only if the chunksegment folder exists and the temp destination guid does not exist and all chunks were not reported to be done
                if (chunkTempDestExists)
                {
                    if (chunkConcatTempDestExists)
                    {
                        // BOOM throw exception because both cant exist- probably problem with concat
                        UnexpectedTransferErrorResume = $"{SrcFile}: Irrecoverable error, Concat is in an intermediate state. Please trasnfer this file without resume flag.";
                    }
                    else if (StartChunksAlreadyTransfered == TotalChunks)
                    {
                        // Add full concat job and you are done
                        Transfer.AddConcatJobToQueue(SrcFile, ChunkSegmentFolder, Dest, TotSize, TotalChunks);
                    }
                    else
                    {
                        // Continue with the job
                        _shouldFileBeResumed = true;
                    }

                }
                else if (chunkConcatTempDestExists)
                {
                    // We only need to add rename part of the concat job
                    Transfer.AddConcatJobToQueue(SrcFile, ChunkSegmentFolder, Dest, TotSize, TotalChunks, true);
                }
                else if (!client.CheckExists(Dest))
                {
                    // At this stage dest has to exist
                    // Again something really wrong happened- probably the rename failed even after retries with some intermediate state
                    UnexpectedTransferErrorResume = $"{SrcFile}: Irrecoverable error, Rename is in an intermediate state. Please trasnfer this file without resume flag.";
                }
                else
                {
                    // All chunks are transferred and the rename happened successfully, put in  record so that we do not have to do these checks again for the file
                    Transfer.AddCompleteRecord(SrcFile,true);
                }
                return _shouldFileBeResumed.Value;
            }
        }

        internal bool ResumeDownload()
        {
            lock (_lock)
            {
                if (_shouldFileBeResumed != null)
                {
                    return _shouldFileBeResumed.Value;
                }
                bool chunkTempDestExists = File.Exists(ChunkSegmentFolder);
                _shouldFileBeResumed = false;
                if (chunkTempDestExists)
                {
                    if (StartChunksAlreadyTransfered == TotalChunks)
                    {
                        // Add concat job
                        Transfer.AddConcatJobToQueue(SrcFile, ChunkSegmentFolder, Dest, TotSize, TotalChunks);
                    }
                    else
                    {
                        // Continue job
                        _shouldFileBeResumed = true;
                    }
                }
                else if (!File.Exists(Dest))
                {
                    // At this stage dest has to exist
                    // Again something really wrong happened- probably the rename failed even after retries with some intermediate state
                    UnexpectedTransferErrorResume = $"{SrcFile}: Irrecoverable error, Rename is in an intermediate state. Please trasnfer this file without resume flag.";
                }
                else
                {
                    // All chunks are transferred and the rename happened successfully, put in  record so that we do not have to do these checks again
                    Transfer.AddCompleteRecord(SrcFile, true);
                }
                return _shouldFileBeResumed.Value;
            }
        }

    }
}