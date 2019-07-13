using System;
using System.IO;
using System.Net;
using System.Text;
using System.Threading;

namespace Microsoft.Azure.DataLake.Store.FileTransfer.Jobs
{
    /// <summary>
    /// Does the main copy of the file for both uploader and downloader
    /// </summary>
    internal class CopyFileJob : BaseJob
    {
        /// <summary>
        /// Index of the chunk of the main file this job corresponds to. If -1 then this job corresponds to a non-chunked file
        /// </summary>
        private int ChunkIndex { get; }
        /// <summary>
        /// Metadata of the file.
        /// </summary>
        private FileMetaData Metadata { get; }
        /// <summary>
        /// ADls client
        /// </summary>
        private AdlsClient Client { get; }
        /// <summary>
        /// Buffersize of read from source and write to destination. This has to same as <see cref="AdlsOutputStream.BufferMaxCapacity"/>. Otherwise we haveto change immplementation of ReadForwardTillNewLine.
        /// </summary>
        private  static readonly int BuffSize = AdlsOutputStream.BufferMaxCapacity;
        // 
        internal static int ReadForwardBuffSize = 8 * 1024;
        /// <summary>
        /// Actual destination of the file
        /// </summary>
        private string Destination { get; }
        // Length to transfer by this copy job
        private readonly long _lengthToRead;
        // Offset of copy: For non-chunked it is always zero.
        private readonly long _offset;
        internal CopyFileJob(int chunkIndex, FileMetaData metadata, AdlsClient client) : base(chunkIndex != -1 ? (1 << 32) + ((chunkIndex + 1) * metadata.Transfer.ChunkSize < metadata.TotSize
                    ? metadata.Transfer.ChunkSize
                    : metadata.TotSize - chunkIndex * metadata.Transfer.ChunkSize) : metadata.TotSize)
        {
            ChunkIndex = chunkIndex;
            Metadata = metadata;
            Client = client;
            //For upload it is the guid file for that chunk if the file size is greater than 256 mb,
            //for download it is guid file for the whole Destination file if the file is greater than 256mb
            Destination = Metadata.GetChunkFileName(ChunkIndex);
            //If one file then length is whole file length, if chunked and it is not the last chunk then it is 240 MB else it is the last chunk size which might be less than 240 MB
            _lengthToRead = ChunkIndex == -1 ? Metadata.TotSize : (Metadata.Transfer.ChunkSize * (ChunkIndex + 1) < Metadata.TotSize
                    ? Metadata.Transfer.ChunkSize : Metadata.TotSize - (ChunkIndex * Metadata.Transfer.ChunkSize));

            //For a non chunked file offset will always be zero, else it will be multiple of 256MB
            _offset = ChunkIndex <= 0 ? 0 : Metadata.Transfer.ChunkSize * ChunkIndex;
        }

        protected override object DoJob()
        {
            long chunkLength = _lengthToRead;
            try
            {
                // If a chunked file was reported to be attempted last transfer and it is resumed, then we have to establish whether we need to transfer any remaining chunks. 
                // Reason is just based on log file we cannot gurantee the state (Please refer FileTransferCommon.AddFileToConsumerQueue for more doc). 
                // We need to verify whether the chunk temp file exists or concat temp file exist (for upload) or the destination exist
                // to establish where we are in resume. If we are anywhere after "chunk temp file/folder does not exist" state then there is no need of transferring chunks
                if (Metadata.IsFileHalfDone)
                {
                    bool notContinue = Metadata.IsUpload ? !Metadata.ResumeUpload(Client) : !Metadata.ResumeDownload();
                    if (notContinue)
                    {
                        // Some irrecoverable problem during resume
                        if (!string.IsNullOrEmpty(Metadata.UnexpectedTransferErrorResume))
                        {
                            return new SingleEntryTransferStatus(Metadata.SrcFile, Metadata.Dest,
                                Metadata.UnexpectedTransferErrorResume, EntryType.Chunk, SingleChunkStatus.Failed);
                        }
                        // Here all the chunks are actually transferred even though it was not reported in logfile
                        // Here we return success for those cases where we have outstanding jobs for chunks so that they get reported as success in log file
                        if (Metadata.StartChunksAlreadyTransfered < Metadata.TotalChunks)
                        {
                            return new SingleEntryTransferStatus(Metadata.SrcFile, Metadata.Dest, "", EntryType.Chunk, SingleChunkStatus.Successful, ChunkIndex, chunkLength);
                        }
                        // This is the case where actually all chunks were already reported to be transferred
                        return new SingleEntryTransferStatus(Metadata.SrcFile, Metadata.Dest,
                            "", EntryType.Chunk, SingleChunkStatus.Skipped);
                        
                    }
                }
                // Determines whether a chunk of a file need to be skipped. This is more effecient for chunked case because other threads
                // do not have to check fileExists again and again. For nonchunked we determine it while creating otherwise there will be two 
                // filesystem calls
                if (ChunkIndex >= 0 && Metadata.ShouldSkipForChunkedFile(Client))
                {
                    return new SingleEntryTransferStatus(Metadata.SrcFile, Metadata.Dest, "", EntryType.File, SingleChunkStatus.Skipped);
                }
                bool uploadDestExists, downloadDestExists;
                // uploadDestExists checks whether the destination exists in remote adl while creating the file
                // downloadDestExists checks whether the destination exists in local disk while creating the file
                //Either uploadDestExists will be true or downloadDestExists can be true. Both can never be true
                // For non chunked job: Determine whether this entry will be skipped while creating the stream, this is why we pass skip flag as output
                using (Stream remoteStream = GetRemoteStream(out uploadDestExists),
                    localStream = GetLocalStream(out downloadDestExists))

                {
                    if (uploadDestExists || downloadDestExists)
                    {
                        return new SingleEntryTransferStatus(Metadata.SrcFile, Metadata.Dest, "", EntryType.File, SingleChunkStatus.Skipped);
                    }
                    TransferChunks(Metadata.IsUpload ? localStream : remoteStream, Metadata.IsUpload ? remoteStream : localStream);
                }
                //No need to update the chunk if it is a job for a non-chunked file
                if (ChunkIndex >= 0)
                {
                    Metadata.UpdateChunk();
                }
                return new SingleEntryTransferStatus(Metadata.SrcFile, Destination, "", ChunkIndex >= 0 ? EntryType.Chunk : EntryType.File, SingleChunkStatus.Successful, ChunkIndex, chunkLength);
            }

            catch (Exception e)
            {
                return new SingleEntryTransferStatus(Metadata.SrcFile, Destination, e.Message, ChunkIndex >= 0 ? EntryType.Chunk : EntryType.File, SingleChunkStatus.Failed, ChunkIndex, chunkLength);
            }
        }
        /// For uploader it is the AdlsOutputStream, for downloader it is the AdlsInputStream. For Uploader: for chunked files it creates the chunk file with Overwrite flag
        /// for non chunked file it creates the file with whatever flag user has entered. If the user has entered IfExists.Fail and file exists it will give an ADLSException and we skip the file
        private Stream GetRemoteStream(out bool skip)
        {
            skip = false;
            if (Metadata.IsUpload)
            {
                try
                {
                    return Client.CreateFile(Destination, ChunkIndex >= 0 ? IfExists.Overwrite : Metadata.Transfer.DoOverwrite);
                }
                catch (AdlsException ex)
                {
                    if (ex.HttpStatus == HttpStatusCode.Forbidden &&
                        ex.RemoteExceptionName.Equals("FileAlreadyExistsException"))
                    {
                        skip = true;
                        return null;
                    }
                    throw ex;
                }
            }
            return Metadata.EgressBufferSize == null ? Client.GetReadStream(Metadata.SrcFile) : Client.GetReadStream(Metadata.SrcFile, (int)Metadata.EgressBufferSize.Value);
        }
        /// For uploader it is FileStream open , For downloader it is FileStream write For Downloader: for chunked downloads it creates the temporary file with FileMode.Create
        /// for non chunked download it creates the file with Create if user has specified overwrite else CreateNew. If the user has entered IfExists.Fail and file exists it will give
        ///  an IOException and we skip the file
        private Stream GetLocalStream(out bool skip)
        {
            skip = false;
            if (Metadata.IsUpload)
            {
                if (Metadata.IngressOrEgressTest)
                {
                    return new RandomDataStream(_lengthToRead);
                }
                return new FileStream(Metadata.SrcFile, FileMode.Open, FileAccess.Read, FileShare.Read);
            }
            if (Metadata.IngressOrEgressTest)
            {
                return Stream.Null;
            }
            try
            {
                return Metadata.CreateOrOpenDownloadFile();
            }
            catch (IOException ex) //For createNew if the file exists it will raise IOException
            {
                if (ChunkIndex == -1)
                {
                    skip = true;
                    return null;
                }
                throw ex;
            }
        }
        
        /// Seeks the read stream to the offset. If download then seeks the write stream to the offset. Copies data from read stream to write stream
        private void TransferChunks(Stream readStream, Stream writeStream)
        {
            readStream.Seek(_offset, SeekOrigin.Begin);
            var readBytes = new byte[BuffSize];
            // Index till which there is data already in the buffer
            var residualDataSize = 0;

            // Only for upload, non-binary and chunked uploads
            // Each thread respnsible for a chunk will look for the first newline in it's chunk and it will start uploading after that
            // And after it's chunk is uploaded then read forward into the next chunk till it gets a new line.
            if (Metadata.IsUpload && !Metadata.IsBinary && ChunkIndex > 0)
            {

                int indexAfterNewLine = ReadForwardTillNewLine(readStream, readBytes, 0);
                int finalReadData = (int)(readStream.Position - _offset); // Bytes of data read for getting a new line
                // If no new line found and the remaining data in stream was less than 4 MB then indexAfterNewLine is 0
                residualDataSize = finalReadData - indexAfterNewLine;
                if (residualDataSize > 0)
                {
                    Buffer.BlockCopy(readBytes, indexAfterNewLine , readBytes, 0, residualDataSize);
                }
            }

            if (!Metadata.IsUpload)
            {
                writeStream.Seek(_offset, SeekOrigin.Begin);
            }
            // if we have done readForwardTilNewLine then less data needs to be read now so update the lengthToRead
            long lengthToRead = _lengthToRead - (readStream.Position - _offset);
            while (lengthToRead > 0)
            {
                // Since we have data till residualDataSize in buffer we have to read remaining data in buffer
                int bufferDataSize = residualDataSize + ReadDataIntoBuffer(readStream, readBytes, residualDataSize, (int)Math.Min(BuffSize - residualDataSize, lengthToRead));
                if (bufferDataSize == residualDataSize)
                { // This will never be the case unless the file is being edited because if there was no data it would have been caught by lengthToRead>0
                    break;
                }

                int indexAfterlastNewLine = bufferDataSize;
                if (Metadata.IsUpload && !Metadata.IsBinary)
                {
                    indexAfterlastNewLine = GetNewLine(readBytes, 0, bufferDataSize, Metadata.EncodeType, true) + 1;
                }
                // For non binary uploads: indexAfterNewLine will be either the index after new line or 0 if no new line is found and data in the buffer is less than 4 MB
                // Rest scenarios: indexAfterNewLine will be buffer data length 
                if (indexAfterlastNewLine != 0)
                {
                    writeStream.Write(readBytes, 0, indexAfterlastNewLine);
                }
                else if (bufferDataSize == AdlsOutputStream.BufferMaxCapacity) // No newlines were found in 4MB
                {
                    throw new AdlsException($"No new lines obtained in {AdlsOutputStream.BufferMaxCapacity} of data at offset {readStream.Position - AdlsOutputStream.BufferMaxCapacity} for file {Metadata.SrcFile}. File should be uploaded as binary.");
                }
                // Length read this turn would be total buffer size minus the residual Data size, update the lengthToRead for next turn
                lengthToRead -= bufferDataSize - residualDataSize;
                // Compute new residualDataSize: data starting from indexAfterNewLine till total buffer size
                residualDataSize = bufferDataSize - indexAfterlastNewLine;
                // Move residual data to the start of the array.
                if (residualDataSize > 0)
                {
                    Buffer.BlockCopy(readBytes, indexAfterlastNewLine, readBytes, 0, residualDataSize);
                }
            }
            // Only for upload, non-binary and chunked uploads
            // Read forward into next chunk till it gets a new line
            if (Metadata.IsUpload && !Metadata.IsBinary && ChunkIndex >= 0 && ChunkIndex < Metadata.TotalChunks)
            {
                int indexAfterNewLine = ReadForwardTillNewLine(readStream, readBytes, residualDataSize);
                if (indexAfterNewLine != 0)
                {
                    residualDataSize = indexAfterNewLine;
                }
            }
            // For non binary uploads there can be residual data
            if (residualDataSize > 0)
            {
                writeStream.Write(readBytes, 0, residualDataSize);
            }
        }
        // Transfers data from stream to buffer and returns the number of bytes read
        private int ReadDataIntoBuffer(Stream readStream, byte[] buffer, int offset, int lengthToRead)
        {
            int bytesRead, prevOffset = offset;
            do
            {
                bytesRead = readStream.Read(buffer, offset, lengthToRead);
                lengthToRead -= bytesRead;
                offset += bytesRead;
            } while (lengthToRead > 0 && bytesRead > 0);
            return offset - prevOffset;
        }
        // Reads forward till it gets a new line. If it does not get a new line in 4MB then throw an exception.
        // Stores the read data while searching the newline in buffer. buffer is of size BuffSize. bufferOffset is 
        // the offset till which data is already present in the buffer. 
        // Returns the offset or position of the byte after new line in the buffer
        private int ReadForwardTillNewLine(Stream readStream, byte[] buffer, int bufferOffset)
        {
            int result = -1;
            while (bufferOffset < BuffSize)
            {
                // bytesRead can return less bytes than requested. Basically the number of bytes of data we send to GetNewLine needs to
                // be divisible by at least 4 because of encoding. So we read at least 8 KB data and then send it to GetNewLine.
                // Most of the time 8KB will be retrieved at one go

                int totBytesRead = ReadDataIntoBuffer(readStream, buffer, bufferOffset, ReadForwardBuffSize);
                bufferOffset += totBytesRead;
                if (totBytesRead > 0)
                {
                    // result is a index withing buffer array, 
                    result = GetNewLine(buffer, bufferOffset - totBytesRead, totBytesRead, Metadata.EncodeType, false);
                    if (result != -1)
                    {
                        // Returns the index of the byte after \n
                        return result + 1;
                    }
                }
                else
                {
                    break;
                }
            }
            if (bufferOffset == AdlsOutputStream.BufferMaxCapacity && result == -1)
            {
                throw new AdlsException($"No new lines obtained in {AdlsOutputStream.BufferMaxCapacity} of data at offset {readStream.Position - AdlsOutputStream.BufferMaxCapacity} for file {Metadata.SrcFile}. File should be uploaded as binary.");
            }
            // This is the case where the current or the next chunk segment will have data less than 4 MB
            return 0;
        }
        // Retruns the the index of the last byte of the character \r or \n or \r\n in buffer. for a particular encoding if \n is from index 
        // 24 to 27 then it returns the index 27
        internal static int GetNewLine(byte[] buffer, int offset, int length, Encoding encoding, bool reverse)
        {
            int bytesPerChar;
            switch (encoding.CodePage)
            {
                // Big Endian Unicode (UTF-16)
                case 1201:
                // Unicode (UTF-16)
                case 1200:
                    bytesPerChar = 2;
                    break;
                // UTF-32
                case 12000:
                    bytesPerChar = 4;
                    break;
                // ASCII case 20127:
                // UTF-8 case 65001:
                // UTF-7 case 65000:
                // Default to UTF-8
                default:
                    bytesPerChar = 1;
                    break;
            }
            int result = -1;
            int lastIndex = offset + length - 1;
            char prevChar = 'a';
            for (int charPos = reverse ? lastIndex : offset; reverse ? charPos >= offset : charPos <= lastIndex; charPos += (reverse ? -1 : 1) * bytesPerChar)
            {
                prevChar = bytesPerChar == 1 ? (char)buffer[charPos]
                    : encoding.GetString(buffer, reverse ? charPos - bytesPerChar + 1 : charPos, bytesPerChar).ToCharArray()[0];
                if (prevChar == '\r' || prevChar == '\n')
                {
                    result = reverse ? charPos : charPos + bytesPerChar - 1;
                    break;
                }
            }
            if (result == -1)
            {
                return -1;
            }
            if (!reverse && result < lastIndex && prevChar == '\r' && '\n' == (bytesPerChar == 1
                    ? (char)buffer[result + 1]
                    : encoding.GetString(buffer, result + 1, bytesPerChar).ToCharArray()[0]))
            {
                result += bytesPerChar;
            }
            return result;
        }
        protected override string JobType()
        {
            return "FileTransfer.CopyFile";
        }

        protected override string JobDetails()
        {
            return $"Source: {Metadata.SrcFile}, Dest: {Destination}, Offset: {_offset}, Length: {_lengthToRead}";
        }
    }
}
