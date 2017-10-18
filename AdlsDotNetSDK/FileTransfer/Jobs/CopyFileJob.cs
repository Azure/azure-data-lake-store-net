using System;
using System.IO;
using System.Net;

namespace Microsoft.Azure.DataLake.Store.FileTransfer.Jobs
{
    /// <summary>
    /// Does the main copy of the file for both uploader and downloader
    /// </summary>
    internal class CopyFileJob : Job
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
        /// Buffersize of read from source and write to destination
        /// </summary>
        private const int BuffSize = 4 * 1024 * 1024;
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

        protected override SingleEntryTransferStatus DoJob()
        {
            long chunkLength = _lengthToRead;
            try
            {
                //Determines whether a chunk of a file need to be skipped. This is more effecient for chunked case because other threads
                // do not have to check fileExists again and again. For nonchunked we determine it while creating otherwise there will be two 
                // filesystem calls
                if (ChunkIndex >= 0 && Metadata.ShouldSkipForChunkedFile(Client))
                {
                    return new SingleEntryTransferStatus(Metadata.Dest, "", EntryType.File, SingleChunkStatus.Skipped);
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
                        return new SingleEntryTransferStatus(Metadata.Dest, "", EntryType.File, SingleChunkStatus.Skipped);
                    }
                    TransferChunks(Metadata.IsUpload ? localStream : remoteStream, Metadata.IsUpload ? remoteStream : localStream);
                }
                //No need to update the chunk if it is a job for a non-chunked file
                if (ChunkIndex >= 0)
                {
                    Metadata.UpdateChunk();
                }
                return new SingleEntryTransferStatus(Destination, "", ChunkIndex >= 0 ? EntryType.Chunk : EntryType.File, SingleChunkStatus.Successful, chunkLength);
            }

            catch (Exception e)
            {
                return new SingleEntryTransferStatus(Destination, e.Message, ChunkIndex >= 0 ? EntryType.Chunk : EntryType.File, SingleChunkStatus.Failed, chunkLength);
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

            if (!Metadata.IsUpload)
            {
                writeStream.Seek(_offset, SeekOrigin.Begin);
            }
            byte[] readBytes = new byte[BuffSize];
            long lengthToRead = _lengthToRead;
            while (lengthToRead > 0)
            {
                // Read may return anything from 0 to numBytesToRead.
                int bytesRead = readStream.Read(readBytes, 0, (int)Math.Min(BuffSize, lengthToRead));
                // Break when the end of the file is reached.
                if (bytesRead > 0)
                {
                    writeStream.Write(readBytes, 0, bytesRead);
                }
                else
                {
                    break;
                }
                lengthToRead -= bytesRead;
            }
        }

        protected override string JobType()
        {
            return "CopyFile";
        }

        protected override string JobDetails()
        {
            return $"Source: {Metadata.SrcFile}, Dest: {Destination}, Offset: {_offset}, Length: {_lengthToRead}";
        }
    }
}
