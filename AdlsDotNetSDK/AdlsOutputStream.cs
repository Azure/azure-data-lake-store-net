using NLog;
using System;
using System.IO;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.DataLake.Store.RetryPolicies;

namespace Microsoft.Azure.DataLake.Store
{
    /// <summary>
    /// ADLS Output stream that writes data to a file on Data lake. It writes data to a buffer and when the buffer gets filled, writes data in bulk to server
    /// Data can be written asynchronously/synchronously. Write is fully synchronous till the transport layer. WriteAsync is fully asynchronous till the transport layer.
    /// AdlsOutputStream is not threadsafe since it uses buffer (maintains state so not stateless). 
    /// </summary>
    public class AdlsOutputStream : Stream
    {
        /// <summary>
        /// Logger to log messages related to output stream
        /// </summary>
        private static readonly Logger OutStreamLog = LogManager.GetLogger("adls.dotnet.OutputStream");
        /// <summary>
        /// Full path of the file
        /// </summary>
        private string Filename { get; }
        /// <summary>
        /// ADLS client
        /// </summary>
        private AdlsClient Client { get; }
        /// <summary>
        /// String containing the lease ID, when a client obtains a lease on a file no other client can make edits to the file 
        /// </summary>
        private string LeaseId { get; }
        /// <summary>
        /// Internal buffer where client writes, when it gets filled up or we do flush/dispose then only we write buffer to server 
        /// </summary>
        private byte[] Buffer { get; set; }
        /// <summary>
        /// Internal buffer pool if passed. We will take a byte array of 4 mb from there and then return it to them after close
        /// </summary>
        private AdlsArrayPool<byte> _bufferPool;
        /// <summary>
        /// Capacity of the internal buffer. Check CopyFileJob.cs before changing this threshold.
        /// </summary>
        internal static int BufferMaxCapacity = 4 * 1024 * 1024;
        internal static int BufferMinCapacity = 1 * 1024 * 1024;
        /// <summary>
        /// Number of bytes written to the server
        /// </summary>
        private int _bufferCapacity;
        /// <summary>
        /// Pointer in the buffer till which data is written
        /// </summary>
        private int BufferSize { get; set; }
        /// <summary>
        /// Pointer in file till which data is written, i.e. length of file
        /// </summary>
        private long FilePointer { get; set; }
        /// <summary>
        /// Whether metadata is synced (data is flushed). Syncing metadata is expensive so we do not want to do it unless 
        /// there has been appends with SYNFLAG.DATA since last append with SYNCFLAG.METADA.
        /// </summary>
        private bool MetaDataSynced { get; set; }
        /// <summary>
        /// Whether the stream is disposed
        /// </summary>
        private bool _isDisposed;
        /// <summary>
        /// Stream cannot read data
        /// </summary>
        public override bool CanRead => false;
        /// <summary>
        /// Stream cannot seek data
        /// </summary>
        public override bool CanSeek => false;
        /// <summary>
        /// Stream can write data
        /// </summary>
        public override bool CanWrite => true;
        /// <summary>
        /// Not supported
        /// </summary>
        public override long Length => throw new NotSupportedException();
        /// <summary>
        /// Set is not supported. Gets the position where next data will be written
        /// </summary>
        public override long Position {
            get => FilePointer + BufferSize;
            set => throw new NotSupportedException();
        }
        private AdlsOutputStream(string filename, AdlsClient client, string leaseId, AdlsArrayPool<byte> bufferPool, int bufferCapacity)
        {
            Filename = filename;
            Client = client;
            LeaseId = string.IsNullOrEmpty(leaseId) ? Guid.NewGuid().ToString() : leaseId;
            BufferSize = 0;
            if(bufferCapacity > BufferMaxCapacity)
            {
                throw new Exception($"BufferCApacity is too big. Maximum Capacity: {BufferMaxCapacity}");
            }
            else if(bufferCapacity < BufferMinCapacity)
            {
                throw new Exception($"BufferCApacity is too small. Minimum Capacity: {BufferMinCapacity}");
            }
            _bufferCapacity = bufferCapacity;
            if (bufferPool != null)
            {
                _bufferPool = bufferPool;
            }
            else
            {
                Buffer = new byte[_bufferCapacity];
            }
        }

        protected AdlsOutputStream()
        {
            
        }
        internal static async Task<AdlsOutputStream> GetAdlsOutputStreamAsync(string filename, AdlsClient client, bool isNew, string leaseId, AdlsArrayPool<byte> bufferPool, int bufferCapacity)
        {
            var adlsOpStream = new AdlsOutputStream(filename, client, leaseId, bufferPool, bufferCapacity);
            await adlsOpStream.InitializeFileSizeAsync(isNew).ConfigureAwait(false);
            if (OutStreamLog.IsTraceEnabled)
            {
                OutStreamLog.Trace($"ADLFileOutputStream, Created for client {client.ClientId} for file {filename}, create={isNew}");
            }
            return adlsOpStream;
        }

        /// <summary>
        /// If buffer pool is apssed then rent from the pool if buffer is released from the alst flush
        /// </summary>
        private async Task CreateBufferIfNotInitializedAsync()
        {
            if(_bufferPool != null && Buffer == null)
            {
                Buffer = await _bufferPool.RentAsync(_bufferCapacity).ConfigureAwait(false);
                if (Buffer == null || Buffer.Length < _bufferCapacity)
                {
                    throw new OutOfMemoryException($"Could not rent a buffer of size {_bufferCapacity}");
                }
            }
        }

        /// <summary>
        /// If buffer is rented from bufferpool, then release the buffer to buffer pool while flush is called
        /// Else buffer is released only when disposing is true
        /// </summary>
        /// <param name="disposing"></param>
        private async Task ReleaseBufferIfInitializedAsync(bool disposing = false)
        {
            if (_bufferPool != null)
            {
                if (Buffer != null)
                {
                   await _bufferPool.ReturnAsync(Buffer, true).ConfigureAwait(false);
                    Buffer = null;
                }
            }
            else if (disposing)
            {
                Buffer = null;
            }
        }

        /// <summary>
        /// Initialize the file size by doing a getfilestatus if we are creating in append mode
        /// </summary>
        /// <param name="isNew">True if we are creating the file else false</param>
        /// <returns></returns>
        private async Task InitializeFileSizeAsync(bool isNew)
        {
            if (isNew)
            {
                FilePointer = 0;
            }
            else
            {
                OperationResponse resp = new OperationResponse();
                //Initialize the filepointer to the current length of file
                // Pass getConsistentlength so that we get the updated length of stream
                DirectoryEntry diren = await Core.GetFileStatusAsync(Filename, UserGroupRepresentation.ObjectID, Client,
                    new RequestOptions(new ExponentialRetryPolicy()), resp, default(CancellationToken), true).ConfigureAwait(false);
                if (diren == null)
                {
                    throw Client.GetExceptionFromResponse(resp, "Error in getting metadata while creating InputStream for file " + Filename + ".");
                }
                FilePointer = diren.Length;
            }
        }
        /// <summary>
        /// Asynchronously flushes data from buffer to server and updates the metadata
        /// </summary>
        /// <param name="cancelToken">Cancellation token</param>
        public override async Task FlushAsync(CancellationToken cancelToken)
        {
            if (_isDisposed)
            {
                throw new ObjectDisposedException("Stream is closed");
            }
            // TODO test double flush
            await WriteServiceAsync(SyncFlag.METADATA, cancelToken).ConfigureAwait(false);
            await ReleaseBufferIfInitializedAsync().ConfigureAwait(false);
        }
        /// <summary>
        /// Synchronously flushes data from buffer to server and updates the metadata
        /// </summary>
        public override void Flush()
        {
            if (_isDisposed)
            {
                throw new ObjectDisposedException("Stream is closed");
            }
            // TODO test double flush
            WriteService(SyncFlag.METADATA);
            ReleaseBufferIfInitializedAsync().GetAwaiter().GetResult();
        }

        /// <summary>
        /// Not supported
        /// </summary>
        /// <param name="buffer"></param>
        /// <param name="offset"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public override int Read(byte[] buffer, int offset, int count)
        {
            throw new NotSupportedException();
        }
        /// <summary>
        /// Not supported
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="origin"></param>
        /// <returns></returns>
        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotSupportedException();
        }
        /// <summary>
        /// Not supported
        /// </summary>
        /// <param name="value"></param>
        public override void SetLength(long value)
        {
            throw new NotSupportedException();
        }
        /// <summary>
        /// Copies data to internal buffer, and updates the internal buffer pointer
        /// </summary>
        /// <param name="buffer">Input byte array</param>
        /// <param name="offset">Offset in the byte array</param>
        /// <param name="count">length to copy</param>
        private void AddDataToBuffer(byte[] buffer, int offset, int count)
        {
            System.Buffer.BlockCopy(buffer, offset, Buffer, BufferSize, count);
            BufferSize += count;
        }
        /// <summary>
        /// Verifies write arguments
        /// </summary>
        /// <param name="buffer">Byte buffer</param>
        /// <param name="offset">Offset</param>
        /// <param name="count">Count</param>
        private void WriteVerify(byte[] buffer, int offset, int count)
        {
            if (buffer == null)
            {
                throw new ArgumentNullException(nameof(buffer));
            }
            if (buffer != null && (offset >= buffer.Length || (offset < 0) || (count + offset > buffer.Length)))
            {
                throw new ArgumentOutOfRangeException(nameof(offset));
            }
            if (_isDisposed)
            {
                throw new ObjectDisposedException("Stream is disposed");

            }
            if (OutStreamLog.IsTraceEnabled)
            {
                OutStreamLog.Trace($"ADLFileOutputStream, Stream write of size {count} for file {Filename} for client {Client.ClientId}");
            }
        }
        /// <summary>
        /// Writes data to internal buffer. If the buffer fills up then writes to the file in server.
        /// Does it asynchronously
        /// </summary>
        /// <param name="buffer">Input byte array containing the Data to write</param>
        /// <param name="offset">Offset in buffer</param>
        /// <param name="count">Count of bytes to write</param>
        /// <param name="cancelToken">Cancellation token</param>
        public override async Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancelToken)
        {
            WriteVerify(buffer, offset, count);
            await CreateBufferIfNotInitializedAsync().ConfigureAwait(false);
            //If putting data in buffer will overflow the buffer
            if (BufferSize + count > _bufferCapacity)
            {
                //If data size is less than 4MB then we should gurantee that the write to be atomic
                if (count <= _bufferCapacity)
                {
                    await WriteServiceAsync(SyncFlag.DATA, cancelToken).ConfigureAwait(false);
                }
                else
                {
                    //Else we just continue writing data to server until the left data is less than buffer size
                    while (BufferSize + count > _bufferCapacity)
                    {
                        int toCopy = _bufferCapacity - BufferSize;
                        AddDataToBuffer(buffer, offset, toCopy);//Adds data to buffer to write to server
                        count -= toCopy;
                        offset += toCopy;
                        await WriteServiceAsync(SyncFlag.DATA, cancelToken).ConfigureAwait(false);//Writes the buffer to server
                    }
                }
            }
            AddDataToBuffer(buffer, offset, count);
        }
        /// <summary>
        /// Writes data to internal buffer. If the buffer fills up then writes to the file in server.
        /// Does it synchronously
        /// </summary>
        /// <param name="buffer">Input byte array containing the Data to write</param>
        /// <param name="offset">Offset in buffer</param>
        /// <param name="count">Count of bytes to write</param>
        public override void Write(byte[] buffer, int offset, int count)
        {
            WriteVerify(buffer, offset, count);
            CreateBufferIfNotInitializedAsync().GetAwaiter().GetResult();
            //If putting data in buffer will overflow the buffer
            if (BufferSize + count > _bufferCapacity)
            {
                //If data size is less than 4MB then we should gurantee that the write to be atomic
                if (count <= _bufferCapacity)
                {
                    WriteService(SyncFlag.DATA);
                }
                else
                {
                    //Else we just continue writing data to server until the left data is less than buffer size
                    while (BufferSize + count > _bufferCapacity)
                    {
                        int toCopy = _bufferCapacity - BufferSize;
                        AddDataToBuffer(buffer, offset, toCopy);//Adds data to buffer to write to server
                        count -= toCopy;
                        offset += toCopy;
                        WriteService(SyncFlag.DATA);//Writes the buffer to server
                    }
                }
            }
            AddDataToBuffer(buffer, offset, count);
        }
        /// <summary>
        /// Releases the unmanaged resources used by the Stream and optionally releases the managed resources
        /// </summary>
        /// <param name="disposing">true to release both managed and unmanaged resources; false to release only unmanaged resources</param>
        protected override void Dispose(bool disposing)
        {
            if (_isDisposed)
            {
                return;
            }
            try
            {
                if (disposing)
                {   
                    // TODO test for dispose after flush
                    //Based on Dispose Pattern reference objects shouldnt be touched during finalizer
                    WriteService(SyncFlag.CLOSE);
                }
            }
            finally
            {
                BufferSize = 0;
                FilePointer = 0;
                if (disposing)
                {
                    ReleaseBufferIfInitializedAsync(true).GetAwaiter().GetResult();
                    _isDisposed = true;
                    if (OutStreamLog.IsTraceEnabled)
                    {
                        OutStreamLog.Trace($"ADLFileOutputStream, Stream closed for file {Filename} for client {Client.ClientId}");
                    }
                }
                base.Dispose(disposing);
            }
        }
        /// <summary>
        /// Makes a Append call to server to write data in buffer. Resets the FilePointer and BufferSize.
        /// This is an asynchronous call.
        /// </summary>
        /// <param name="flag">Type of append- It is just data or to update metadata or close the lease </param>
        /// <param name="cancelToken">Cancellation token</param>
        /// <returns></returns>
        private async Task WriteServiceAsync(SyncFlag flag, CancellationToken cancelToken)
        {
            if (BufferSize == 0 && (flag == SyncFlag.DATA || (MetaDataSynced && flag == SyncFlag.METADATA)))
            {
                return;
            }
            if (OutStreamLog.IsTraceEnabled)
            {
                OutStreamLog.Trace($"ADLFileOutputStream, Stream flush of size {BufferSize} at offset {FilePointer} for file {Filename} for client {Client.ClientId}");
            }
            OperationResponse resp = new OperationResponse();
            await Core.AppendAsync(Filename, LeaseId, LeaseId, flag, FilePointer, Buffer, 0, BufferSize, Client, new RequestOptions(new ExponentialRetryPolicy()), resp, cancelToken).ConfigureAwait(false);
            if (!resp.IsSuccessful)
            {
                // if this was a retry and we get bad offset, then this might be because we got a transient
                // failure on first try, but request succeeded on back-end. In that case, the retry would fail
                // with bad offset. To detect that, we check if there was a retry done, and if the current error we
                // have is bad offset.
                // If so, do a zero-length append at the current expected Offset, and if that succeeds,
                // then the file length must be good - swallow the error. If this append fails, then the last append
                // did not succeed and we have some other offset on server - bubble up the error.
                if (resp.Retries > 0 && resp.HttpStatus == HttpStatusCode.BadRequest &&
                    resp.RemoteExceptionName.Equals("BadOffsetException"))
                {
                    bool zeroAppendIsSuccesful = await PerformZeroLengthAppendAsync(FilePointer + BufferSize, cancelToken).ConfigureAwait(false);
                    if (zeroAppendIsSuccesful)
                    {
                        if (OutStreamLog.IsDebugEnabled)
                        {
                            OutStreamLog.Debug($"ADLFileOutputStream, Zero size Append succeded and the expected FileSize is {FilePointer + BufferSize}, ignoring BadOffsetException for session {LeaseId} for file {Filename} for client {Client.ClientId}");
                        }
                        FilePointer += BufferSize;
                        BufferSize = 0;
                        MetaDataSynced = false;
                        return;
                    }
                    if (OutStreamLog.IsDebugEnabled)
                    {
                        OutStreamLog.Debug($"ADLFileOutputStream, Append failed at offset {FilePointer} for session {LeaseId} for file {Filename} for client {Client.ClientId}");
                    }
                }
                throw Client.GetExceptionFromResponse(resp, $"Error in appending for file {Filename} at offset {FilePointer}.");
            }
            MetaDataSynced = flag == SyncFlag.METADATA;//Make sure if metadata is already updated, then do not try to update metada again unless data has been written
            FilePointer += BufferSize;//Update the filepointer as data is written to server
            BufferSize = 0;//Resets buffersize since all data in buffer is flushed to server
        }
        /// <summary>
        /// Makes a Append call to server to write data in buffer. Resets the FilePointer and BufferSize.
        /// This is a synchronous call.
        /// </summary>
        /// <param name="flag">Type of append- It is just data or to update metadata or close the lease </param>
        /// <returns></returns>
        private void WriteService(SyncFlag flag)
        {
            if (BufferSize == 0 && (flag == SyncFlag.DATA || MetaDataSynced && flag == SyncFlag.METADATA))
            {
                return;
            }

            if (OutStreamLog.IsTraceEnabled)
            {
                OutStreamLog.Trace($"ADLFileOutputStream, Stream flush of size {BufferSize} at offset {FilePointer} for file {Filename} for client {Client.ClientId}");
            }
            OperationResponse resp = new OperationResponse();
            Core.Append(Filename, LeaseId, LeaseId, flag, FilePointer, Buffer, 0, BufferSize, Client, new RequestOptions(new ExponentialRetryPolicy()), resp);
            if (!resp.IsSuccessful)
            {
                // if this was a retry and we get bad offset, then this might be because we got a transient
                // failure on first try, but request succeeded on back-end. In that case, the retry would fail
                // with bad offset. To detect that, we check if there was a retry done, and if the current error we
                // have is bad offset.
                // If so, do a zero-length append at the current expected Offset, and if that succeeds,
                // then the file length must be good - swallow the error. If this append fails, then the last append
                // did not succeed and we have some other offset on server - bubble up the error.
                if (resp.Retries > 0 && resp.HttpStatus == HttpStatusCode.BadRequest &&
                    resp.RemoteExceptionName.Equals("BadOffsetException"))
                {
                    bool zeroAppendIsSuccesful = PerformZeroLengthAppend(FilePointer + BufferSize);
                    if (zeroAppendIsSuccesful)
                    {
                        if (OutStreamLog.IsDebugEnabled)
                        {
                            OutStreamLog.Debug($"ADLFileOutputStream, Zero size Append succeded and the expected FileSize is {FilePointer + BufferSize}, ignoring BadOffsetException for session {LeaseId} for file {Filename} for client {Client.ClientId}");
                        }
                        FilePointer += BufferSize;
                        BufferSize = 0;
                        MetaDataSynced = false;
                        return;
                    }
                    if (OutStreamLog.IsDebugEnabled)
                    {
                        OutStreamLog.Debug($"ADLFileOutputStream, Append failed at offset {FilePointer} for session {LeaseId} for file {Filename} for client {Client.ClientId}");
                    }
                }
                throw Client.GetExceptionFromResponse(resp, $"Error in appending for file {Filename} at offset {FilePointer}.");
            }
            MetaDataSynced = flag == SyncFlag.METADATA;//Make sure if metadata is already updated, then do not try to update metada again unless data has been written
            FilePointer += BufferSize;//Update the filepointer as data is written to server
            BufferSize = 0;//Resets buffersize since all data in buffer is flushed to server
        }
        /// <summary>
        /// Performs append of zero length to see whether the file is in consistent state with the client.
        /// This is an asynchronous operation.
        /// </summary>
        /// <param name="offsetFile">Offset in file at which the append will be made</param>
        /// <param name="cancelToken">Cancellation token</param>
        /// <returns></returns>
        private async Task<bool> PerformZeroLengthAppendAsync(long offsetFile, CancellationToken cancelToken)
        {
            OperationResponse resp = new OperationResponse();
            await Core.AppendAsync(Filename, LeaseId, LeaseId, SyncFlag.DATA, offsetFile, null, -1, 0, Client, new RequestOptions(new ExponentialRetryPolicy()), resp, cancelToken).ConfigureAwait(false);
            return resp.IsSuccessful;
        }
        /// <summary>
        /// Performs append of zero length to see whether the file is in consistent state with the client.
        /// This is a synchronous operation.
        /// </summary>
        /// <param name="offsetFile">Offset in file at which the append will be made</param>
        /// <returns></returns>
        private bool PerformZeroLengthAppend(long offsetFile)
        {
            OperationResponse resp = new OperationResponse();
            Core.Append(Filename, LeaseId, LeaseId, SyncFlag.DATA, offsetFile, null, -1, 0, Client, new RequestOptions(new ExponentialRetryPolicy()), resp);
            return resp.IsSuccessful;
        }
    }
}
