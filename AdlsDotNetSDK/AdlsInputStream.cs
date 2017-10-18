using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.DataLake.Store.RetryPolicies;
using NLog;

namespace Microsoft.Azure.DataLake.Store
{
    /// <summary>
    /// ADLS Input stream that reads data from a file on Data lake. It reads data in bulk from server to a buffer and then provides buffered output to the client as per request.
    /// Data can be read asynchronously/synchronously. Data can be read serially or from arbitrary points in file. Read is fully synchronous till the transport layer. ReadAsync is fully synchronous till the transport layer. 
    /// </summary>
    public class AdlsInputStream : Stream
    {
        /// <summary>
        /// Logger to log messages related to input stream
        /// </summary>
        private static readonly Logger IpStreamLog = LogManager.GetLogger("adls.dotnet.InputStream");
        /// <summary>
        /// Filename including the full path
        /// </summary>
        private string Filename { get; }
        /// <summary>
        /// UUID that is used to obtain the file handler (stream) easily at server
        /// </summary>
        private string SessionId { get; }
        /// <summary>
        /// ADLS client
        /// </summary>
        private AdlsClient Client { get; }
        /// <summary>
        /// Metadata of the file. Basically needed to know the total length of the file
        /// </summary>
        private DirectoryEntry Entry { get;}
        /// <summary>
        /// Internal buffer that stores the data read from server
        /// </summary>
        private byte[] Buffer { get; set; }
        /// <summary>
        /// Current pointer of the stream in the file. Expressed as bytes from the begining
        /// </summary>
        private long FilePointer { get; set; }
        /// <summary>
        /// Pointer within the internal buffer array
        /// </summary>
        private long BufferPointer { get; set; }
        /// <summary>
        /// Number of bytes read from the server
        /// </summary>
        private long BufferSize { get; set; }
        /// <summary>
        /// Maximum size of the internal buffer
        /// </summary>
        private int BufferCapacity { get; }

        /// <summary>
        /// Default Maximum size of the internal buffer
        /// </summary>
        internal const int DefaultBufferCapacity = 4 * 1024 * 1024;
        /// <summary>
        /// Flag whether stream is disposed
        /// </summary>
        private bool _isDisposed;
        /// <summary>
        /// Whether stream can read data
        /// </summary>
        public override bool CanRead => true;
        /// <summary>
        /// Whether the stream can seek data
        /// </summary>
        public override bool CanSeek => true;
        /// <summary>
        /// Whether the stream can write data
        /// </summary>
        public override bool CanWrite => false;
        /// <summary>
        /// total Length of the file
        /// </summary>
        public override long Length => Entry.Length;
        /// <summary>
        /// Position of the stream from begining
        /// </summary>
        public override long Position { get => FilePointer; set => Seek(value, SeekOrigin.Begin); }
        /// <summary>
        /// Not supported
        /// </summary>
        public override void Flush()
        {
            throw new NotSupportedException();
        }

        internal AdlsInputStream(string filename, AdlsClient client, DirectoryEntry der,int bufferCapacity=DefaultBufferCapacity)
        {
            Filename = filename;
            Client = client;
            Entry = der;
            SessionId = Guid.NewGuid().ToString();
            FilePointer = 0;
            BufferPointer = 0;
            BufferSize = 0;
            BufferCapacity = bufferCapacity;
            Buffer = new byte[BufferCapacity];
            if (IpStreamLog.IsTraceEnabled)
            {
                IpStreamLog.Trace($"ADLFileInputStream, Created for file {Filename}, client {client.ClientId}");
            }
        }
        /// <summary>
        /// Reads a sequence of bytes from the current stream and advances the position within the stream by the number of bytes read
        /// Synchronous operation.
        /// </summary>
        /// <param name="output">Output byte array</param>
        /// <param name="offset">offset at which data should be put in the output array</param>
        /// <param name="count">Count of the bytes read</param>
        /// <returns>Number of bytes read</returns>
        public override int Read(byte[] output, int offset, int count)
        {
            if (!BeforeReadService(output, offset, count))
            {
                return 0;
            }
            //Read pointer of the internal buffer has reached the end
            if (BufferPointer == BufferSize)
            {
                int bytesRead = ReadService();
                if (bytesRead == 0) { return 0; }
            }
            return AfterReadService(output, offset, count);
        }
        /// <summary>
        /// Reads a sequence of bytes from the current stream and advances the position within the stream by the number of bytes read
        /// Asynchronous operation.
        /// </summary>
        /// <param name="output">Output byte array</param>
        /// <param name="offset">offset at which data should be put in the output array</param>
        /// <param name="count">Count of the bytes read</param>
        /// <param name="cancelToken">Cancellation Token</param>
        /// <returns>Number of bytes read</returns>
        public override async Task<int> ReadAsync(byte[] output, int offset, int count, CancellationToken cancelToken)
        {
            if (!BeforeReadService(output, offset, count))
            {
                return 0;
            }
            //Read pointer of the internal buffer has reached the end
            if (BufferPointer == BufferSize)
            {
                int bytesRead = await ReadServiceAsync(cancelToken);
                if (bytesRead == 0) { return 0; }
            }
            return AfterReadService(output, offset, count);
        }
        /// <summary>
        /// Verifies the Read arguments before it tries to read actual data
        /// </summary>
        /// <param name="output">Output byte array</param>
        /// <param name="offset">offset at which data should be put in the output array</param>
        /// <param name="count">Count of the bytes read</param>
        /// <returns>False if we have reached end of the file else true</returns>
        private bool BeforeReadService(byte[] output, int offset, int count)
        {
            if (output == null)
            {
                throw new ArgumentNullException(nameof(output));
            }
            if (_isDisposed)
            {
                throw new ObjectDisposedException("Stream is disposed");
            }
            if (FilePointer >= Entry.Length) return false;
            if (output != null && (offset >= output.Length || offset < 0))
            {
                throw new ArgumentOutOfRangeException(nameof(offset));
            }
            if (output != null && count + offset > output.Length)
            {
                throw new ArgumentOutOfRangeException(nameof(count));
            }
            if (IpStreamLog.IsTraceEnabled)
            {
                IpStreamLog.Trace("ADLFileInputStream, Stream read at offset {0} for file {1} for client {2}", FilePointer, Filename, Client.ClientId);
            }
            return true;
        }
        /// <summary>
        /// Copies data from it's internal buffer to output.
        /// </summary>
        /// <param name="output">Output byte array</param>
        /// <param name="offset">offset at which data should be put in the output array</param>
        /// <param name="count">Count of the bytes read</param>
        /// <returns>Number of bytes read</returns>
        private int AfterReadService(byte[] output, int offset, int count)
        {
            long bytesRemaining = BufferSize - BufferPointer;//Bytes that are not yet read from buffer
            long toCopyBytes = Math.Min(bytesRemaining, count);//Number of bytes to be returned to the user, if no. of bytes left in buffer is less than what user has wanted then return only those that are left
            System.Buffer.BlockCopy(Buffer, (int)BufferPointer, output, offset, (int)toCopyBytes);
            BufferPointer += toCopyBytes;//Update the read pointer of the internal buffer
            FilePointer += toCopyBytes;
            return (int)toCopyBytes;
        }
        /// <summary>
        /// Makes the call to server to read data in bulk. Resets Buffer pointer and size. Asynchronous operation.
        /// </summary>
        /// <param name="cancelToken">Cancellation Token</param>
        /// <returns>Number of bytes read</returns>
        private async Task<int> ReadServiceAsync(CancellationToken cancelToken)
        {
            if (BufferPointer < BufferSize) return 0;
            if (FilePointer >= Entry.Length) return 0;
            if (IpStreamLog.IsTraceEnabled)
            {
                IpStreamLog.Trace($"ADLFileInputStream.ReadServiceAsync, Read from server at offset {FilePointer} for file {Filename} for client {Client.ClientId}");
            }
            OperationResponse resp = new OperationResponse();
            BufferSize = await Core.OpenAsync(Filename, SessionId, FilePointer, Buffer, 0, BufferCapacity, Client, new RequestOptions(new ExponentialRetryPolicy()), resp, cancelToken);
            if (!resp.IsSuccessful)
            {
                throw Client.GetExceptionFromResponse(resp, $"Error in reading file {Filename} at offset {FilePointer}.");
            }
            BufferPointer = 0;//Resets the read pointer since new data is read into buffer
            return (int)BufferSize;
        }
        /// <summary>
        /// Makes the call to server to read data in bulk. Resets Buffer pointer and size. Asynchronous operation.
        /// </summary>
        /// <returns>Number of bytes read</returns>
        private int ReadService()
        {
            if (BufferPointer < BufferSize) return 0;
            if (FilePointer >= Entry.Length) return 0;
            if (IpStreamLog.IsTraceEnabled)
            {
                IpStreamLog.Trace($"ADLFileInputStream.ReadServiceAsync, Read from server at offset {FilePointer} for file {Filename} for client {Client.ClientId}");
            }
            OperationResponse resp = new OperationResponse();
            BufferSize = Core.Open(Filename, SessionId, FilePointer, Buffer, 0, BufferCapacity, Client, new RequestOptions(new ExponentialRetryPolicy()), resp);
            if (!resp.IsSuccessful)
            {
                throw Client.GetExceptionFromResponse(resp, $"Error in reading file {Filename} at offset {FilePointer}.");
            }
            BufferPointer = 0;//Resets the read pointer since new data is read into buffer
            return (int)BufferSize;
        }
        /// <summary>
        /// Reads a sequence of bytes directly from the server. It does not update anything in the stream.
        /// </summary>
        /// <param name="position">Position in the file from where it should start reading data</param>
        /// <param name="output">Output byte array</param>
        /// <param name="offset">offset at which data should be put in the output array</param>
        /// <param name="count">Count of the bytes read</param>
        /// <returns>Number of bytes read</returns>
        public int Read(long position, byte[] output, int offset, int count)
        {
            return ReadAsync(position, output, offset, count).GetAwaiter().GetResult();
        }
        /// <summary>
        /// Reads a sequence of bytes directly from the server. It does not update anything in the stream.
        /// </summary>
        /// <param name="position">Position in the file from where it should start reading data</param>
        /// <param name="output">Output byte array</param>
        /// <param name="offset">offset at which data should be put in the output array</param>
        /// <param name="count">Count of the bytes read</param>
        /// <returns>Number of bytes read</returns>
        public async Task<int> ReadAsync(long position, byte[] output, int offset, int count)
        {
            if (output == null)
            {
                throw new ArgumentNullException(nameof(output));
            }
            if (output != null && (offset >= output.Length || offset < 0))
            {
                throw new ArgumentOutOfRangeException(nameof(offset));
            }
            if (output != null && count + offset > output.Length)
            {
                throw new ArgumentOutOfRangeException(nameof(count));
            }
            OperationResponse resp = new OperationResponse();
            int bytesRead = await Core.OpenAsync(Filename, SessionId, position, output, offset, count, Client, new RequestOptions(new ExponentialRetryPolicy()), resp);
            if (!resp.IsSuccessful)
            {
                throw Client.GetExceptionFromResponse(resp, $"Error in reading file {Filename} at offset {position}.");
            }
            if (IpStreamLog.IsTraceEnabled)
            {
                IpStreamLog.Trace($"ADLFileInputStream.Read, Read offset {offset} for file {Filename} for client {Client.ClientId}");
            }
            return bytesRead;
        }
        /// <summary>
        /// Updates the position of the stream based on SeekOrigin
        /// </summary>
        /// <param name="offset">Byte offset relative to the origin parameter</param>
        /// <param name="origin">A value of type SeekOrigin indicating the reference point used to obtain the new position.</param>
        /// <returns>Current new position of the stream</returns>
        public override long Seek(long offset, SeekOrigin origin)
        {
            if (IpStreamLog.IsTraceEnabled)
            {
                IpStreamLog.Trace($"ADLFileInputStream, Seek to offset {offset} from {Enum.GetName(typeof(SeekOrigin), origin)} for file {Filename} for client {Client.ClientId}");
            }
            if (_isDisposed)
            {
                throw new ObjectDisposedException("Stream is disposed");
            }
            long prevFilePointer = FilePointer;//Store previous Filepointer
            if (origin == SeekOrigin.Current)
            {
                FilePointer += offset;
            }
            else if (origin == SeekOrigin.Begin)
            {
                FilePointer = offset;
            }
            else//SeekOrigin.End
            {
                FilePointer = Entry.Length + offset;
            }
            if (FilePointer < 0)
            {
                FilePointer = prevFilePointer;
                throw new IOException("Cannot seek before the begining of the file");
            }
            if (FilePointer > Entry.Length)
            {
                FilePointer = prevFilePointer;
                throw new IOException("Cannot seek beyond the end of the file");
            }
            long diffFilePointer = FilePointer - prevFilePointer;//Calculate the offset between current pointer and new pointer
            BufferPointer += diffFilePointer;//Update the BufferPointer based on how much the FilePointer moved
            if (BufferPointer < 0 || BufferPointer >= BufferSize)
            {
                BufferPointer = BufferSize = 0;//Current Filepointer points to data which does not exist in buffer
            }
            return FilePointer;
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
        /// Not supported
        /// </summary>
        /// <param name="buffer"></param>
        /// <param name="offset"></param>
        /// <param name="count"></param>
        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new NotSupportedException();
        }

        /// <summary>
        /// Releases the unmanaged resources used by the Stream and optionally releases the managed resources
        /// </summary>
        /// <param name="disposing">true to release both managed and unmanaged resources; false to release only unmanaged resources</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                Buffer = null;
                _isDisposed = true;
            }
            BufferSize = BufferPointer = 0;
            FilePointer = 0;
            if (IpStreamLog.IsTraceEnabled)
            {
                IpStreamLog.Trace($"ADLFileInputStream, Closed for file {Filename}, client {Client.ClientId}");
            }
            base.Dispose(disposing);
        }

    }
}
