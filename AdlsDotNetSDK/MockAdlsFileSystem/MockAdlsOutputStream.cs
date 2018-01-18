
using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.Azure.DataLake.Store.MockAdlsFileSystem
{
    /// <summary>
    /// Mock Adls Output stream for unit test
    /// </summary>
    public sealed class MockAdlsOutputStream : AdlsOutputStream
    {
        private readonly Stream _internalStream;
        /// <summary>
        /// Set is not supported. Gets the position where next data will be written
        /// </summary>
        public override long Position
        {
            get => _internalStream.Position;

            set => throw new NotSupportedException();
        }

        internal MockAdlsOutputStream(Stream internalStream)
        {
            _internalStream = internalStream;
        }
        /// <summary>
        /// Asynchronously flushes data from buffer to underlying stream and updates the metadata
        /// </summary>
        /// <param name="cancelToken">Cancellation token</param>
        public override async Task FlushAsync(CancellationToken cancelToken)
        {
            await _internalStream.FlushAsync(cancelToken).ConfigureAwait(false);
        }
        /// <summary>
        /// Synchronously flushes data from buffer to underlying stream and updates the metadata
        /// </summary>
        public override void Flush()
        {
            _internalStream.Flush();
        }
        /// <summary>
        /// Writes data to internal buffer. If the buffer fills up then writes to the underlying stream.
        /// Does it asynchronously
        /// </summary>
        /// <param name="buffer">Input byte array containing the Data to write</param>
        /// <param name="offset">Offset in buffer</param>
        /// <param name="count">Count of bytes to write</param>
        /// <param name="cancelToken">Cancellation token</param>
        public override async Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancelToken)
        {
            await _internalStream.WriteAsync(buffer, offset, count, cancelToken).ConfigureAwait(false);
        }
        /// <summary>
        /// Writes data to internal buffer. If the buffer fills up then writes to the underlying stream.
        /// Does it synchronously
        /// </summary>
        /// <param name="buffer">Input byte array containing the Data to write</param>
        /// <param name="offset">Offset in buffer</param>
        /// <param name="count">Count of bytes to write</param>
        public override void Write(byte[] buffer, int offset, int count)
        {
            _internalStream.Write(buffer, offset, count);
        }
        /// <summary>
        /// Releases the unmanaged resources used by the Stream and optionally releases the managed resources. For this implementation, we do
        /// not dispose the underlying stream since we use the stream for both read and write.
        /// </summary>
        /// <param name="disposing">true to release both managed and unmanaged resources; false to release only unmanaged resources</param>
        protected override void Dispose(bool disposing)
        {
            // Flush the stream and take it to begining, This is the memory stream for whole file so do not close it
            _internalStream.Flush();
            _internalStream.Seek(0, SeekOrigin.Begin);
        }
    }
}
