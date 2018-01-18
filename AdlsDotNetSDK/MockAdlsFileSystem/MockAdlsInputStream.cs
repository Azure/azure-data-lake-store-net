

using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.Azure.DataLake.Store.MockAdlsFileSystem
{
    /// <summary>
    /// Mock Adls Input stream for unit test
    /// </summary>
    public sealed class MockAdlsInputStream : AdlsInputStream
    {
        // Internal stream - If set then no need to look at the service, for testing purposes
        private readonly Stream _internalStream;
        /// <summary>
        /// Length of the stream, Cannot be set only retrieved
        /// </summary>
        public override long Length => _internalStream.Length;
        /// <summary>
        /// Position of the stream from begining
        /// </summary>
        public override long Position
        {
            get => _internalStream.Position;
            set => Seek(value, SeekOrigin.Begin);
        }

        internal MockAdlsInputStream(Stream internalStream)
        {
            _internalStream = internalStream;
        }
        /// <summary>
        /// Reads a sequence of bytes from the current underlying stream and advances the position within the stream by the number of bytes read
        /// Synchronous operation.
        /// </summary>
        /// <param name="output">Output byte array</param>
        /// <param name="offset">offset at which data should be put in the output array</param>
        /// <param name="count">Count of the bytes read</param>
        /// <returns>Number of bytes read</returns>
        public override int Read(byte[] output, int offset, int count)
        {
            return _internalStream.Read(output, offset, count);
        }
        /// <summary>
        /// Reads a sequence of bytes from the current underlying stream and advances the position within the stream by the number of bytes read
        /// Asynchronous operation.
        /// </summary>
        /// <param name="output">Output byte array</param>
        /// <param name="offset">offset at which data should be put in the output array</param>
        /// <param name="count">Count of the bytes read</param>
        /// <param name="cancelToken">Cancellation Token</param>
        /// <returns>Number of bytes read</returns>
        public override async Task<int> ReadAsync(byte[] output, int offset, int count, CancellationToken cancelToken)
        {
            return await _internalStream.ReadAsync(output, offset, count, cancelToken).ConfigureAwait(false);
        }
        /// <summary>
        /// Updates the position of the underlying stream based on SeekOrigin
        /// </summary>
        /// <param name="offset">Byte offset relative to the origin parameter</param>
        /// <param name="origin">A value of type SeekOrigin indicating the reference point used to obtain the new position.</param>
        /// <returns>Current new position of the stream</returns>
        public override long Seek(long offset, SeekOrigin origin)
        {
            return _internalStream.Seek(offset, origin);
        }
        /// <summary>
        /// Releases the unmanaged resources used by the Stream and optionally releases the managed resources. For this implementation, we do
        /// not dispose the underlying stream since we use the stream for both read and write.
        /// </summary>
        /// <param name="disposing">true to release both managed and unmanaged resources; false to release only unmanaged resources</param>
        protected override void Dispose(bool disposing)
        {
            _internalStream.Seek(0, SeekOrigin.Begin);
        }
    }
}
