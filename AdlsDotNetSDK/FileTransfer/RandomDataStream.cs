using System;
using System.Threading;
using System.IO;
using System.Security.Cryptography;

namespace Microsoft.Azure.DataLake.Store.FileTransfer

{
    internal class RandomDataStream : Stream
    {
        private static int size = 1024 * 1024 * 16;
        private static byte[] internalBuffer = new byte[size * 2];
        long cursor = 0;
        long streamLength = -1;

        static RandomDataStream()
        {
            RandomNumberGenerator prng = RandomNumberGenerator.Create();
            byte[] random = new byte[size];
            prng.GetBytes(random);
            Buffer.BlockCopy(random, 0, internalBuffer, 0, size);
            Buffer.BlockCopy(random, 0, internalBuffer, size, size);
        }

        internal RandomDataStream()
            : this(-1)
        {
        }

        internal RandomDataStream(long length)
        {
            streamLength = length;
        }

        public byte[] InternalBuffer
        {
            get { return internalBuffer; }
        }

        public int InternalBufferSize
        {
            get { return size; }
        }


        public override bool CanRead
        {
            get { return true; }
        }

        public override bool CanSeek
        {
            get { return true; }
        }

        public override bool CanWrite
        {
            get { return false; }
        }

        public override void Flush()
        {
        }

        public override long Length
        {
            get
            {
                if (streamLength != -1) { return streamLength; }
                else { throw new NotImplementedException(); }
            }
        }

        public override long Position
        {
            get
            {
                return Interlocked.Read(ref cursor);
            }
            set
            {
                Seek(Position, SeekOrigin.Begin);
            }
        }

        public Stream GetSubstream(long offset, int length)
        {
            if (length > size) throw new ArgumentOutOfRangeException(nameof(length));
            if (offset < 0) throw new ArgumentOutOfRangeException(nameof(offset));
            int bufferOffset = (int)(offset % size);                         // this will fit in an int since size is int
            return new MemoryStream(internalBuffer, bufferOffset, size);
        }

        public Stream GetSubstream(int count)
        {
            if (count > size) throw new ArgumentOutOfRangeException(nameof(count));
            long tCursor;
            int tCount;
            do
            {
                tCursor =  Interlocked.Read(ref cursor); 
                tCount = count;
                if (tCursor + tCount > streamLength) tCount = (int)(streamLength - tCursor);
                if (tCount < 1) return null;
            } while (Interlocked.CompareExchange(ref cursor, tCursor + tCount, tCursor) != tCursor);
            int bufferOffset = (int)(tCursor % size);                         // this will fit in an int since size is int
            return new MemoryStream(internalBuffer, bufferOffset, tCount);
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            if (count > size) count = size;
            long tCursor;
            int tCount;
            do
            {
                tCursor = Interlocked.Read(ref cursor);
                tCount = count;
                if (tCursor + tCount > streamLength) tCount = (int)(streamLength - tCursor);
                if (tCount < 1) return 0;
            } while (Interlocked.CompareExchange(ref cursor, tCursor + tCount, tCursor) != tCursor);
            Buffer.BlockCopy(internalBuffer, (int)(tCursor % size), buffer, offset, tCount);
            return tCount;
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            long newCursor = cursor;

            if (origin == SeekOrigin.Begin)
            {
                newCursor = offset % size;
                if (newCursor > streamLength - 1) newCursor = streamLength - 1;
                Interlocked.Exchange(ref cursor, newCursor);
            }

            if (origin == SeekOrigin.Current)
            {
                //Do this: cursor = cursor+offset;
                long tCursor;
                do
                {
                    tCursor = Interlocked.Read(ref cursor);
                    long tOffset = offset;
                    if (tCursor + tOffset >= streamLength) tOffset = (streamLength - 1) - tCursor;
                    newCursor = tCursor + tOffset;
                } while (Interlocked.CompareExchange(ref cursor, newCursor, tCursor) != tCursor);
            }

            if (origin == SeekOrigin.End)
            {
                if (offset <= 0 && streamLength != -1 && (-offset < streamLength))
                {
                    newCursor = streamLength + offset;
                    Interlocked.Exchange(ref cursor, newCursor);
                }
            }
            return newCursor;
        }

        public override void SetLength(long value)
        {
            if (value > 0) streamLength = value;
            if (cursor > streamLength) cursor = streamLength;   // this *should* be ok without interlocked - need to think more
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new NotImplementedException();
        }
    }
}
