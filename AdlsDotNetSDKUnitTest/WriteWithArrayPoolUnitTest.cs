using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.Azure.DataLake.Store.UnitTest
{
    /// <summary>
    /// Class which creates an array of arraypools of type T
    /// </summary>
    /// <typeparam name="T">the type parameter for the class</typeparam>
    public class FixedStandardArrayPool<T> : AdlsArrayPool<T>
    {
        private int _numberOfRentCalled = 0;
        internal int NumberOfRentCalled { get { return _numberOfRentCalled; } }
        private int _numberOfReturnCalled = 0;
        internal int NumberOfReturnCalled { get { return _numberOfReturnCalled; } }
        /// <summary>
        /// Size of the each buffer array
        /// </summary>
        private readonly int bufferSize;

        /// <summary>
        /// Pool of buffer arrays that may be reused
        /// </summary>
        private readonly BlockingCollection<T[]> pool;

        /// <summary>
        /// Total number of objects created 
        /// </summary>
        private int totalObjectsCreated;

        /// <summary>
        /// Initializes a new instance of the <see cref="FixedStandardArrayPool{T}"/> class.
        /// </summary>
        /// <param name="bufferSize">Size of the buffers that can be rented</param>
        /// <param name="concurrency">Number of concurrent rents allowed</param>
        public FixedStandardArrayPool(int bufferSize, int concurrency)
        {
            this.pool = new BlockingCollection<T[]>(concurrency);
            this.totalObjectsCreated = 0;
            this.bufferSize = bufferSize;
        }

        /// <summary>
        /// Rent a buffer from the pool.
        /// 
        /// The array returned by Rent is owned by the caller of rent, but should be returned to 
        /// the pool via a call to Return when the renter no longer needs a copy.
        /// </summary>
        /// <param name="minimumLength">ignored value</param>
        /// <returns>Array of type <see cref="T"/> with length <see cref="bufferSize"/></returns>
        public override T[] Rent(int minimumLength)
        {
            T[] buffer;
            Interlocked.Increment(ref _numberOfRentCalled);
            if (!this.pool.TryTake(out buffer))
            {
                // Create a new one if the pool is not full
                lock (this)
                {
                    if (totalObjectsCreated < this.pool.BoundedCapacity)
                    {
                        buffer = new T[this.bufferSize];
                        totalObjectsCreated++;
                    }
                }

                // No new T was created, so wait for a release to happen.
                if (buffer == null)
                {
                    buffer = this.pool.Take();
                }
            }

            return buffer;
        }

        public override Task<T[]> RentAsync(int minimumLength)
        {
            return Task.FromResult(Rent(minimumLength));
        }

        /// <summary>
        /// Return a buffer to the pool
        /// </summary>
        /// <param name="buffer">Buffer for return</param>
        /// <param name="clearArray">Should array be cleared before returning to the pool</param>
        public override void Return(T[] buffer, bool clearArray = false)
        {
            if (clearArray)
            {
                Array.Clear(buffer, 0, buffer.Length);
            }
            Interlocked.Increment(ref _numberOfReturnCalled);
            lock (this)
            {
                // If we have over allocated or returned an array which is not matching in length, then we will not store this buffer.
                if (this.pool.Count >= this.pool.BoundedCapacity || buffer.Length != this.bufferSize)
                {
                    return;
                }

                // Otherwise Add it to Queue
                this.pool.Add(buffer);
            }
        }

        public override Task ReturnAsync(T[] array, bool clearArray = false)
        {
            Return(array, clearArray);
            return Task.FromResult(default(T));
        }

        internal void Reset()
        {
            _numberOfRentCalled = 0;
            _numberOfReturnCalled = 0;
        }
    }

    [TestClass]
    public class WriteWithArrayPoolUnitTest
    {
        private static FixedStandardArrayPool<byte> _arrayPool;
        
        /// <summary>
        /// Adls Client
        /// </summary>
        private static AdlsClient _adlsClient;
        private static readonly string RemotePath = "/ReadWriteWithArrayPoolUnitTest" + SdkUnitTest.TestId;
        [ClassInitialize]
        public static void SetupTest(TestContext context)
        {
            _arrayPool = new FixedStandardArrayPool<byte>(4 * 1024 * 1024, 2);
            _adlsClient = SdkUnitTest.SetupSuperClient();
        }

        [TestMethod]
        public void SerialCreateAndAppend()
        {
            int count = 3;
            string path = RemotePath + "/SerialCreateAndAppend";
            int totLength = 8 * 1024 * 1024;
            string text1 = SdkUnitTest.RandomString(totLength);
            byte[] textByte1 = Encoding.UTF8.GetBytes(text1);
            for (int index = 0; index < count; index++)
            {
                using (var stream = _adlsClient.CreateFile(path + index, IfExists.Overwrite, _arrayPool, 4 * 1024 * 1024))
                {
                    // Flush in empty buffer, no return called
                    stream.Flush();
                    // Rent called
                    stream.Write(textByte1, 0, textByte1.Length);
                    //Return called
                    stream.Flush();
                    stream.Flush();
                    // Rent called
                    stream.Write(textByte1, 0, textByte1.Length);
                    // Rent should not be called
                    stream.Write(textByte1, 0, textByte1.Length);
                }
            };
            Assert.IsTrue(_arrayPool.NumberOfRentCalled == count * 2);
            Assert.IsTrue(_arrayPool.NumberOfReturnCalled == count * 2);
            for (int index = 0; index < count; index++)
            {
                string output = "";
                using (var istream = _adlsClient.GetReadStream(path + index))
                {
                    int noOfBytes;
                    byte[] buffer = new byte[2 * 1024 * 1024];
                    do
                    {
                        noOfBytes = istream.Read(buffer, 0, buffer.Length);
                        output += Encoding.UTF8.GetString(buffer, 0, noOfBytes);
                    } while (noOfBytes > 0);
                }
                Assert.IsTrue(output.Equals(text1 + text1 + text1));
            };
            _arrayPool.Reset();
        }

        [TestMethod]
        public void ParallelCreateAndAppend()
        {
            int count = 3;
            string path = RemotePath + "/ParallelCreateAndAppend";
            int totLength = 2 * 1024 * 1024;
            string text1 = SdkUnitTest.RandomString(totLength);
            byte[] textByte1 = Encoding.UTF8.GetBytes(text1);
            Parallel.For(0, count, index=> {
                using (var stream = _adlsClient.CreateFile(path + index, IfExists.Overwrite, _arrayPool, 4*1024*1024))
                {
                    // Flush in empty buffer, no return called
                    stream.Flush();
                    // Rent called
                    stream.Write(textByte1, 0, textByte1.Length);
                    //Return called
                    stream.Flush();
                    stream.Flush();
                    // Rent called
                    stream.Write(textByte1, 0, textByte1.Length);
                    // Rent should not be called
                    stream.Write(textByte1, 0, textByte1.Length);
                }
            });
            Assert.IsTrue(_arrayPool.NumberOfRentCalled == count * 2);
            Assert.IsTrue(_arrayPool.NumberOfReturnCalled == count * 2);
            for (int index = 0; index < count; index++)
            {
                  string output = "";
                  using (var istream = _adlsClient.GetReadStream(path+index))
                  {
                      int noOfBytes;
                      byte[] buffer = new byte[2*1024 * 1024];
                      do
                      {
                          noOfBytes = istream.Read(buffer, 0, buffer.Length);
                          output += Encoding.UTF8.GetString(buffer, 0, noOfBytes);
                      } while (noOfBytes > 0);
                  }
                Assert.IsTrue(output.Equals(text1 + text1 + text1));
            };
            _arrayPool.Reset();
        }
    }
}
