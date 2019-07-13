using System.Threading.Tasks;

namespace Microsoft.Azure.DataLake.Store
{
    /// <summary>
    /// Abstract class built on top of ArrayPool that exposes async methods of Rent Return
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public abstract class AdlsArrayPool<T> : System.Buffers.ArrayPool<T>
    {
        /// <summary>
        /// Synchronous method of renting buffer.
        /// </summary>
        /// <param name="minimumLength">The minimum length of the array.</param>
        /// <returns>An array of type T[] that is at least minimumLength in length.</returns>
        public override T[] Rent(int minimumLength)
        {
            return RentAsync(minimumLength).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Asynchronous method of renting buffer
        /// </summary>
        /// <param name="minimumLength">The minimum length of the array.</param>
        /// <returns>An array of type T[] that is at least minimumLength in length.</returns>
        public abstract Task<T[]> RentAsync(int minimumLength);

        /// <summary>
        /// Synchronous method to return an array to the pool that was previously obtained using the <see cref="Rent(int)"/> method on the same <see cref="AdlsArrayPool{T}"/> instance
        /// </summary>
        /// <param name="array">A buffer to return to the pool that was previously obtained using the <see cref="Rent(int)"/> method.</param>
        /// <param name="clearArray">Indicates whether the contents of the buffer should be cleared before reuse. If clearArray is set to true, and if the pool will store the buffer to enable subsequent reuse, the <see cref="Return(T[], bool)"/> method will clear the array of its contents so that a subsequent caller using the <see cref="Rent(int)"/> method will not see the content of the previous caller. If clearArray is set to false or if the pool will release the buffer, the array's contents are left unchanged.</param>
        public override void Return(T[] array, bool clearArray = false)
        {
            ReturnAsync(array, clearArray).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Asynchronous method to return an array to the pool that was previously obtained using the <see cref="RentAsync(int)"/> method on the same <see cref="AdlsArrayPool{T}"/> instance
        /// </summary>
        /// <param name="array">A buffer to return to the pool that was previously obtained using the <see cref="Rent(int)"/> method.</param>
        /// <param name="clearArray">Indicates whether the contents of the buffer should be cleared before reuse. If clearArray is set to true, and if the pool will store the buffer to enable subsequent reuse, the <see cref="Return(T[], bool)"/> method will clear the array of its contents so that a subsequent caller using the <see cref="Rent(int)"/> method will not see the content of the previous caller. If clearArray is set to false or if the pool will release the buffer, the array's contents are left unchanged.</param>
        public abstract Task ReturnAsync(T[] array, bool clearArray = false);
    }
}
