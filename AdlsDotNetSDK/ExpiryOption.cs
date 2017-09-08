
namespace Microsoft.Azure.DataLake.Store
{
    /// <summary>
    /// Different types of expiry options
    /// </summary>
    public enum ExpiryOption
    {
        /// <summary>
        /// File will never expire. ExpiryTime is ignored.
        /// </summary>
        NeverExpire,
        /// <summary>
        /// ExpiryTime is added to the current time
        /// </summary>
        RelativeToNow,
        /// <summary>
        /// ExpiryTime is added to the creation time
        /// </summary>
        RelativeToCreationDate,
        /// <summary>
        /// ExpiryTime is the actual time
        /// </summary>
        Absolute
    }
}