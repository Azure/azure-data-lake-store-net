namespace Microsoft.Azure.DataLake.Store
{
    /// <summary>
    /// Flags that are passed as parameters as a part of Http request for operations Write and Append
    /// </summary>
    public enum SyncFlag
    {
        /// <summary>
        /// Holds the lease.
        /// </summary>
        DATA,
        /// <summary>
        /// Metadata needs to be updated after data is appended
        /// </summary>
        METADATA,
        /// <summary>
        /// update metadata.
        /// Close the file handler or stream
        /// Releases the lease.
        /// </summary>
        CLOSE
    }
}