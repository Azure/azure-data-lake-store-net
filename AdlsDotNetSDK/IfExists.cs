namespace Microsoft.Azure.DataLake.Store
{
    /// <summary>
    /// Enum specifying actions to take while creating new file if the file exists
    /// </summary>
    public enum IfExists
    {
        /// <summary>
        /// Overwrite the existing file
        /// </summary>
        Overwrite,
        /// <summary>
        /// Fails the request
        /// </summary>
        Fail
    }
}
