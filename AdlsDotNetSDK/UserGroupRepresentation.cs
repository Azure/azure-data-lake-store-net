namespace Microsoft.Azure.DataLake.Store
{
    /// <summary>
    /// Enumerater defining how the user or group objects will be represented in calls that return group or user
    /// </summary>
    public enum UserGroupRepresentation
    {
        /// <summary>
        /// Object ID which is a immutable GUID representing the user or group.
        /// </summary>
        OID,
        /// <summary>
        /// User Principal name of the user or group. This can be changed. This involves an additional lookup.
        /// </summary>
        UPN
    }
}
