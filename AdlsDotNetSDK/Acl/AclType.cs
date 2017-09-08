namespace Microsoft.Azure.DataLake.Store.Acl
{
    /// <summary>
    /// Type of ACL entry
    /// </summary>
    public enum AclType
    {
        user,
        group,
        other,
        mask
    }
}
