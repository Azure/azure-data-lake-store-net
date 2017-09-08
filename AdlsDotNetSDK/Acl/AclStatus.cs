using System.Collections.Generic;

namespace Microsoft.Azure.DataLake.Store.Acl
{
    /// <summary>
    /// Class that encapsulates Acl entries, owner ID, group ID, octal permission and sticky bit (only for a directory)
    /// </summary>
    public class AclStatus
    {
        /// <summary>
        /// ACL entries of the file
        /// </summary>
        public List<AclEntry> Entries { get; }
        /// <summary>
        /// Owner of the file
        /// </summary>
        public string Owner { get; }
        /// <summary>
        /// Group ID that owns the file
        /// </summary>
        public string Group { get; }
        /// <summary>
        /// Octal permission
        /// </summary>
        public string Permission { get; }
        /// <summary>
        /// Sticky Bit
        /// </summary>
        public bool StickyBit { get; }
        /// <summary>
        /// Initializes Acl Status
        /// </summary>
        /// <param name="list">Acl Entry list</param>
        /// <param name="owner">Owner</param>
        /// <param name="group">Group Id</param>
        /// <param name="permission">Permission string</param>
        /// <param name="stickyBit">Sticky Bit</param>
        public AclStatus(List<AclEntry> list, string owner, string group, string permission, bool stickyBit)
        {
            Entries = list;
            Owner = owner;
            Group = group;
            Permission = permission;
            StickyBit = stickyBit;
        }
    }
}
