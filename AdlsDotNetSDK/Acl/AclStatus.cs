using System;
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
        public List<AclEntry> Entries { get; internal set; }
        /// <summary>
        /// Owner of the file
        /// </summary>
        public string Owner { get; internal set; }
        /// <summary>
        /// Group ID that owns the file
        /// </summary>
        public string Group { get; internal set; }
        /// <summary>
        /// Octal permission
        /// </summary>
        public string Permission { get; internal set; }
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

        internal AclStatus(AclStatus status)
        {
            Entries=new List<AclEntry>(status.Entries);
            Owner = new string(status.Owner.ToCharArray());
            Group=new string(status.Group.ToCharArray());
            Permission = new string(status.Permission.ToCharArray());
            StickyBit = status.StickyBit;
        }
    }
}
