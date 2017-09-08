using System;
using System.Collections.Generic;
using System.Text;

namespace Microsoft.Azure.DataLake.Store.Acl
{
    public class AclEntry
    {
        /// <summary>
        /// Type of ACL entry: User/group/Other/mask
        /// </summary>
        public AclType Type { get; }
        /// <summary>
        /// Object ID of the object depending on the type of the ACL entry
        /// </summary>
        public string TypeName { get; }
        /// <summary>
        /// ACCESS or DEFAULT
        /// </summary>
        public AclScope Scope { get; }
        /// <summary>
        /// The type of ACL to set
        /// </summary>
        public AclAction Action { get; }
        /// <summary>
        /// Public constructor
        /// </summary>
        /// <param name="type">Type of ACL entry: User/group/Other/mask</param>
        /// <param name="typeName">Object ID of the object depending on the type of the ACL entry</param>
        /// <param name="scope">ACCESS or DEFAULT</param>
        /// <param name="action">The type of ACL to set</param>
        public AclEntry(AclType type, string typeName, AclScope scope, AclAction action)
        {
            Type = type;
            TypeName = typeName;
            Scope = scope;
            Action = action;
        }
        /// <summary>
        /// Parses a AclEntry string into acl type, acl type Id, acl scope and acl action (permissions).
        /// Throws exception if the acl string is not correct.
        /// </summary>
        /// <param name="aclEntry">Acl Entry string delimited by ':'</param>
        /// <param name="removeAcl">Whether this string is for removing Acl.</param>
        /// <returns>Acl Entry instance</returns>
        public static AclEntry ParseAclEntryString(string aclEntry, bool removeAcl)
        {
            aclEntry = aclEntry.Trim();
            string[] parts = aclEntry.Split(':');
            if (parts.Length > 4)
            {
                throw new ArgumentException("Invalid AclEntry string: " + aclEntry);
            }
            if (parts.Length == 4 && !parts[0].Equals("default"))
            {
                throw new ArgumentException("Invalid AclEntry string: " + aclEntry);
            }
            int strtPartIndx = 0;
            AclScope scope;
            if (parts.Length == 4) //Because it is of AclScope default
            {
                strtPartIndx++;
                scope = AclScope.DEFAULT;
            }
            else
            {
                scope = AclScope.ACCESS;
            }
            AclType aclType = (AclType)Enum.Parse(typeof(AclType), parts[strtPartIndx].Trim());//This will throw exception
            string aclNm = parts[strtPartIndx + 1].Trim();
            if (aclType == AclType.mask && !String.IsNullOrEmpty(aclNm))
            {
                throw new ArgumentException("AclType Mask should not contain userId or group Id");
            }
            if (aclType == AclType.other && !String.IsNullOrEmpty(aclNm))
            {
                throw new ArgumentException("AclType Other should not contain userId or group Id");
            }
            AclAction action = AclAction.None;
            if (!removeAcl)
            {
                AclAction? ac = AclActionExtension.GetAclAction(parts[strtPartIndx + 2].Trim());
                if (ac == null)
                {
                    throw new ArgumentException("Invalid permission in aclentry " + aclEntry);
                }
                action = ac.Value;
            }
            return new AclEntry(aclType, aclNm, scope, action);
        }
        /// <summary>
        /// Parses each acl entry string and then returns the list of all acl entries
        /// </summary>
        /// <param name="aclEntries">String containing the acl entries each entry is delimited by ','</param>
        /// <returns>List of acl entries</returns>
        public static List<AclEntry> ParseAclEntriesString(string aclEntries)
        {
            aclEntries = aclEntries.Trim();
            string[] parts = aclEntries.Split(',');
            List<AclEntry> aclEntriesList = new List<AclEntry>(parts.Length);
            foreach (string part in parts)
            {
                aclEntriesList.Add(ParseAclEntryString(part, false));
            }
            return aclEntriesList;
        }
        /// <summary>
        /// Serializes the ACL entries from a list of ACL entries to a string format
        /// </summary>
        /// <param name="aclList">List of ACL entries</param>
        /// <param name="removeAcl">True if is called while removing ACLs</param>
        /// <returns>List of Acl entries concatenated in a string format each entry is delimited by ','</returns>
        public static string SerializeAcl(List<AclEntry> aclList, bool removeAcl)
        {
            StringBuilder sb = new StringBuilder();
            foreach (var aclEntry in aclList)
            {
                sb.Append((sb.Length == 0 ? "" : ",") + aclEntry.ToString(removeAcl));
            }
            return sb.ToString();
        }

        public override string ToString()
        {
            return ToString(false);
        }

        /// <summary>
        /// Returns the string format of each ACL entry
        /// </summary>
        /// <param name="removeAcl">True if is called while removing ACLs</param>
        /// <returns>Acl entry in a string format where each part is delimited by ':'</returns>
        public string ToString(bool removeAcl)
        {
            return (Scope == AclScope.DEFAULT ? "default:" : "") + Enum.GetName(typeof(AclType), Type) + ":" + TypeName + (removeAcl ? "" : ":" + Action.GetRwx());
        }
        /// <summary>
        /// Returns true if the type, type name, scope and action are all same
        /// </summary>
        /// <param name="entry">Acl entry</param>
        /// <returns>True if Acl entries are equal else false</returns>
        public bool Equals(AclEntry entry)
        {
            return Type.Equals(entry.Type) && TypeName.Equals(entry.TypeName) &&
                   Scope.Equals(entry.Scope) && Action.Equals(entry.Action);
        }
        /// <summary>
        /// Returns true if the type, type name, scope and action are all same
        /// </summary>
        /// <param name="obj">Acl entry</param>
        /// <returns>true if AclEntries are same else false</returns>
        public override bool Equals(object obj)
        {
            if (obj == null)
            {
                return false;
            }
            AclEntry entry = obj as AclEntry;
            if (entry == null)
            {
                return false;
            }
            return Equals(entry);
        }
    }
}
