using System;
using System.Text;

namespace Microsoft.Azure.DataLake.Store.Acl
{
    /// <summary>
    /// Available Access control permissions
    /// </summary>
    [Flags]
    public enum AclAction
    {
        /// <summary>
        /// No permissions
        /// </summary>
        None = 0,
        /// <summary>
        /// Execute only permissions
        /// </summary>
        ExecuteOnly = 1,
        /// <summary>
        /// Write only permissions
        /// </summary>
        WriteOnly = 2,
        /// <summary>
        /// Read only permissions
        /// </summary>
        ReadOnly = 4,
        /// <summary>
        /// Read and write permission
        /// </summary>
        ReadWrite = ReadOnly | WriteOnly,
        /// <summary>
        /// Read and execute permission
        /// </summary>
        ReadExecute = ReadOnly | ExecuteOnly,
        /// <summary>
        /// Write and execute permission
        /// </summary>
        WriteExecute = WriteOnly | ExecuteOnly,
        /// <summary>
        /// All read, write and execute permission
        /// </summary>
        All = ReadOnly | WriteOnly | ExecuteOnly
    }
    /// <summary>
    /// Extension class
    /// </summary>
    public static class AclActionExtension
    {
        /// <summary>
        /// Returns the AclAction corresponding to the octal permission. For "rwx" returns AclAction.All
        /// </summary>
        /// <param name="rwx">Octal permission</param>
        /// <returns>Acl Action</returns>
        public static AclAction? GetAclAction(string rwx)
        {
            rwx = rwx.ToLower();
            if (rwx.Length != 3)
            {
                return null;
            }
            int val = 0;
            if (rwx[0] == 'r')
            {
                val += 4;
            }
            else if (rwx[0] != '-')
            {
                return null;
            }
            if (rwx[1] == 'w')
            {
                val += 2;
            }
            else if (rwx[1] != '-')
            {
                return null;
            }
            if (rwx[2] == 'x')
            {
                val += 1;
            }
            else if (rwx[2] != '-')
            {
                return null;
            }
            return (AclAction)val;
        }
        /// <summary>
        /// Extension method that returns the octal permission corresponding to the AclACtion. For ex: AclAction.ReadExecute => "r-x"
        /// </summary>
        /// <param name="act">Acl aCtion</param>
        /// <returns>Octal permission string</returns>
        public static string GetRwx(this AclAction act)
        {
            StringBuilder sb=new StringBuilder(3);
            sb.Append((act & AclAction.ReadOnly) > 0?'r':'-');
            sb.Append((act & AclAction.WriteOnly) > 0 ? 'w' : '-');
            sb.Append((act & AclAction.ExecuteOnly) > 0 ? 'x' : '-');
            return sb.ToString();
        }
    }

}
