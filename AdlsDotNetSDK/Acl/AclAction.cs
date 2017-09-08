using System;
using System.Text;

namespace Microsoft.Azure.DataLake.Store.Acl
{
    [Flags]
    public enum AclAction
    {
        None = 0,
        ExecuteOnly = 1,
        WriteOnly = 2,
        ReadOnly = 4,
        ReadWrite = ReadOnly | WriteOnly,
        ReadExecute = ReadOnly | ExecuteOnly,
        WriteExecute = WriteOnly | ExecuteOnly,
        All = ReadOnly | WriteOnly | ExecuteOnly
    }

    public static class AclActionExtension
    {
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
