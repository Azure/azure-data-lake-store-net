using System;

namespace Microsoft.Azure.DataLake.Store
{
    /// <summary>
    /// Enum containing two types of directory entry
    /// </summary>
    public enum DirectoryEntryType
    {
        DIRECTORY,
        FILE
    }
    /// <summary>
    /// Class that encapsulates the metadata of the directory entry
    /// </summary>
    public class DirectoryEntry
    {
        /// <summary>
        /// Name of the entry
        /// </summary>
        public String Name { get; }

        /// <summary>
        /// Full Path of the directory entry
        /// </summary>
        public String FullName { get; }

        /// <summary>
        /// Size of the file. Zero for directory
        /// </summary>
        public long Length { get; }
        /// <summary>
        /// Group owner of the file or directory
        /// </summary>
        public String Group { get; }
        /// <summary>
        /// User owner of the file or directory 
        /// </summary>
        public String User { get; }
        /// <summary>
        /// Instant when the file was last accessed
        /// </summary>
        public DateTime? LastAccessTime { get; }
        /// <summary>
        /// Instant when the file was last modified
        /// </summary>
        public DateTime? LastModifiedTime { get; }
        /// <summary>
        /// Type- File or directory
        /// </summary>
        public DirectoryEntryType Type { get; }
        /// <summary>
        /// Boolean indicating whether ACLs are set
        /// </summary>
        public bool HasAcl { get; }
        /// <summary>
        /// Instant when the file will expire
        /// </summary>
        public DateTime? ExpiryTime { get; }
        /// <summary>
        /// Unix style permission string
        /// </summary>
        public String Permission { get; }
        /// <summary>
        /// Default constructor
        /// </summary>
        public DirectoryEntry() { Name = null; }
        /// <summary>
        /// Initializes the name and full name
        /// </summary>
        /// <param name="fullName"></param>
        internal DirectoryEntry(string fullName)
        {
            Name = fullName;
            FullName = fullName;
        }
        /// <summary>
        /// Constructor that initializes each property
        /// </summary>
        /// <param name="name">Name</param>
        /// <param name="fullName">Full path</param>
        /// <param name="length">Size of file</param>
        /// <param name="group">Group owner</param>
        /// <param name="user">User owner</param>
        /// <param name="lastAccessTime">Last access time</param>
        /// <param name="lastModifiedTime">Last modified time</param>
        /// <param name="type">File or directory</param>
        /// <param name="permission">Unix style permission</param>
        /// <param name="hasAcl">Whether ACLs are set</param>
        /// <param name="expiryTime">Time when file would expire</param>
        internal DirectoryEntry(String name, String fullName, long length, String group, String user, long lastAccessTime, long lastModifiedTime, string type, String permission, bool hasAcl, long expiryTime)
        {
            Name = name;
            FullName = fullName;
            Length = length;
            Group = group;
            User = user;
            LastAccessTime = lastAccessTime < 0 ? null : (DateTime?)GetDateTimeFromServerTime(lastAccessTime);
            LastModifiedTime = lastModifiedTime < 0 ? null : (DateTime?)GetDateTimeFromServerTime(lastModifiedTime);
            Type = (DirectoryEntryType)Enum.Parse(typeof(DirectoryEntryType), type);
            Permission = permission;
            HasAcl = hasAcl;
            ExpiryTime = expiryTime <= 0 ? null : (DateTime?)GetDateTimeFromServerTime(expiryTime);
        }
        /// <summary>
        /// Returns a DateTime instance from server time obtained as milliseconds from 1/1/1970.
        /// </summary>
        /// <param name="time">server time obtained as milliseconds from 1/1/1970</param>
        /// <returns>DateTime instance</returns>
        internal static DateTime GetDateTimeFromServerTime(long time)
        {
            return new DateTime(1970, 1, 1,0,0,0,DateTimeKind.Utc).Add(new TimeSpan(time * 10000));
        }
    }

}
