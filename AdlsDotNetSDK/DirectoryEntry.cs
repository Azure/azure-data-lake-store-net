using Microsoft.Azure.DataLake.Store.Serialization;
using Newtonsoft.Json;
using System;

namespace Microsoft.Azure.DataLake.Store
{
    /// <summary>
    /// Enum containing two types of directory entry
    /// </summary>
    [JsonConverter(typeof(FileTypeEnumConverter))]
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
        [JsonProperty(PropertyName = "pathSuffix")]
        public string Name { get; internal set; }

        /// <summary>
        /// Full Path of the directory entry
        /// </summary>
        public string FullName { get; internal set; }

        /// <summary>
        /// Size of the file. Zero for directory
        /// </summary>
        [JsonProperty(PropertyName = "length")]
        public long Length { get; internal set; }
        /// <summary>
        /// Group owner of the file or directory
        /// </summary>
        [JsonProperty(PropertyName = "group")]
        public String Group { get; internal set; }
        /// <summary>
        /// User owner of the file or directory 
        /// </summary>
        [JsonProperty(PropertyName = "owner")]
        public string User { get; internal set; }
        /// <summary>
        /// Instant when the file was last accessed
        /// </summary>
        [JsonProperty(PropertyName = "accessTime")]
        [JsonConverter(typeof(ServerDateTimeConverter))]
        public DateTime? LastAccessTime { get; internal set; }
        /// <summary>
        /// Instant when the file was last modified
        /// </summary>
        [JsonProperty(PropertyName = "modificationTime")]
        [JsonConverter(typeof(ServerDateTimeConverter))]
        public DateTime? LastModifiedTime { get; internal set; }
        /// <summary>
        /// Type- File or directory
        /// </summary>
        [JsonProperty(PropertyName = "type")]
        public DirectoryEntryType Type { get; internal set; }
        /// <summary>
        /// Boolean indicating whether ACLs are set
        /// </summary>
        [JsonProperty(PropertyName = "aclBit")]
        public bool HasAcl { get; internal set; }
        /// <summary>
        /// Instant when the file will expire
        /// </summary>
        [JsonProperty(PropertyName = "msExpirationTime")]
        [JsonConverter(typeof(ExpirationDateTimeConverter))]
        public DateTime? ExpiryTime { get; internal set; }
        /// <summary>
        /// Unix style permission string
        /// </summary>
        [JsonProperty(PropertyName = "permission")]
        public string Permission { get; internal set; }
        /// <summary>
        /// Default constructor
        /// </summary>
        public DirectoryEntry() { }

        /// <summary>
        /// Initializes the name and full name
        /// </summary>
        /// <param name="fullName"></param>
        internal DirectoryEntry(string fullName)
        {
            FullName = Name = fullName;
        }

        internal DirectoryEntry(DirectoryEntry dir)
        {
            Name = dir.Name;
            FullName = dir.FullName;
            Length = dir.Length;
            Group = dir.Group;
            User = dir.User;
            LastAccessTime = dir.LastAccessTime;
            LastModifiedTime = dir.LastModifiedTime;
            Type = dir.Type;
            Permission = dir.Permission;
            HasAcl = HasAcl;
            ExpiryTime = dir.ExpiryTime;
        }
        /// <summary>
        /// Constructor that initializes each property
        /// </summary>
        /// <param name="name">Name</param>
        /// <param name="fullName">Full path</param>
        /// <param name="length">Size of file</param>
        /// <param name="group">Group owner</param>
        /// <param name="user">User owner</param>
        /// <param name="lastAccessTime">Last access time obtained as milliseconds from 1/1/1970</param>
        /// <param name="lastModifiedTime">Last modified time obtained as milliseconds from 1/1/1970</param>
        /// <param name="type">File or directory</param>
        /// <param name="permission">Unix style permission</param>
        /// <param name="hasAcl">Whether ACLs are set</param>
        /// <param name="expiryTime">Time when file would expire obtained as milliseconds from 1/1/1970</param>
        public DirectoryEntry(String name, String fullName, long length, String group, String user, long lastAccessTime, long lastModifiedTime, string type, String permission, bool hasAcl, long expiryTime)
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
