using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using Microsoft.Azure.DataLake.Store.Serialization;

namespace Microsoft.Azure.DataLake.Store
{
    /// <summary>
    /// Enum containing two types of trash entry
    /// 
    /// </summary>
    public enum TrashEntryType
    {
        DIRECTORY,
        FILE
    }

    /// <summary>
    /// Class 
    /// </summary>
    public class EnumerateDeletedItemsProgress
    {
        /// <summary>
        /// ContinuationToken for next API call to enumerate remaining trash entries
        /// </summary>
        [JsonProperty(PropertyName = "nextListAfter")]
        public string NextListAfter
        {
            get;
            internal set;
        }

        /// <summary>
        /// Number of entries searched
        /// </summary>
        [JsonProperty(PropertyName = "numSearched")]
        public long NumSearched
        {
            get;
            internal set;
        }

        /// <summary>
        /// Number of entries found
        /// </summary>
        public int NumFound
        {
            get;
            internal set;
        }
    }

    /// <summary>
    /// Class 
    /// </summary>
    public class TrashStatus : EnumerateDeletedItemsProgress
    {
        /// <summary>
        /// Trash status response to enumeratedeleteditems
        /// </summary>
        [JsonProperty(PropertyName = "trashDirEntry")]
        public IEnumerable<TrashEntry> TrashEntries
        {
            get;
            internal set;
        }

        /// <summary>
        /// Default constructor
        /// </summary>
        internal TrashStatus() { }

        internal TrashStatus(TrashStatus status)
        {
            TrashEntries = status.TrashEntries;
        }
    }


    /// <summary>
    /// Class that encapsulates the metadata of the trash entry
    /// </summary>
    public class TrashEntry
    {
        /// <summary>
        /// Original path of the entry
        /// </summary>
        [JsonProperty(PropertyName = "originalPath")]
        public String OriginalPath { get; internal set; }

        /// <summary>
        /// Full Path of the trash entry
        /// </summary>
        [JsonProperty(PropertyName = "trashDirPath")]
        public String TrashDirPath { get; internal set; }

        /// <summary>
        /// Type of entry - file or folder
        /// </summary>
        [JsonProperty(PropertyName = "type")]
        [JsonConverter(typeof(TrashTypeEnumConverter))]
        public TrashEntryType Type { get; internal set; }
        /// <summary>
        /// Group owner of the file or directory
        /// </summary>
        [JsonProperty(PropertyName = "creationTime")]
        [JsonConverter(typeof(ServerDateTimeConverter))]
        public DateTime? CreationTime { get; internal set; }
        /// <summary>
        /// User owner of the file or directory 
        /// </summary>
        
        /// <summary>
        /// Default constructor
        /// </summary>
        public TrashEntry() { }

        internal TrashEntry(TrashEntry entry)
        {
            OriginalPath = new string(entry.OriginalPath.ToCharArray());
            TrashDirPath = new string(entry.TrashDirPath.ToCharArray());
            Type = entry.Type;
            CreationTime = entry.CreationTime;
        }
        /// <summary>
        /// Constructor that initializes each property
        /// </summary>
        /// <param name="originalPath">Original Path of the entry</param>
        /// <param name="trashDirPath">Trash path of the entry</param>
        /// <param name="type">Type of entry</param>
        /// <param name="creationTime">Creation time obtained as milliseconds from 1/1/1970</param>
        internal TrashEntry(String originalPath, String trashDirPath, string type, long creationTime)
        {
            OriginalPath = originalPath;
            TrashDirPath = trashDirPath;
            CreationTime = creationTime < 0 ? null : (DateTime?)GetDateTimeFromServerTime(creationTime);
            Type = (TrashEntryType)Enum.Parse(typeof(TrashEntryType), type);
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
