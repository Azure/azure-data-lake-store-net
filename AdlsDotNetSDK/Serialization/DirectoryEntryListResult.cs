using Newtonsoft.Json;

namespace Microsoft.Azure.DataLake.Store.Serialization
{
    internal class DirectoryEntryListResult<T> where T:class
    {
        [JsonProperty(PropertyName = "fileStatuses")]
        internal DirectoryEntryList<T> FileStatuses { get; private set; }
    }
}
