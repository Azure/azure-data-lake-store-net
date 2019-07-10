using Newtonsoft.Json;

namespace Microsoft.Azure.DataLake.Store.Serialization
{
    internal class DirectoryEntryResult<T> where T:class
    {
        [JsonProperty(PropertyName = "fileStatus")]
        internal T FileStatus { get; private set; }
    }
}
