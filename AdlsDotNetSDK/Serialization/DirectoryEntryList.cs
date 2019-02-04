using Newtonsoft.Json;
using System.Collections.Generic;

namespace Microsoft.Azure.DataLake.Store.Serialization
{
    internal class DirectoryEntryList<T> where T:class
    {
        [JsonProperty(PropertyName = "fileStatus")]
        internal List<T> FileStatus { get; private set; }
        [JsonProperty(PropertyName = "continuationToken")]
        public string ContinuationToken { get; internal set; }
    }
}
