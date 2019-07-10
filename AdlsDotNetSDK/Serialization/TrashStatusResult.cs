using Newtonsoft.Json;

namespace Microsoft.Azure.DataLake.Store.Serialization
{
    internal class TrashStatusResult
    {
        [JsonProperty(PropertyName = "trashDir")]
        internal TrashStatus TrashStatusRes{ get; private set; }
    }
}
