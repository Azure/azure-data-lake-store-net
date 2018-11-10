using Newtonsoft.Json;
using System;
using System.IO;

namespace Microsoft.Azure.DataLake.Store.Serialization
{
    internal static class JsonCustomConvert
    {
        internal static T DeserializeObject<T>(Stream stream, JsonSerializerSettings settings)
        {
            if (stream == null)
            {
                throw new ArgumentNullException("Json stream is null");
            }

            // Use Create() instead of CreateDefault() here so that our own settings aren't merged with the defaults.  
            var serializer = JsonSerializer.Create(settings);
            serializer.CheckAdditionalContent = true;
            using (StreamReader stReader = new StreamReader(stream))
            {
                using (var jsonReader = new JsonTextReader(stReader))
                {

                    return (T)serializer.Deserialize(jsonReader, typeof(T));
                }
            }
        }
    }
}
