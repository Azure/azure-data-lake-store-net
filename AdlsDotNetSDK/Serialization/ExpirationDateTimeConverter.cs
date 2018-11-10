using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using System;

namespace Microsoft.Azure.DataLake.Store.Serialization
{
    /// <summary>
    /// This is separate from ServerDateTimeConverter because for expiration time we need to return null if expiration is 0
    /// </summary>
    internal class ExpirationDateTimeConverter : DateTimeConverterBase
    {
        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            if (reader.TokenType != JsonToken.Integer)
            {
                throw new Exception(
                    String.Format("Unexpected token parsing date. Expected Integer, got {0}.",
                    reader.TokenType));
            }

            var ticks = (long)reader.Value;

            if (ticks <= 0)
            {
                return null;
            }
            else
            {
                return new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc).Add(new TimeSpan(ticks * 10000));
            }

        }
        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            throw new NotImplementedException();
        }
    }
}
