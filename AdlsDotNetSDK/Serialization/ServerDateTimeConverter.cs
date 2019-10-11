using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using NLog;
using System;

namespace Microsoft.Azure.DataLake.Store.Serialization
{
    internal class ServerDateTimeConverter : DateTimeConverterBase
    {
        private static readonly Logger DateTimeConverterLogger = LogManager.GetLogger("adls.dotnet.ServerDateTimeConverter");
        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            if (reader.TokenType != JsonToken.Integer)
            {
                throw new Exception(
                    String.Format("Unexpected token parsing date. Expected Integer, got {0}.",
                    reader.TokenType));
            }

            var ticks = (long)reader.Value;

            if(ticks < 0)
            {
                return null;
            }
            try
            {
                return new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc).Add(new TimeSpan(ticks * 10000));
            }
            catch(ArgumentOutOfRangeException ex)
            {
                if (DateTimeConverterLogger.IsDebugEnabled)
                {
                    DateTimeConverterLogger.Debug($"Exception: {ex.Message} Ticks: {ticks}");
                }
                return new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            }

        }

        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            throw new NotImplementedException();
        }
    }
}
