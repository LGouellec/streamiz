using System.Text;
using Confluent.Kafka;
using Newtonsoft.Json;

namespace Streamiz.Kafka.Net.SerDes
{
    public class JsonSerDes<T> : AbstractSerDes<T>
        where T : class
    {
        public override byte[] Serialize(T data, SerializationContext context)
            => data != null ? Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(data)) : null;

        public override T Deserialize(byte[] data, SerializationContext context)
            => data != null ? JsonConvert.DeserializeObject<T>(Encoding.UTF8.GetString(data)) : null;
    }
}