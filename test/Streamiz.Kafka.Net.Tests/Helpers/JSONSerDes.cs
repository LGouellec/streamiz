using Confluent.Kafka;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Kafka.Net.Tests.Helpers
{
    public class JSONSerDes<T> : AbstractSerDes<T>
    {
        private readonly StringSerDes innerSerdes = new StringSerDes();

        public override T Deserialize(byte[] data, SerializationContext context)
        {
            var s = innerSerdes.Deserialize(data, context);
            return Newtonsoft.Json.JsonConvert.DeserializeObject<T>(s);
        }

        public override byte[] Serialize(T data, SerializationContext context)
        {
            var s = Newtonsoft.Json.JsonConvert.SerializeObject(data);
            return innerSerdes.Serialize(s, context);
        }
    }
}
