using Confluent.Kafka;
using Streamiz.Kafka.Net.Crosscutting;

namespace Streamiz.Kafka.Net.SerDes.Internal
{
    internal class BytesSerDes : AbstractSerDes<Bytes>
    {
        public override Bytes Deserialize(byte[] data, SerializationContext context)
            => Bytes.Wrap(data);

        public override byte[] Serialize(Bytes data, SerializationContext context)
            => data?.Get;
    }
}
