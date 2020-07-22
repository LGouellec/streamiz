using Confluent.Kafka;
using System;

namespace Streamiz.Kafka.Net.SerDes
{
    public class FloatSerDes : AbstractSerDes<float>
    {
        public override float Deserialize(byte[] data, SerializationContext context)
            => BitConverter.ToSingle(data);

        public override byte[] Serialize(float data, SerializationContext context)
            => BitConverter.GetBytes(data);
    }
}
