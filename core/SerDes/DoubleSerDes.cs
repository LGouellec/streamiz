using Confluent.Kafka;
using System;

namespace Streamiz.Kafka.Net.SerDes
{
    public class DoubleSerDes : AbstractSerDes<double>
    {
        public override double Deserialize(byte[] data, SerializationContext context)
            => BitConverter.ToDouble(data);

        public override byte[] Serialize(double data, SerializationContext context)
            => BitConverter.GetBytes(data);
    }
}
