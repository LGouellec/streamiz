using System;

namespace Streamiz.Kafka.Net.SerDes
{
    public class DoubleSerDes : AbstractSerDes<double>
    {
        public override double Deserialize(byte[] data)
            => BitConverter.ToDouble(data);

        public override byte[] Serialize(double data)
            => BitConverter.GetBytes(data);
    }
}
