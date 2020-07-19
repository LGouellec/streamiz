using System;

namespace Streamiz.Kafka.Net.SerDes
{
    public class FloatSerDes : AbstractSerDes<float>
    {
        public override float Deserialize(byte[] data)
            => BitConverter.ToSingle(data);

        public override byte[] Serialize(float data)
            => BitConverter.GetBytes(data);
    }
}
