using Confluent.Kafka;
using Streamiz.Kafka.Net.SerDes;
using System;
using System.Text;

namespace Streamiz.Kafka.Net.Tests.Helpers
{
    internal class SerdesThrowException : AbstractSerDes<string>
    {
        public override string Deserialize(byte[] data, SerializationContext context)
        {
            if (data.Length % 2 == 0)
                throw new NotImplementedException();
            else
                return Encoding.UTF8.GetString(data);
        }

        public override byte[] Serialize(string data, SerializationContext context)
        {
            return Encoding.UTF8.GetBytes(data);
        }
    }
}
