using System.Text;

namespace Kafka.Streams.Net.SerDes
{
    public class StringSerDes : AbstractSerDes<string>
    {
        private readonly Encoding encoding;

        public StringSerDes()
            : this(Encoding.UTF8)
        {
        }

        public StringSerDes(Encoding encoding)
        {
            this.encoding = encoding;
        }

        public override string Deserialize(byte[] data)
        {
            return encoding.GetString(data);
        }

        public override byte[] Serialize(string data)
        {
            return encoding.GetBytes(data);
        }
    }
}
