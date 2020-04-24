using Streamiz.Kafka.Net.State;
using System.IO;

namespace Streamiz.Kafka.Net.SerDes
{
    public class ValueAndTimestampSerDes<V> : AbstractSerDes<ValueAndTimestamp<V>>
    {
        public ISerDes<V> InnerSerdes { get; internal set; }

        public ValueAndTimestampSerDes(ISerDes<V> innerSerdes)
        {
            this.InnerSerdes = innerSerdes;
        }

        public override ValueAndTimestamp<V> Deserialize(byte[] data)
        {
            using (var stream = new MemoryStream(data))
            using (var reader = new BinaryReader(stream))
            {
                long t = reader.ReadInt64();
                int length = reader.ReadInt32();
                byte[] d = reader.ReadBytes(length);
                V v = InnerSerdes.Deserialize(d);
                ValueAndTimestamp<V> obj = ValueAndTimestamp<V>.Make(v, t);
                return obj;
            }
        }

        public override byte[] Serialize(ValueAndTimestamp<V> data)
        {
            using(var stream = new MemoryStream())
            using (var writer = new BinaryWriter(stream))
            {
                byte[] innerobj = InnerSerdes.Serialize(data.Value);

                writer.Write(data.Timestamp);
                writer.Write(innerobj.Length);
                writer.Write(innerobj);
                writer.Flush();
                return stream.ToArray();
            }
        }
    }
}
