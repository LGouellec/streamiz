using Confluent.Kafka;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.State;
using System.IO;

namespace Streamiz.Kafka.Net.SerDes
{
    internal class ValueAndTimestampSerDes<V> : AbstractSerDes<ValueAndTimestamp<V>>
    {
        public ISerDes<V> InnerSerdes { get; internal set; }

        public ValueAndTimestampSerDes(ISerDes<V> innerSerdes)
        {
            InnerSerdes = innerSerdes ?? throw new StreamsException($"The inner serdes is not compatible to the actual value type {typeof(V).FullName}. Provide correct Serdes via method parameters(using the DSL)");
        }

        public override ValueAndTimestamp<V> Deserialize(byte[] data, SerializationContext context)
        {
            if (data != null)
            {
                using (var stream = new MemoryStream(data))
                using (var reader = new BinaryReader(stream))
                {
                    long t = reader.ReadInt64();
                    int length = reader.ReadInt32();
                    byte[] d = reader.ReadBytes(length);
                    V v = InnerSerdes.Deserialize(d, context);
                    ValueAndTimestamp<V> obj = ValueAndTimestamp<V>.Make(v, t);
                    return obj;
                }
            }
            else
                return null;
        }

        public override byte[] Serialize(ValueAndTimestamp<V> data, SerializationContext context)
        {
            if (data != null)
            {
                using (var stream = new MemoryStream())
                using (var writer = new BinaryWriter(stream))
                {
                    byte[] innerobj = InnerSerdes.Serialize(data.Value, context);

                    writer.Write(data.Timestamp);
                    writer.Write(innerobj.Length);
                    writer.Write(innerobj);
                    writer.Flush();
                    return stream.ToArray();
                }
            }
            else
                return null;
        }
    }
}
