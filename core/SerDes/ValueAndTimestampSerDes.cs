using Confluent.Kafka;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.State;
using System.IO;

namespace Streamiz.Kafka.Net.SerDes
{
    internal class ValueAndTimestampSerDes
    {
        public static (long, byte[]) Extract(byte[] data)
        {
            using (var stream = new MemoryStream(data))
            using (var reader = new BinaryReader(stream))
            {
                long t = reader.ReadInt64();
                int length = reader.ReadInt32();
                byte[] d = reader.ReadBytes(length);
                return (t, d);
            }
        }
    }
    
    /// <summary>
    /// SerDes for <see cref="ValueAndTimestamp{V}"/>.
    /// </summary>
    /// <typeparam name="V">inner value type</typeparam>
    public class ValueAndTimestampSerDes<V> : AbstractSerDes<ValueAndTimestamp<V>>
    {
        /// <summary>
        /// Inner serdes.
        /// </summary>
        public ISerDes<V> InnerSerdes { get; }

        /// <summary>
        /// Constructor with inner serdes
        /// </summary>
        /// <param name="innerSerdes">Inner serdes</param>
        /// <exception cref="StreamsException">exceptiont throws if inner serdes is null</exception>
        public ValueAndTimestampSerDes(ISerDes<V> innerSerdes)
        {
            InnerSerdes = innerSerdes ?? throw new StreamsException($"The inner serdes is not compatible to the actual value type {typeof(V).FullName}. Provide correct Serdes via method parameters(using the DSL)");
        }

        /// <summary>
        /// Deserialize byte array into a <see cref="ValueAndTimestamp{V}" /> instance.
        /// </summary>
        /// <param name="data">data serialized</param>
        /// <param name="context">serialization context</param>
        /// <returns>return a value and timestamp record</returns>
        public override ValueAndTimestamp<V> Deserialize(byte[] data, SerializationContext context)
        {
            if (data != null)
            {
                var meta = ValueAndTimestampSerDes.Extract(data);
                V v = InnerSerdes.Deserialize(meta.Item2, context);
                ValueAndTimestamp<V> obj = ValueAndTimestamp<V>.Make(v, meta.Item1);
                return obj;
            }
            else
                return null;
        }

        /// <summary>
        /// Serialize a <see cref="ValueAndTimestamp{V}" /> instance into a byte array.
        /// </summary>
        /// <param name="data">data to serialize</param>
        /// <param name="context">serialization context</param>
        /// <returns>serialized bytes</returns>
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