using Confluent.Kafka;

namespace Streamiz.Kafka.Net.SerDes
{
    /// <summary>
    /// Basic ByteArraySerdes. 
    /// This is the default serdes present in <see cref="IStreamConfig"/> if you doesn't set your default key and value serdes.
    /// </summary>
    public class ByteArraySerDes : AbstractSerDes<byte[]>
    {
        /// <summary>
        /// Deserialize just return <code>data</code>.
        /// </summary>
        /// <param name="data">serialized bytes</param>
        /// <param name="context">serialization context</param>
        /// <returns>Return <code>data</code> parameter</returns>
        public override byte[] Deserialize(byte[] data, SerializationContext context) => data;

        /// <summary>
        /// Serialize just return <code>data</code>.
        /// </summary>
        /// <param name="data">typed data</param>
        /// <param name="context">serialization context</param>
        /// <returns>Return <code>data</code> parameter</returns>
        public override byte[] Serialize(byte[] data, SerializationContext context) => data;
    }
}