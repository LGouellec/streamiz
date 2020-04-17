namespace Kafka.Streams.Net.SerDes
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
        /// <returns>Return <code>data</code> parameter</returns>
        public override byte[] Deserialize(byte[] data) => data;

        /// <summary>
        /// Serialize just return <code>data</code>.
        /// </summary>
        /// <param name="data">typed data</param>
        /// <returns>Return <code>data</code> parameter</returns>
        public override byte[] Serialize(byte[] data) => data;
    }
}