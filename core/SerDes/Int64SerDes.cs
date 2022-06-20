using Confluent.Kafka;
using System;

namespace Streamiz.Kafka.Net.SerDes
{
    /// <summary>
    /// SerDes for <see cref="Int64"/> or <see cref="long"/>
    /// </summary>
    public class Int64SerDes : AbstractSerDes<long>
    {
        /// <summary>
        /// Deserialize a record value from a byte array into <see cref="Int64"/> value
        /// </summary>
        /// <param name="data">serialized bytes.</param>
        /// <param name="context">serialization context</param>
        /// <returns>deserialized <see cref="Int64"/> using data; may be null</returns>
        public override long Deserialize(byte[] data, SerializationContext context) => BitConverter.ToInt64(data, 0);

        /// <summary>
        /// Convert long <code>data</code> into a byte array.
        /// </summary>
        /// <param name="data"><see cref="Int64"/> data</param>
        /// <param name="context">serialization context</param>
        /// <returns>serialized bytes</returns>
        public override byte[] Serialize(long data, SerializationContext context) => BitConverter.GetBytes(data);
    }
}
