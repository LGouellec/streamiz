using Confluent.Kafka;
using System;

namespace Streamiz.Kafka.Net.SerDes
{
    /// <summary>
    /// SerDes for <see cref="float"/>
    /// </summary>
    public class FloatSerDes : AbstractSerDes<float>
    {
        /// <summary>
        /// Deserialize a record value from a byte array into <see cref="float"/> value
        /// </summary>
        /// <param name="data">serialized bytes.</param>
        /// <param name="context">serialization context</param>
        /// <returns>deserialized <see cref="float"/> using data; may be null</returns>
        public override float Deserialize(byte[] data, SerializationContext context)
            => BitConverter.ToSingle(data, 0);

        /// <summary>
        /// Convert double <code>data</code> into a byte array.
        /// </summary>
        /// <param name="data"><see cref="float"/> data</param>
        /// <param name="context">serialization context</param>
        /// <returns>serialized bytes</returns>
        public override byte[] Serialize(float data, SerializationContext context)
            => BitConverter.GetBytes(data);
    }
}
