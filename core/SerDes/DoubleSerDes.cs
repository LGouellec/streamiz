using Confluent.Kafka;
using System;

namespace Streamiz.Kafka.Net.SerDes
{
    /// <summary>
    /// SerDes for <see cref="double"/>
    /// </summary>
    public class DoubleSerDes : AbstractSerDes<double>
    {
        /// <summary>
        /// Deserialize a record value from a byte array into <see cref="double"/> value
        /// </summary>
        /// <param name="data">serialized bytes.</param>
        /// <param name="context">serialization context</param>
        /// <returns>deserialized <see cref="double"/> using data; may be null</returns>
        public override double Deserialize(byte[] data, SerializationContext context)
            => BitConverter.ToDouble(data, 0);

        /// <summary>
        /// Convert double <code>data</code> into a byte array.
        /// </summary>
        /// <param name="data"><see cref="double"/> data</param>
        /// <param name="context">serialization context</param>
        /// <returns>serialized bytes</returns>
        public override byte[] Serialize(double data, SerializationContext context)
            => BitConverter.GetBytes(data);
    }
}
