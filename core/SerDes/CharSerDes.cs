using System;

namespace Streamiz.Kafka.Net.SerDes
{
    /// <summary>
    /// SerDes for <see cref="char"/>.
    /// </summary>
    public class CharSerDes : AbstractSerDes<char>
    {
        /// <summary>
        /// Deserialize a record value from a byte array into char value
        /// </summary>
        /// <param name="data">serialized bytes.</param>
        /// <returns>deserialized <see cref="char"/> using data; may be null</returns>
        public override char Deserialize(byte[] data) => BitConverter.ToChar(data);

        /// <summary>
        /// Convert <see cref="char"/> <code>data</code> into a byte array.
        /// </summary>
        /// <param name="data"><see cref="char"/> data</param>
        /// <returns>serialized bytes</returns>
        public override byte[] Serialize(char data) => BitConverter.GetBytes(data);
    }
}
