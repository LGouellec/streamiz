using System.Text;

namespace Streamiz.Kafka.Net.SerDes
{
    /// <summary>
    /// SerDes for <see cref="string"/>. Default this serdes using <see cref="Encoding.UTF8"/>
    /// </summary>
    public class StringSerDes : AbstractSerDes<string>
    {
        private readonly Encoding encoding;

        /// <summary>
        /// Empty constructor
        /// </summary>
        public StringSerDes()
            : this(Encoding.UTF8)
        {
        }

        /// <summary>
        /// Constructor with specific encoding
        /// </summary>
        /// <param name="encoding">Encoding</param>
        public StringSerDes(Encoding encoding)
        {
            this.encoding = encoding;
        }

        /// <summary>
        /// Deserialize a record value from a byte array into string value
        /// </summary>
        /// <param name="data">serialized bytes.</param>
        /// <returns>deserialized string using encoding data; may be null</returns>
        public override string Deserialize(byte[] data)
        {
            return data != null ? encoding.GetString(data) : null;
        }

        /// <summary>
        /// Convert string <code>data</code> into a byte array using encoding.
        /// </summary>
        /// <param name="data">string data</param>
        /// <returns>serialized bytes</returns>
        public override byte[] Serialize(string data)
        {
            return data != null ? encoding.GetBytes(data) : null;
        }
    }
}
