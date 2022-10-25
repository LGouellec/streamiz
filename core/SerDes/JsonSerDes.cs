using System.Text;
using Confluent.Kafka;
using Newtonsoft.Json;

namespace Streamiz.Kafka.Net.SerDes
{
    /// <summary>
    /// Json SerDes without schema for <typeparam name="T"/>. Internally this serdes use the Newtonsoft library.
    /// </summary>
    public class JsonSerDes<T> : AbstractSerDes<T>
        where T : class
    {
        /// <summary>
        /// Convert string <code>data</code> into a byte array with a json serializer.
        /// </summary>
        /// <param name="data">object data</param>
        /// <param name="context">serialization context</param>
        /// <returns>serialized bytes</returns>
        public override byte[] Serialize(T data, SerializationContext context)
            => data != null ? Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(data)) : null;

        /// <summary>
        /// Deserialize a record value from a byte array into <typeparam name="T"/> object with a json deserializer.
        /// </summary>
        /// <param name="data">serialized bytes</param>
        /// <param name="context">serialization context</param>
        /// <returns>deserialized object data; may be null</returns>
        public override T Deserialize(byte[] data, SerializationContext context)
            => data != null ? JsonConvert.DeserializeObject<T>(Encoding.UTF8.GetString(data)) : null;
    }
}