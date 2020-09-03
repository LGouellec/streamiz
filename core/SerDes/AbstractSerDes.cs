using Confluent.Kafka;
using System;

namespace Streamiz.Kafka.Net.SerDes
{
    /// <summary>
    /// Abstract SerDes class that implement <see cref="ISerDes{T}"/> and <see cref="ISerDes"/>.
    /// If you must implement your own serdes, please herit to <see cref="AbstractSerDes{T}"/>.
    /// </summary>
    /// <typeparam name="T">Type to be serialized from and deserialized into.</typeparam>
    public abstract class AbstractSerDes<T> : ISerDes<T>
    {
        /// <summary>
        /// initialized boolean
        /// </summary>
        protected bool isInitialized = false;

        /// <summary>
        /// Deserialize a record value from a byte array into an object.
        /// </summary>
        /// <param name="data">serialized bytes; may be null; implementations are recommended to handle null by returning a value or null rather than throwing an exception.</param>
        /// <param name="context">Context relevant to a serialization or deserialization operation.</param>
        /// <returns>deserialized object data; may be null</returns>
        public object DeserializeObject(byte[] data, SerializationContext context) => Deserialize(data, context);

        /// <summary>
        /// Convert <code>data</code> into a byte array.
        /// </summary>
        /// <param name="data">object data</param>
        /// <param name="context">Context relevant to a serialization or deserialization operation.</param>
        /// <returns>serialized bytes</returns>
        public byte[] SerializeObject(object data, SerializationContext context)
        {
            if (data is T || data == null)
                return Serialize((T)data, context);
            else
                throw new InvalidOperationException($"Impossible to serialize data type {data.GetType().Name} with {GetType().Name}<{typeof(T).Name}>");
        }

        /// <summary>
        /// [ABSTRACT] - Convert <code>data</code> into a byte array.
        /// </summary>
        /// <param name="data">typed data</param>
        /// <param name="context">Context relevant to a serialization or deserialization operation.</param>
        /// <returns>serialized bytes</returns>
        public abstract byte[] Serialize(T data, SerializationContext context);

        /// <summary>
        /// [ABSTRACT] - Deserialize a record value from a byte array into a value or object.
        /// </summary>
        /// <param name="data">serialized bytes; may be null; implementations are recommended to handle null by returning a value or null rather than throwing an exception.</param>
        /// <param name="context">Context relevant to a serialization or deserialization operation.</param>
        /// <returns>deserialized typed data; may be null</returns>
        public abstract T Deserialize(byte[] data, SerializationContext context);

        /// <summary>
        /// Initialize method with a current context which contains <see cref="IStreamConfig"/>.
        /// Can be used to initialize the serdes according to some parameters present in the configuration such as the schema.registry.url
        /// </summary>
        /// <param name="context">SerDesContext with stream configuration</param>
        public virtual void Initialize(SerDesContext context)
        {
            isInitialized = true;
        }
    }
}
