using System;

namespace Streamiz.Kafka.Net.Stream
{
    /// <summary>
    /// The <see cref="IKeyValueMapper{K, V, VR}"/> interface for mapping a keyvalue pair to a new value of arbitrary type.
    /// For example, it can be used to
    /// - map from an input keyvalue pair to an output keyvalue pair with different key and/or value type
    /// - map from an input record to a new key (with arbitrary key type as specified by <typeparamref name="VR"/>
    /// This is a stateless record-by-record operation, i.e, <see cref="IKeyValueMapper{K, V, VR}.Apply(K, V)"/> is invoked individually for each
    /// record of a stream.
    /// </summary>
    /// <typeparam name="K">key type</typeparam>
    /// <typeparam name="V">value type</typeparam>
    /// <typeparam name="VR">mapped value type</typeparam>
    public interface IKeyValueMapper<K, V, VR>
    {
        /// <summary>
        /// Map a record with the given key and value to a new value.
        /// </summary>
        /// <param name="key">the key of the record</param>
        /// <param name="value">the value of the record</param>
        /// <returns>the new value</returns>
        VR Apply(K key, V value);
    }

    internal class WrappedKeyValueMapper<K, V, VR> : IKeyValueMapper<K, V, VR>
    {
        private readonly Func<K, V, VR> wrappedFunction;

        public WrappedKeyValueMapper(Func<K, V, VR> function)
        {
            this.wrappedFunction = function;
        }
        public VR Apply(K key, V value) => wrappedFunction.Invoke(key, value);
    }
}
