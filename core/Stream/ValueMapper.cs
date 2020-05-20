using System;

namespace Streamiz.Kafka.Net.Stream
{
    /// <summary>
    /// The <see cref="IValueMapper{V, VR}"/> interface for mapping a value to a new value of arbitrary type.
    /// This is a stateless record-by-record operation, i.e, <see cref="IValueMapper{V, VR}.Apply(V)"/> is invoked individually for each record
    /// of a stream.
    /// If <see cref="IValueMapper{V, VR}"/> is applied to a keyvalue pair record the record's
    /// key is preserved.
    /// If a record's key and value should be modified <see cref="IKeyValueMapper{K, V, VR}"/> can be used.
    /// </summary>
    /// <typeparam name="V">value type</typeparam>
    /// <typeparam name="VR">mapped value type</typeparam>
    public interface IValueMapper<in V, out VR>
    {
        /// <summary>
        /// Map the given value to a new value.
        /// </summary>
        /// <param name="value">Value to be mapped</param>
        /// <returns>New value</returns>
        VR Apply(V value);
    }

    internal class WrappedValueMapper<V, VR> : IValueMapper<V, VR>
    {
        private readonly Func<V, VR> wrappedFunction;

        public WrappedValueMapper(Func<V, VR> function)
        {
            this.wrappedFunction = function ?? throw new ArgumentNullException($"Mapper function can't be null");
        }

        public VR Apply(V value) => wrappedFunction.Invoke(value);
    }
}
