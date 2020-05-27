using System;

namespace Streamiz.Kafka.Net.Stream
{
    /// <summary>
    /// The <see cref="Aggregator{K, V, VA}" /> interface for aggregating values of the given key.
    /// This is a generalization of {@link Reducer}
    /// and allows to have different types for input value and aggregation
    /// result.
    /// <see cref="Aggregator{K, V, VA}" /> is used in combination with <see cref="Initializer{VA}" />
    /// that provides an initial aggregation value.
    /// <see cref="Aggregator{K, V, VA}" /> can be used to implement aggregation functions like count.
    /// </summary>
    /// <typeparam name="K">key type</typeparam>
    /// <typeparam name="V">input value type</typeparam>
    /// <typeparam name="VA">aggregate value type</typeparam>
    public interface Aggregator<in K, in V, VA>
    {
        /// <summary>
        /// Compute a new aggregate from the key and value of a record and the current aggregate of the same key.
        /// </summary>
        /// <param name="key">the key of the record</param>
        /// <param name="value">the value of the record</param>
        /// <param name="aggregate">the current aggregate value</param>
        /// <returns>the new aggregate value</returns>
        VA Apply(K key, V value, VA aggregate);
    }
    
    internal class WrappedAggregator<K, V, VA> : Aggregator<K, V, VA>
    {
        private readonly Func<K, V, VA, VA> function;

        public WrappedAggregator(Func<K, V, VA, VA> function)
        {
            this.function = function ?? throw new ArgumentNullException($"Aggregator function can't be null");
        }

        public VA Apply(K key, V value, VA aggregate) => function.Invoke(key, value, aggregate);
    }

}
