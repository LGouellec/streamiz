namespace Streamiz.Kafka.Net.Stream
{
    /// <summary>
    /// The <see cref="Reducer{V}"/> interface for combining two values of the same type into a new value.
    /// In contrast to <see cref="Aggregator{K, V, VA}" />
    /// the result type must be the same as the input type.
    /// <p>
    /// The provided values can be either original values from input keyvalue
    /// pair records or be a previously
    /// computed result from <see cref="Reducer{V}.Apply(V, V)"/>
    /// </p>
    /// <see cref="Reducer{V}"/> can be used to implement aggregation functions like sum, min, or max.
    /// </summary>
    /// <typeparam name="V">value type</typeparam>
    public interface Reducer<V>
    {
        /// <summary>
        /// Aggregate the two given values into a single one.
        /// </summary>
        /// <param name="value1">the first value for the aggregation</param>
        /// <param name="value2">the second value for the aggregation</param>
        /// <returns>the aggregated value</returns>
        V Apply(V value1, V value2);
    }
}
