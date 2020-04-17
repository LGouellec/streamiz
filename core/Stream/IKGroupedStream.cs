namespace Kafka.Streams.Net.Stream
{
    /// <summary>
    /// TODO IMPLEMENT. <see cref="IKGroupedStream{K, V}"/> is an abstraction of a grouped record stream of keyvalue pairs.
    /// It is an intermediate representation of a <see cref="IKStream{K, V}"/> in order to apply an aggregation operation on the original
    /// <see cref="IKStream{K, V}"/> records.
    /// A <see cref="IKGroupedStream{K, V}"/> must be obtained from a <see cref="IKStream{K, V}"/> via <see cref="IKStream{K, V}.GroupByKey(string)"/> or
    /// <see cref="IKStream{K, V}.GroupBy{KR, KRS}(System.Func{K, V, KR}, string)" />
    /// </summary>
    /// <typeparam name="K">Type of key</typeparam>
    /// <typeparam name="V">Type of value</typeparam>
    public interface IKGroupedStream<K, V>
    {
        // TODO : Statefull operation
    }
}
