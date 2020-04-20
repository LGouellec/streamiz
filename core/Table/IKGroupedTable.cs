namespace Streamiz.Kafka.Net.Table
{
    /// <summary>
    /// TODO IMPLEMENT. <see cref="IKGroupedTable{K, V}"/> is an abstraction of a re-grouped changelog stream from a primary-keyed table,
    /// usually on a different grouping key than the original primary key.
    /// A <see cref="IKGroupedTable{K, V}"/> must be obtained from a <see cref="IKTable{K, V}"/> via <see cref="IKTable{K, V}.GroupBy{KR, VR}(System.Func{K, V, System.Collections.Generic.KeyValuePair{KR, VR}}, string)"/>
    /// </summary>
    /// <typeparam name="K">Type of key</typeparam>
    /// <typeparam name="V">Type of value</typeparam>
    public interface IKGroupedTable<K, V>
    {
        // TODO : Statefull operation
    }
}
