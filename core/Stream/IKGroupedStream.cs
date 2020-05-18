using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Kafka.Net.Stream
{
    /// <summary>
    /// <see cref="IKGroupedStream{K, V}"/> is an abstraction of a grouped record stream of keyvalue pairs.
    /// It is an intermediate representation of a <see cref="IKStream{K, V}"/> in order to apply an aggregation operation on the original
    /// <see cref="IKStream{K, V}"/> records.
    /// A <see cref="IKGroupedStream{K, V}"/> must be obtained from a <see cref="IKStream{K, V}"/> via <see cref="IKStream{K, V}.GroupByKey(string)"/> or
    /// <see cref="IKStream{K, V}.GroupBy{KR, KRS}(System.Func{K, V, KR}, string)" />
    /// </summary>
    /// <typeparam name="K">Type of key</typeparam>
    /// <typeparam name="V">Type of value</typeparam>
    public interface IKGroupedStream<K, V>
    {
        /// <summary>
        /// Count the number of records in this stream by the grouped key.
        /// Records with null key or value are ignored.
        /// The result is written into a local <see cref="IKeyValueStore{K, V}"/> (which is basically an ever-updating materialized view).
        /// Furthermore, updates to the store are sent downstream into a <see cref="IKTable{K, V}" /> changelog stream.
        /// </summary>
        /// <returns>a <see cref="IKTable{K, V}" /> that contains "update" records with unmodified keys and <see cref="long" /> values that represent the latest (rolling) count (i.e., number of records) for each key</returns>
        IKTable<K, long> Count();

        /// <summary>
        /// Count the number of records in this stream by the grouped key.
        /// Records with null key or value are ignored.
        /// The result is written into a local <see cref="IKeyValueStore{K, V}"/> (which is basically an ever-updating materialized view).
        /// Furthermore, updates to the store are sent downstream into a <see cref="IKTable{K, V}" /> changelog stream.
        /// </summary>
        /// <param name="named">a config used to name the processor in the topology</param>
        /// <returns>a <see cref="IKTable{K, V}" /> that contains "update" records with unmodified keys and <see cref="long" /> values that represent the latest (rolling) count (i.e., number of records) for each key</returns>
        IKTable<K, long> Count(string named);

        /// <summary>
        /// Count the number of records in this stream by the grouped key.
        /// Records with null key or value are ignored.
        /// The result is written into a local <see cref="IKeyValueStore{K, V}"/> (which is basically an ever-updating materialized view)
        /// provided by the given store name in <see cref="Materialized{K, V, S}" />
        /// Furthermore, updates to the store are sent downstream into a <see cref="IKTable{K, V}" /> changelog stream.
        /// <p>
        /// Not all updates might get sent downstream, as an internal cache is used to deduplicate consecutive updates to
        /// the same key.
        /// </p>
        /// To query the local <see cref="IKeyValueStore{K, V}"/> it must be obtained via
        /// <see cref="KafkaStream.Store{T, K, V}(StoreQueryParameters{T, K, V})" />.
        /// </summary>
        /// <param name="materialized">an instance of <see cref="Materialized{K, V, S}"/> used to materialize a state store. Node: the valueSerde will be automatically set to <see cref="Int64SerDes"/> if there is no valueSerde provided </param>
        /// <param name="named">a named config used to name the processor in the topology</param>
        /// <returns>a <see cref="IKTable{K, V}"/> that contains "update" records with unmodified keys and <see cref="long"/> values that represent the latest (rolling) count (i.e., number of records) for each key</returns>
        IKTable<K, long> Count(Materialized<K, long, IKeyValueStore<Bytes, byte[]>> materialized, string named = null);
    }
}
