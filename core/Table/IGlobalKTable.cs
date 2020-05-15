namespace Streamiz.Kafka.Net.Table
{
    /// <summary>
    /// IGlobalKTable is an abstraction of a <i>changelog stream</i> from a primary-keyed table.
    /// Each record in this changelog stream is an update on the primary-keyed table with the record key as the primary key.
    /// IGlobalKTable can only be used as right-hand side input for <see cref="Stream.IKStream{K,V}"/>-table joins.
    /// 
    /// In contrast to a <see cref="IKTable{K, V}"/> that is partitioned over all <see cref="KafkaStream"/> instances, a IGlobalKTable
    /// is fully replicated per <see cref="KafkaStream"/> instance.
    /// Every partition of the underlying topic is consumed by each GlobalKTable, such that the full set of data is
    /// available in every <see cref="KafkaStream"/> instance.
    /// This provides the ability to perform joins with <see cref="Stream.IKStream{K,V}"/> without having to repartition the input stream.
    /// All joins with the GlobalKTable require that a <see cref="Stream.IKeyValueMapper{K,V,VR}"/> is provided that can map from the
    /// key and value of the left hand side <see cref="Stream.IKStream{K,V}"/> to the key of the right hand side GlobalKTable.
    /// 
    /// An IGlobalKTable is created via a <see cref="StreamBuilder"/>. For example:
    /// <code>
    /// builder.GlobalTable(topic, new StringSerDes(), new StringSerDes(), InMemory{string, string}.As("topics-store"));
    /// </code>
    /// all GlobalKTables are backed by a <see cref="State.ReadOnlyKeyValueStore{K, V}"/> and are therefore queryable via the
    /// interactive queries API.
    /// For example:
    /// <code>
    /// builder.GlobalTable(topic, new StringSerDes(), new StringSerDes(), InMemory{string, string}.As("topics-store"))
    /// ...
    /// KafkaStream strem = ...;
    /// ...
    /// var queryableStoreType = QueryableStoreTypes.KeyValueStore{string, string}();
    /// var store = stream.Store(StoreQueryParameters.FromNameAndType("topics-store", queryableStoreType));
    /// store.Get(key); // can be done on any key, as all keys are present
    /// </code>
    /// Note that in contrast to <see cref="IKTable{K, V}"/> a GlobalKTable's state holds a full copy of the underlying topic,
    /// thus all keys can be queried locally.
    /// 
    /// Records from the source topic that have null keys are dropped.
    /// </summary>
    public interface IGlobalKTable
    {
        /// <summary>
        /// The name of the local state store that can be used to query this <see cref="IGlobalKTable"/>
        /// or null if this <see cref="IGlobalKTable"/> cannot be queried.
        /// </summary>
        string QueryableStoreName { get; }
    }
}
