namespace Streamiz.Kafka.Net.Table.Internal
{
    internal class GlobalKTable<K, V> : IGlobalKTable<K, V>
    {
        public GlobalKTable(IKTableValueGetterSupplier<K, V> valueGetterSupplier, string queryableStoreName)
        {
            ValueGetterSupplier = valueGetterSupplier;
            QueryableStoreName = queryableStoreName;
        }

        public IKTableValueGetterSupplier<K, V> ValueGetterSupplier { get; }

        public string QueryableStoreName { get; }
    }
}
