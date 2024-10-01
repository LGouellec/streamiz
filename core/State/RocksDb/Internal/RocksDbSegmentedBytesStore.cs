namespace Streamiz.Kafka.Net.State.Internal
{
    internal class RocksDbSegmentedBytesStore :
        AbstractRocksDBSegmentedBytesStore<RocksDbKeyValueSegment>
    {
        public RocksDbSegmentedBytesStore(
            string name,
            string metricScope,
            long retention,
            long segmentInterval, IKeySchema keySchema) 
            : base(name,
                  keySchema,
                   new RocksDbKeyValueSegments(name, retention, segmentInterval, metricScope))
        {
        }
    }
}