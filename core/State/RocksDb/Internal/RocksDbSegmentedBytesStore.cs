using Streamiz.Kafka.Net.State.Internal;

namespace Streamiz.Kafka.Net.State.Internal
{
    internal class RocksDbSegmentedBytesStore :
        AbstractRocksDBSegmentedBytesStore<RocksDbKeyValueSegment>
    {
        public RocksDbSegmentedBytesStore(string name, long retention, long segmentInterval, IKeySchema keySchema) 
            : base(name,
                  keySchema,
                   new RocksDbKeyValueSegments(name, retention, segmentInterval))
        {
        }
    }
}