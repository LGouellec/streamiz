using Confluent.Kafka;
using Moq;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.State.Cache;
using Streamiz.Kafka.Net.State.Internal;

namespace Streamiz.Kafka.Net.Tests.Stores;

public class CachingPersistentWindowStoreTests :
    AbstractPersistentWindowStoreTests
{
    protected override IWindowStore<Bytes, byte[]> GetBackWindowStore()
    {
        var bytesStore = new RocksDbSegmentedBytesStore(
            "test-w-store",
            (long)RETENTION_MS.TotalMilliseconds,
            SEGMENT_INTERVAL,
            keySchema);

        return new RocksDbWindowStore(
            bytesStore,
            WINDOW_SIZE
            , false);
    }
}