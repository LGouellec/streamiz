using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.State.InMemory;

namespace Streamiz.Kafka.Net.Tests.Stores;

public class CachingInMemoryWindowStoreTests 
    : AbstractPersistentWindowStoreTests
{
    protected override IWindowStore<Bytes, byte[]> GetBackWindowStore()
    {
        return new InMemoryWindowStore(
            "test-w-store",
            RETENTION_MS,
            WINDOW_SIZE,
            false
        );
    }
}