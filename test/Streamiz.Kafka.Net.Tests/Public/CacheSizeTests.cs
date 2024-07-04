using NUnit.Framework;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.State.Cache;
using Streamiz.Kafka.Net.State.InMemory;
using Streamiz.Kafka.Net.Table;
using Streamiz.Kafka.Net.Tests.Helpers;

namespace Streamiz.Kafka.Net.Tests.Public;

public class CacheSizeTests
{
    [Test]
    public void CacheSizeKVStoreEnabledWithDefaultConf()
    {
        var config = new StreamConfig();
        config.DefaultStateStoreCacheMaxBytes = CacheSize.OfMb(1).CacheSizeBytes;
        var context = new MockProcessorContext(new TaskId { Id = 0, Partition = 0 }, config);
        var inMemoryKeyValue = new InMemoryKeyValueStore("store");
        var cache = new CachingKeyValueStore(inMemoryKeyValue, null);
        cache.Init(context, cache);
        Assert.IsTrue(cache.IsCachedStore);
        Assert.AreEqual(CacheSize.OfMb(1).CacheSizeBytes, cache.Cache.Capacity);
    }
    
    [Test]
    public void CacheSizeKVStoreEnabledWithSpecificConf()
    {
        var config = new StreamConfig();
        config.DefaultStateStoreCacheMaxBytes = CacheSize.OfMb(1).CacheSizeBytes;
        var context = new MockProcessorContext(new TaskId { Id = 0, Partition = 0 }, config);
        var inMemoryKeyValue = new InMemoryKeyValueStore("store");
        var cache = new CachingKeyValueStore(inMemoryKeyValue,  CacheSize.OfMb(10));
        cache.Init(context, cache);
        Assert.IsTrue(cache.IsCachedStore);
        Assert.AreEqual(CacheSize.OfMb(10).CacheSizeBytes, cache.Cache.Capacity);
    }
    
    [Test]
    public void CacheSizeKVStoreDisabled()
    {
        var config = new StreamConfig();
        config.DefaultStateStoreCacheMaxBytes = 0L;
        var context = new MockProcessorContext(new TaskId { Id = 0, Partition = 0 }, config);
        var inMemoryKeyValue = new InMemoryKeyValueStore("store");
        var cache = new CachingKeyValueStore(inMemoryKeyValue, null);
        cache.Init(context, cache);
        Assert.IsFalse(cache.IsCachedStore);
    }
    
    [Test]
    public void CacheSizeKVStoreDisabledExplicitConf()
    {
        var config = new StreamConfig();
        config.DefaultStateStoreCacheMaxBytes = 0L;
        var context = new MockProcessorContext(new TaskId { Id = 0, Partition = 0 }, config);
        var inMemoryKeyValue = new InMemoryKeyValueStore("store");
        var cache = new CachingKeyValueStore(inMemoryKeyValue, CacheSize.OfB(0));
        cache.Init(context, cache);
        Assert.IsFalse(cache.IsCachedStore);
    }
}