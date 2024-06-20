using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.State.Cache;
using Streamiz.Kafka.Net.State.Cache.Internal;

namespace Streamiz.Kafka.Net.Tests.Stores;

public class MemoryCacheTests
{
    private MemoryCache<Bytes, CacheEntryValue> memoryCache;
    private const int headerSizeCacheEntry = 25;
    
    [SetUp]
    public void Initialize()
    {
        var options = new MemoryCacheOptions();
        options.SizeLimit = 1000;
        options.CompactionPercentage = 0.1;

        memoryCache = new MemoryCache<Bytes, CacheEntryValue>(options, new BytesComparer());
    }

    [TearDown]
    public void Dispose()
    {
        memoryCache?.Dispose();
    }
    
    private CacheEntryValue CreateValueEntry(string value)
    {
        return new CacheEntryValue(
            Encoding.UTF8.GetBytes(value),
            new Headers(), 
            0,
            DateTime.UtcNow.GetMilliseconds(), 
            0, 
            "topic");
    }
    
    private void PutCache(Bytes key, string value, PostEvictionDelegate<Bytes, CacheEntryValue> evictionDelegate)
    {
        var entry = CreateValueEntry(value);
        long totalSize = key.Get.LongLength + entry.Size;

        var memoryCacheEntryOptions = new MemoryCacheEntryOptions<Bytes, CacheEntryValue>()
            .SetSize(totalSize)
            .RegisterPostEvictionCallback(evictionDelegate, memoryCache);
            
        memoryCache.Set(key, entry, memoryCacheEntryOptions, EvictionReason.Setted);
    } 
    
    [Test]
    public void KeepTrackOnSize()
    {
        var bytes = Bytes.Wrap(new byte[] { 1 });
        
        PutCache(bytes, "coucou", (key, value, reason, state) => { });
        PutCache(bytes, "sylvain", (key, value, reason, state) => { });
        Assert.AreEqual(1 + headerSizeCacheEntry + "sylvain".Length, memoryCache.Size);
    }
    
    [Test]
    public void ShouldPutGet() {
        PutCache(Bytes.Wrap(new byte[]{1}), "a", (key, value, reason, state) => { });
        PutCache(Bytes.Wrap(new byte[]{2}), "b", (key, value, reason, state) => { });
        PutCache(Bytes.Wrap(new byte[]{3}), "c", (key, value, reason, state) => { });
        
        Assert.AreEqual("a", Encoding.UTF8.GetString(memoryCache.Get(Bytes.Wrap(new byte[]{1})).Value));
        Assert.AreEqual("b", Encoding.UTF8.GetString(memoryCache.Get(Bytes.Wrap(new byte[]{2})).Value));
        Assert.AreEqual("c", Encoding.UTF8.GetString(memoryCache.Get(Bytes.Wrap(new byte[]{3})).Value));
        Assert.AreEqual(3, memoryCache.GetCurrentStatistics().TotalHits);
    }
    
    [Test]
    public void ShouldDeleteAndUpdateSize() {
        PutCache(Bytes.Wrap(new byte[]{1}), "a", (key, value, reason, state) => { });
        memoryCache.Remove(Bytes.Wrap(new byte[]{1}));
        Assert.AreEqual(0, memoryCache.Size);
    }
    
    [Test]
    public void ShouldEvictEldestEntry() {
        memoryCache?.Dispose();
        
        var options = new MemoryCacheOptions();
        options.SizeLimit = 50;
        options.CompactionPercentage = 0.1;
        memoryCache = new MemoryCache<Bytes, CacheEntryValue>(options, new BytesComparer());
        
        PutCache(Bytes.Wrap(new byte[]{1}), "test123", (key, value, reason, state) => {
            Assert.AreEqual(EvictionReason.Capacity, reason);
        });
        Thread.Sleep(5);
        PutCache(Bytes.Wrap(new byte[]{2}), "test456", (key, value, reason, state) => { });
        Thread.Sleep(5);
        PutCache(Bytes.Wrap(new byte[]{3}), "test789", (key, value, reason, state) => { });


        Assert.IsNull(memoryCache.Get(Bytes.Wrap(new byte[] { 1 })));
        Assert.AreEqual(2L, memoryCache.Count);
    }
    
    [Test]
    public void ShouldEvictLRU() {
        memoryCache?.Dispose();

        var results = new List<String>();
        var expected = new List<String>{
            "test123", "test456", "test789"
        };
        
        var expectedbis = new List<String>{
            "test456", "test789", "test123"
        };
        
        var clockTime = new MockSystemTime(DateTime.Now);
        
        var options = new MemoryCacheOptions();
        options.SizeLimit = 100000;
        options.CompactionPercentage = 0.1;
        memoryCache = new MemoryCache<Bytes, CacheEntryValue>(options, new BytesComparer(), clockTime);

        PostEvictionDelegate<Bytes, CacheEntryValue> deleg = (key, value, reason, state) => {
            results.Add(Encoding.UTF8.GetString(value.Value));
        };
        
        PutCache(Bytes.Wrap(new byte[]{1}), "test123", deleg);
        clockTime.AdvanceTime(TimeSpan.FromMinutes(1));
        PutCache(Bytes.Wrap(new byte[]{2}), "test456", deleg);
        clockTime.AdvanceTime(TimeSpan.FromMinutes(1));
        PutCache(Bytes.Wrap(new byte[]{3}), "test789", deleg);
        clockTime.AdvanceTime(TimeSpan.FromMinutes(1));

        memoryCache.Compact(1d); // total flush
       
        Assert.AreEqual(expected, results);
        results.Clear();
        
        PutCache(Bytes.Wrap(new byte[]{1}), "test123", deleg);
        clockTime.AdvanceTime(TimeSpan.FromMinutes(1));
        PutCache(Bytes.Wrap(new byte[]{2}), "test456", deleg);
        clockTime.AdvanceTime(TimeSpan.FromMinutes(1));
        PutCache(Bytes.Wrap(new byte[]{3}), "test789", deleg);
        clockTime.AdvanceTime(TimeSpan.FromMinutes(1));
        memoryCache.Get(Bytes.Wrap(new byte[] { 1 }));
        memoryCache.Compact(1d); // total
        Assert.AreEqual(expectedbis, results);
    }

}