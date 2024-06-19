using System;
using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.State.Cache;
using Streamiz.Kafka.Net.State.Cache.Enumerator;
using Streamiz.Kafka.Net.State.Cache.Internal;
using Streamiz.Kafka.Net.State.Enumerator;
using Streamiz.Kafka.Net.State.InMemory;

namespace Streamiz.Kafka.Net.Tests.Stores;

public class MergedStoredCacheKeyValueEnumeratorTest
{
    private IKeyValueStore<Bytes, byte[]> internalStore;
    private MemoryCache<Bytes, CacheEntryValue> cacheStore;

    [SetUp]
    public void Begin()
    {
        internalStore = new InMemoryKeyValueStore("in-mem-store");
        cacheStore = new MemoryCache<Bytes, CacheEntryValue>(new MemoryCacheOptions
        {
            SizeLimit = Int32.MaxValue,
            CompactionPercentage = .20
        }, new BytesComparer());
    }

    [TearDown]
    public void Dispose()
    {
        internalStore.Close();
        cacheStore.Dispose();
    }

    private MergedStoredCacheKeyValueEnumerator CreateEnumerator()
    {
        var cacheEnumerator = new CacheEnumerator<Bytes, CacheEntryValue>(
            cacheStore.KeySetEnumerable(true), cacheStore, () => { });
        var storeEnumerator = new WrapEnumerableKeyValueEnumerator<Bytes, byte[]>(internalStore.All());
        return new MergedStoredCacheKeyValueEnumerator(cacheEnumerator, storeEnumerator, true);
    }

    private void PutCache(Bytes key, byte[] value)
    {
        var cacheEntry = new CacheEntryValue(
            value,
            new Headers(),
            0,
            100,
            0,
            "topic");

        long totalSize = key.Get.LongLength + cacheEntry.Size;

        var memoryCacheEntryOptions = new MemoryCacheEntryOptions<Bytes, CacheEntryValue>()
            .SetSize(totalSize);

        cacheStore.Set(key, cacheEntry, memoryCacheEntryOptions, EvictionReason.Setted);
    }

    [Test]
    public void ShouldIterateOverRange()
    {
        byte[][] bytes =
        {
            new byte[] { 0 }, new byte[] { 1 }, new byte[] { 2 }, new byte[] { 3 }, new byte[] { 4 },
            new byte[] { 5 }, new byte[] { 6 }, new byte[] { 7 }, new byte[] { 8 }, new byte[] { 9 }, new byte[] { 10 },
            new byte[] { 11 }
        };

        for (int i = 0; i < bytes.Length; i += 2)
        {
            internalStore.Put(Bytes.Wrap(bytes[i]), bytes[i]); // 0 2 4 6 8 10
            PutCache(Bytes.Wrap(bytes[i + 1]), bytes[i + 1]); // 1 3 5 7 8 11
        }

        Bytes from = Bytes.Wrap(new byte[] {2});
        Bytes to = Bytes.Wrap(new byte[] {9});
        
        var cacheEnumerator = new CacheEnumerator<Bytes, CacheEntryValue>(
            cacheStore.KeyRange(from, to, true, true), cacheStore, () => { });
        var storeEnumerator = internalStore.Range(from, to);
        var mergedEnumerator = new MergedStoredCacheKeyValueEnumerator(cacheEnumerator, storeEnumerator, true);
        
        // 23456789
        
        byte[][] values = new byte[8][];
        int index = 0;
        int bytesIndex = 2;
        while (mergedEnumerator.MoveNext()) {
            Assert.NotNull(mergedEnumerator.Current);
             byte[] value = mergedEnumerator.Current.Value.Value;
            values[index++] = value;
            Assert.AreEqual(bytes[bytesIndex++], value);
        }
        mergedEnumerator.Dispose();
    }

    [Test]
    public void ShouldReverseIterateOverRange() {
        byte[][] bytes =
        {
            new byte[] { 0 }, new byte[] { 1 }, new byte[] { 2 }, new byte[] { 3 }, new byte[] { 4 },
            new byte[] { 5 }, new byte[] { 6 }, new byte[] { 7 }, new byte[] { 8 }, new byte[] { 9 }, new byte[] { 10 },
            new byte[] { 11 }
        };
        
        for (int i = 0; i < bytes.Length; i += 2)
        {
            internalStore.Put(Bytes.Wrap(bytes[i]), bytes[i]); // 0 2 4 6 8 10
            PutCache(Bytes.Wrap(bytes[i + 1]), bytes[i + 1]); // 1 3 5 7 8 11
        }

        Bytes from = Bytes.Wrap(new byte[] {2});
        Bytes to = Bytes.Wrap(new byte[] {9});
        
        var cacheEnumerator = new CacheEnumerator<Bytes, CacheEntryValue>(
            cacheStore.KeyRange(from, to, true, false), cacheStore, null);
        var storeEnumerator = internalStore.ReverseRange(from, to);
        var mergedEnumerator = new MergedStoredCacheKeyValueEnumerator(cacheEnumerator, storeEnumerator, false);
        
        // 98765432
        byte[][] values = new byte[8][];
        int index = 0;
        int bytesIndex = 9;
        while (mergedEnumerator.MoveNext()) {
            Assert.NotNull(mergedEnumerator.Current);
            byte[] value = mergedEnumerator.Current.Value.Value;
            values[index++] = value;
            Assert.AreEqual(bytes[bytesIndex--], value);
        }
        mergedEnumerator.Dispose();
    }
    
    [Test]
    public void ShouldSkipLargerDeletedCacheValue()
    {
        byte[][] bytes = { new byte[] { 0 }, new byte[] { 1 } };
        internalStore.Put(Bytes.Wrap(bytes[0]), bytes[0]);
        PutCache(Bytes.Wrap(bytes[1]), null);
        using var enumerator = CreateEnumerator();
        Assert.IsTrue(enumerator.MoveNext());
        Assert.NotNull(enumerator.Current);
        Assert.AreEqual(new byte[] { 0 }, enumerator.Current.Value.Key.Get);
        Assert.IsFalse(enumerator.MoveNext());
    }

    [Test]
    public void ShouldSkipSmallerDeletedCachedValue()
    {
        byte[][] bytes = { new byte[] { 0 }, new byte[] { 1 } };
        PutCache(Bytes.Wrap(bytes[0]), null);
        internalStore.Put(Bytes.Wrap(bytes[1]), bytes[1]);
        using var enumerator = CreateEnumerator();
        Assert.IsTrue(enumerator.MoveNext());
        Assert.NotNull(enumerator.Current);
        Assert.AreEqual(new byte[] { 1 }, enumerator.Current.Value.Key.Get);
        Assert.IsFalse(enumerator.MoveNext());
    }

    [Test]
    public void ShouldIgnoreIfDeletedInCacheButExistsInStore()
    {
        byte[][] bytes = { new byte[] { 0 } };
        PutCache(Bytes.Wrap(bytes[0]), null);
        internalStore.Put(Bytes.Wrap(bytes[0]), bytes[0]);
        using var enumerator = CreateEnumerator();
        Assert.IsFalse(enumerator.MoveNext());
    }

    [Test]
    public void ShouldNotHaveNextIfAllCachedItemsDeleted()
    {
        byte[][] bytes = { new byte[] { 0 }, new byte[] { 1 }, new byte[] { 2 } };
        foreach (byte[] aByte in bytes)
        {
            Bytes aBytes = Bytes.Wrap(aByte);
            internalStore.Put(aBytes, aByte);
            PutCache(aBytes, null);
        }

        using var enumerator = CreateEnumerator();
        Assert.IsFalse(enumerator.MoveNext());
    }

    [Test]
    public void ShouldNotHaveNextIfOnlyCacheItemsAndAllDeleted()
    {
        byte[][] bytes = { new byte[] { 0 }, new byte[] { 1 }, new byte[] { 2 } };
        foreach (byte[] aByte in bytes)
        {
            PutCache(Bytes.Wrap(aByte), null);
        }

        using var enumerator = CreateEnumerator();
        Assert.IsFalse(enumerator.MoveNext());
    }

    [Test]
    public void ShouldSkipAllDeletedFromCache()
    {
        byte[][] bytes =
        {
            new byte[] { 0 }, new byte[] { 1 }, new byte[] { 2 }, new byte[] { 3 }, new byte[] { 4 }, new byte[] { 5 },
            new byte[] { 6 }, new byte[] { 7 }, new byte[] { 8 }, new byte[] { 9 }, new byte[] { 10 }, new byte[] { 11 }
        };

        foreach (byte[] aByte in bytes)
        {
            Bytes aBytes = Bytes.Wrap(aByte);
            internalStore.Put(aBytes, aByte);
            PutCache(aBytes, aByte);
        }

        PutCache(Bytes.Wrap(new byte[] { 1 }), null);
        PutCache(Bytes.Wrap(new byte[] { 2 }), null);
        PutCache(Bytes.Wrap(new byte[] { 3 }), null);
        PutCache(Bytes.Wrap(new byte[] { 8 }), null);
        PutCache(Bytes.Wrap(new byte[] { 11 }), null);

        using var enumerator = CreateEnumerator();
        enumerator.MoveNext();
        Assert.NotNull(enumerator.Current);
        Assert.AreEqual(new byte[] { 0 }, enumerator.Current.Value.Value);
        enumerator.MoveNext();
        Assert.NotNull(enumerator.Current);
        Assert.AreEqual(new byte[] { 4 }, enumerator.Current.Value.Value);
        enumerator.MoveNext();
        Assert.NotNull(enumerator.Current);
        Assert.AreEqual(new byte[] { 5 }, enumerator.Current.Value.Value);
        enumerator.MoveNext();
        Assert.NotNull(enumerator.Current);
        Assert.AreEqual(new byte[] { 6 }, enumerator.Current.Value.Value);
        enumerator.MoveNext();
        Assert.NotNull(enumerator.Current);
        Assert.AreEqual(new byte[] { 7 }, enumerator.Current.Value.Value);
        enumerator.MoveNext();
        Assert.NotNull(enumerator.Current);
        Assert.AreEqual(new byte[] { 9 }, enumerator.Current.Value.Value);
        enumerator.MoveNext();
        Assert.NotNull(enumerator.Current);
        Assert.AreEqual(new byte[] { 10 }, enumerator.Current.Value.Value);
        Assert.IsFalse(enumerator.MoveNext());
    }
}