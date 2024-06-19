using System;
using System.Collections.Generic;
using System.Text;
using NUnit.Framework;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.SerDes.Internal;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.State.Cache;
using Streamiz.Kafka.Net.State.Cache.Enumerator;
using Streamiz.Kafka.Net.State.Enumerator;
using Streamiz.Kafka.Net.State.Helper;
using Streamiz.Kafka.Net.State.Internal;
using Streamiz.Kafka.Net.Stream;

namespace Streamiz.Kafka.Net.Tests.Stores;

public class MergedSortedCacheWrappedWindowStoreKeyValueEnumeratorTests
{
    private static IKeyValueEnumerator<Windowed<Bytes>, byte[]> StoreKvs()
    {
        return new WrapEnumerableKeyValueEnumerator<Windowed<Bytes>, byte[]>(
            new List<KeyValuePair<Windowed<Bytes>, byte[]>>
            {
                new(new Windowed<Bytes>(Bytes.Wrap(Encoding.UTF8.GetBytes(storeKey)), storeWindow),
                    Encoding.UTF8.GetBytes(storeKey))
            });
    }

    private static IKeyValueEnumerator<Bytes, CacheEntryValue> CacheKvs()
    {
        return new WrapEnumerableKeyValueEnumerator<Bytes, CacheEntryValue>(
            new List<KeyValuePair<Bytes, CacheEntryValue>>
            {
                new(segmentedCacheFunction.CacheKey(WindowKeyHelper.ToStoreKeyBinary(
                        new Windowed<string>(cacheKey, cacheWindow), 0, new StringSerDes())),
                    new CacheEntryValue(Encoding.UTF8.GetBytes(cacheKey)))
            });
    }

    private class MockSegmentedCacheFunction : SegmentedCacheFunction
    {
        public MockSegmentedCacheFunction(IKeySchema keySchema, long segmentInterval)
            : base(keySchema, segmentInterval)
        {
        }

        public override long SegmentId(Bytes key)
        {
            return 0L;
        }
    }

    private static readonly int WINDOW_SIZE = 10;
    private static readonly String storeKey = "a";
    private static readonly String cacheKey = "b";
    private static readonly TimeWindow storeWindow = new(0, 1);
    private static readonly TimeWindow cacheWindow = new(10, 20);

    private static readonly MockSegmentedCacheFunction
        segmentedCacheFunction = new MockSegmentedCacheFunction(null, -1);

    private IKeyValueEnumerator<Windowed<Bytes>, byte[]> storeKvs;
    private IKeyValueEnumerator<Bytes, CacheEntryValue> cacheKvs;
    private ISerDes<Bytes> serdes = new BytesSerDes();

    private MergedSortedCacheWindowStoreKeyValueEnumerator CreateEnumerator(
        IKeyValueEnumerator<Windowed<Bytes>, byte[]> storeKvs,
        IKeyValueEnumerator<Bytes, CacheEntryValue> cacheKvs,
        bool forward)
    {
        return new MergedSortedCacheWindowStoreKeyValueEnumerator(
            cacheKvs,
            storeKvs,
            WINDOW_SIZE,
            segmentedCacheFunction,
            serdes,
            "topic",
            WindowKeyHelper.FromStoreKey,
            WindowKeyHelper.ToStoreKeyBinary,
            forward
        );
    }

    private KeyValuePair<Windowed<Bytes>, byte[]> ConvertKeyValuePair(KeyValuePair<Windowed<string>, string> pair)
    {
        return new KeyValuePair<Windowed<Bytes>, byte[]>(
            new Windowed<Bytes>(Bytes.Wrap(Encoding.UTF8.GetBytes(pair.Key.Key)), pair.Key.Window),
            Encoding.UTF8.GetBytes(pair.Value));
    }

    private Windowed<string> ConvertWindowedKey(Windowed<Bytes> windowed)
    {
        var key = Encoding.UTF8.GetString(windowed.Key.Get);
        return new Windowed<string>(key, windowed.Window);
    }

    [SetUp]
    public void Init()
    {
        storeKvs = StoreKvs();
        cacheKvs = CacheKvs();
    }


    [Test]
    public void ShouldHaveNextFromStore() {
        using var enumerator = CreateEnumerator(storeKvs, EmptyKeyValueEnumerator<Bytes, CacheEntryValue>.Empty, true);
        Assert.IsTrue(enumerator.MoveNext());
    }
    
    [Test]
    public void ShouldHaveNextFromReverseStore() {
        using var enumerator = CreateEnumerator(storeKvs, EmptyKeyValueEnumerator<Bytes, CacheEntryValue>.Empty, false);
        Assert.IsTrue(enumerator.MoveNext());
    }
    
    [Test]
    public void ShouldGetNextFromStore() {
        using var enumerator = CreateEnumerator(storeKvs, EmptyKeyValueEnumerator<Bytes, CacheEntryValue>.Empty, true);
        Assert.IsTrue(enumerator.MoveNext());
        Assert.IsTrue(enumerator.Current.HasValue);
        Assert.AreEqual(
            ConvertKeyValuePair(new KeyValuePair<Windowed<string>, string>(
                new Windowed<string>(storeKey, storeWindow), storeKey)),
            enumerator.Current.Value);
    }
    
    [Test]
    public void ShouldGetNextFromReverseStore() {
        using var enumerator = CreateEnumerator(storeKvs, EmptyKeyValueEnumerator<Bytes, CacheEntryValue>.Empty, false);
        Assert.IsTrue(enumerator.MoveNext());
        Assert.IsTrue(enumerator.Current.HasValue);
        Assert.AreEqual(
            ConvertKeyValuePair(new KeyValuePair<Windowed<string>, string>(
                new Windowed<string>(storeKey, storeWindow), storeKey)),
            enumerator.Current.Value);
    }
    
    [Test]
    public void ShouldPeekNextKeyFromStore() {
        using var enumerator = CreateEnumerator(storeKvs, EmptyKeyValueEnumerator<Bytes, CacheEntryValue>.Empty, true);
        Assert.IsTrue(enumerator.MoveNext());
        Assert.AreEqual(
            new Windowed<string>(storeKey, storeWindow),
            ConvertWindowedKey(enumerator.PeekNextKey()));
    }
    
    [Test]
    public void ShouldPeekNextKeyFromReverseStore() {
        using var enumerator = CreateEnumerator(storeKvs, EmptyKeyValueEnumerator<Bytes, CacheEntryValue>.Empty, false);
        Assert.IsTrue(enumerator.MoveNext());
        Assert.AreEqual(
            new Windowed<string>(storeKey, storeWindow),
            ConvertWindowedKey(enumerator.PeekNextKey()));
    }
    
    [Test]
    public void ShouldHaveNextFromCache() {
        using var enumerator = CreateEnumerator(
            EmptyKeyValueEnumerator<Windowed<Bytes>, byte[]>.Empty,
            cacheKvs,
            true);
        Assert.IsTrue(enumerator.MoveNext());
    }
    
    [Test]
    public void ShouldHaveNextFromReverseCache() {
        using var enumerator = CreateEnumerator(
            EmptyKeyValueEnumerator<Windowed<Bytes>, byte[]>.Empty,
            cacheKvs,
            false);
        Assert.IsTrue(enumerator.MoveNext());
    }
    
    [Test]
    public void ShoulGetNextFromCache() {
        using var enumerator = CreateEnumerator(
            EmptyKeyValueEnumerator<Windowed<Bytes>, byte[]>.Empty,
            cacheKvs,
            true);
        Assert.IsTrue(enumerator.MoveNext());
        Assert.IsTrue(enumerator.Current.HasValue);
        Assert.AreEqual(
            ConvertKeyValuePair(new KeyValuePair<Windowed<string>, string>(
                new Windowed<string>(cacheKey, cacheWindow), cacheKey)),
            enumerator.Current.Value);
    }
    
    [Test]
    public void ShoulGetNextFromReverseCache() {
        using var enumerator = CreateEnumerator(
            EmptyKeyValueEnumerator<Windowed<Bytes>, byte[]>.Empty,
            cacheKvs,
            false);
        Assert.IsTrue(enumerator.MoveNext());
        Assert.IsTrue(enumerator.Current.HasValue);
        Assert.AreEqual(
            ConvertKeyValuePair(new KeyValuePair<Windowed<string>, string>(
                new Windowed<string>(cacheKey, cacheWindow), cacheKey)),
            enumerator.Current.Value);
    }
    
    [Test]
    public void ShoulPeekNextKeyFromCache() {
        using var enumerator = CreateEnumerator(
            EmptyKeyValueEnumerator<Windowed<Bytes>, byte[]>.Empty,
            cacheKvs,
            true);
        Assert.IsTrue(enumerator.MoveNext());
        Assert.AreEqual(
            new Windowed<string>(cacheKey, cacheWindow),
            ConvertWindowedKey(enumerator.PeekNextKey()));
    }
    
    [Test]
    public void ShoulPeekNextKeyFromReverseCache() {
        using var enumerator = CreateEnumerator(
            EmptyKeyValueEnumerator<Windowed<Bytes>, byte[]>.Empty,
            cacheKvs,
            false);
        Assert.IsTrue(enumerator.MoveNext());
        Assert.AreEqual(
            new Windowed<string>(cacheKey, cacheWindow),
            ConvertWindowedKey(enumerator.PeekNextKey()));
    }
    
    [Test]
    public void ShouldIterateBothStoreAndCache() {
        using var enumerator = CreateEnumerator(
            storeKvs,
            cacheKvs,
            true);
        
        Assert.IsTrue(enumerator.MoveNext());
        Assert.IsTrue(enumerator.Current.HasValue);
        Assert.AreEqual(
            ConvertKeyValuePair(new KeyValuePair<Windowed<string>, string>(
                new Windowed<string>(storeKey, storeWindow), storeKey)),
            enumerator.Current.Value);
        
        Assert.IsTrue(enumerator.MoveNext());
        Assert.IsTrue(enumerator.Current.HasValue);
        Assert.AreEqual(
            ConvertKeyValuePair(new KeyValuePair<Windowed<string>, string>(
                new Windowed<string>(cacheKey, cacheWindow), cacheKey)),
            enumerator.Current.Value);
        
        Assert.IsFalse(enumerator.MoveNext());
    }
    
    [Test]
    public void ShouldReverseIterateBothStoreAndCache() {
        using var enumerator = CreateEnumerator(
            storeKvs,
            cacheKvs,
            false);
        
        Assert.IsTrue(enumerator.MoveNext());
        Assert.IsTrue(enumerator.Current.HasValue);
        Assert.AreEqual(
            ConvertKeyValuePair(new KeyValuePair<Windowed<string>, string>(
                new Windowed<string>(cacheKey, cacheWindow), cacheKey)),
            enumerator.Current.Value);
        
        Assert.IsTrue(enumerator.MoveNext());
        Assert.IsTrue(enumerator.Current.HasValue);
        Assert.AreEqual(
            ConvertKeyValuePair(new KeyValuePair<Windowed<string>, string>(
                new Windowed<string>(storeKey, storeWindow), storeKey)),
            enumerator.Current.Value);
        
        Assert.IsFalse(enumerator.MoveNext());
    }
}