using System;
using System.Collections.Generic;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Metrics.Internal;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.SerDes.Internal;
using Streamiz.Kafka.Net.State.Cache.Enumerator;
using Streamiz.Kafka.Net.State.Cache.Internal;
using Streamiz.Kafka.Net.State.Enumerator;
using Streamiz.Kafka.Net.State.Helper;
using Streamiz.Kafka.Net.State.Internal;
using Streamiz.Kafka.Net.Table;
using Streamiz.Kafka.Net.Table.Internal;

namespace Streamiz.Kafka.Net.State.Cache
{
    // TODO : involve hit Ratio
    internal class CachingWindowStore : 
        WrappedStateStore<IWindowStore<Bytes, byte[]>>,
        IWindowStore<Bytes, byte[]>,
        ICachedStateStore<byte[], byte[]>
    {
        private readonly long _windowSize;
        private readonly CacheSize _cacheSize;

        private MemoryCache<Bytes, CacheEntryValue> cache;
        private bool cachingEnabled;
        private bool sendOldValue;
        
        private Action<KeyValuePair<byte[],Change<byte[]>>> flushListener;
        
        private Sensor hitRatioSensor;
        private Sensor totalCacheSizeSensor;

        public CachingWindowStore(
            IWindowStore<Bytes, byte[]> wrapped,
            long windowSize,
            long segmentInterval,
            IKeySchema keySchema,
            CacheSize cacheSize)
            : base(wrapped)
        {
            _windowSize = windowSize;
            _cacheSize = cacheSize;
            SegmentInterval = segmentInterval;
            KeySchema = keySchema;
            SegmentCacheFunction = new SegmentedCacheFunction(KeySchema, segmentInterval);
            MaxObservedTimestamp = -1;
        }

        internal MemoryCache<Bytes, CacheEntryValue> Cache => cache;
        internal long MaxObservedTimestamp { get; set; }
        internal long SegmentInterval { get; }
        internal ICacheFunction SegmentCacheFunction { get; }
        internal IKeySchema KeySchema { get; }

        public override bool IsCachedStore => cachingEnabled;

        public override void Init(ProcessorContext context, IStateStore root)
        {
            base.Init(context, root);
            CreateCache(context);
            RegisterMetrics();
        }
        
        protected virtual void RegisterMetrics()
        {
            if (cachingEnabled)
            {
                hitRatioSensor = CachingMetrics.HitRatioSensor(context.Id, "cache-window-store", Name, context.Metrics);
                totalCacheSizeSensor =
                    CachingMetrics.TotalCacheSizeBytesSensor(context.Id, "cache-window-store", Name, context.Metrics);
            }
        }

        // Only for testing
        internal void CreateCache(ProcessorContext context)
        {
            cachingEnabled = context.Configuration.DefaultStateStoreCacheMaxBytes > 0 ||
                             _cacheSize is { CacheSizeBytes: > 0 };
            if(cachingEnabled)
                cache = new MemoryCache<Bytes, CacheEntryValue>(new MemoryCacheOptions {
                    SizeLimit = _cacheSize is { CacheSizeBytes: > 0 } ? _cacheSize.CacheSizeBytes :  context.Configuration.DefaultStateStoreCacheMaxBytes,
                    CompactionPercentage = .20
                }, new BytesComparer());
        }

        public byte[] Fetch(Bytes key, long time)
        {
            if (cachingEnabled)
            {
                var bytesKey = WindowKeyHelper.ToStoreKeyBinary(key, time, 0);
                var cacheKey = SegmentCacheFunction.CacheKey(bytesKey);

                byte[] value = null;

                if (cache.TryGetValue(cacheKey, out CacheEntryValue priorEntry))
                    value = priorEntry.Value;

                var currentStat = cache.GetCurrentStatistics();
                hitRatioSensor.Record((double)currentStat.TotalHits / (currentStat.TotalMisses + currentStat.TotalHits));

                return value ?? wrapped.Fetch(key, time);
            }
            
            return wrapped.Fetch(key, time);
        }

        public IWindowStoreEnumerator<byte[]> Fetch(Bytes key, DateTime from, DateTime to)
            => Fetch(key, from.GetMilliseconds(), to.GetMilliseconds());

        public IWindowStoreEnumerator<byte[]> Fetch(Bytes key, long from, long to)
        {
            if (cachingEnabled)
            {
                var wrappedEnumerator = wrapped.Fetch(key, from, to);
                IKeyValueEnumerator<Bytes, CacheEntryValue> cacheEnumerator = wrapped.Persistent
                    ? new CacheEnumeratorWrapper(this, key, from, to, true)
                    : FetchInternal(
                        SegmentCacheFunction.CacheKey(KeySchema.LowerRangeFixedSize(key, from)),
                        SegmentCacheFunction.CacheKey(KeySchema.UpperRangeFixedSize(key, to)), true);

                var hasNextCondition = KeySchema.HasNextCondition(key, key, from, to);
                var filteredCacheEnumerator =
                    new FilteredCacheEnumerator(cacheEnumerator, hasNextCondition, SegmentCacheFunction);

                return new MergedSortedCacheWindowStoreEnumerator(
                    SegmentCacheFunction,
                    filteredCacheEnumerator,
                    wrappedEnumerator,
                    true);
            }
            return wrapped.Fetch(key, from, to);
        }

        public IKeyValueEnumerator<Windowed<Bytes>, byte[]> All()
        {
            if (cachingEnabled)
            {
                var wrappedEnumerator = wrapped.All();
                var cacheEnumerator = new CacheEnumerator<Bytes, CacheEntryValue>(
                    cache.KeySetEnumerable(true),
                    cache,
                    UpdateRatioSensor);

                return new MergedSortedCacheWindowStoreKeyValueEnumerator(
                    cacheEnumerator,
                    wrappedEnumerator,
                    _windowSize,
                    SegmentCacheFunction,
                    new BytesSerDes(),
                    changelogTopic,
                    WindowKeyHelper.FromStoreKey,
                    WindowKeyHelper.ToStoreKeyBinary,
                    true);
            }
            return wrapped.All();
        }

        public IKeyValueEnumerator<Windowed<Bytes>, byte[]> FetchAll(DateTime from, DateTime to)
        {
            if (cachingEnabled)
            {
                var wrappedEnumerator = wrapped.FetchAll(from, to);
                var cacheEnumerator = new CacheEnumerator<Bytes, CacheEntryValue>(
                    cache.KeySetEnumerable(true),
                    cache,
                    UpdateRatioSensor
                );

                var hasNextCondition =
                    KeySchema.HasNextCondition(null, null, from.GetMilliseconds(), to.GetMilliseconds());
                var filteredCacheEnumerator =
                    new FilteredCacheEnumerator(cacheEnumerator, hasNextCondition, SegmentCacheFunction);

                return new MergedSortedCacheWindowStoreKeyValueEnumerator(
                    filteredCacheEnumerator,
                    wrappedEnumerator,
                    _windowSize,
                    SegmentCacheFunction,
                    new BytesSerDes(),
                    changelogTopic,
                    WindowKeyHelper.FromStoreKey,
                    WindowKeyHelper.ToStoreKeyBinary,
                    true);
            }
            
            return wrapped.FetchAll(from, to);
        }

        public void Put(Bytes key, byte[] value, long windowStartTimestamp)
        {
            if (cachingEnabled)
            {
                var keyBytes = WindowKeyHelper.ToStoreKeyBinary(key, windowStartTimestamp, 0);
                var cacheEntry = new CacheEntryValue(
                    value,
                    context.RecordContext.Headers,
                    context.Offset,
                    context.Timestamp,
                    context.Partition,
                    context.Topic);
                
                PutInternal(SegmentCacheFunction.CacheKey(keyBytes), cacheEntry);
                MaxObservedTimestamp = Math.Max(KeySchema.SegmentTimestamp(keyBytes), MaxObservedTimestamp);
            }
            else 
                wrapped.Put(key, value, windowStartTimestamp);
        }

        public override void Flush()
        {
            if (cachingEnabled)
            {
                cache.Compact(1); // Compact 100% of the cache
                base.Flush();
            }
            else
                wrapped.Flush();
        }
        
        public bool SetFlushListener(Action<KeyValuePair<byte[], Change<byte[]>>> listener, bool sendOldChanges)
        {
            flushListener = listener;
            sendOldValue = sendOldChanges;
            return true;
        }
        
        private void PutInternal(Bytes key, CacheEntryValue entry, bool fromWrappedCache = false)
        {
            long totalSize = key.Get.LongLength + entry.Size;

            var memoryCacheEntryOptions = new MemoryCacheEntryOptions<Bytes, CacheEntryValue>()
                .SetSize(totalSize)
                .RegisterPostEvictionCallback(CacheEntryEviction, cache);
            
            cache.Set(key, entry, memoryCacheEntryOptions, fromWrappedCache ? EvictionReason.None : EvictionReason.Setted);
            totalCacheSizeSensor.Record(cache.Size);
        }
        
        internal CacheEnumerator<Bytes, CacheEntryValue> FetchInternal(Bytes keyFrom, Bytes keyTo, bool forward)
        {
            return new CacheEnumerator<Bytes, CacheEntryValue>(
                cache.KeyRange(keyFrom, keyTo, true, forward),
                cache,
                UpdateRatioSensor);
        }

        private void UpdateRatioSensor()
        {
            var currentStat = cache.GetCurrentStatistics();
            hitRatioSensor.Record((double)currentStat.TotalHits / (currentStat.TotalMisses + currentStat.TotalHits));
        }
        
        private void CacheEntryEviction(Bytes key, CacheEntryValue value, EvictionReason reason, MemoryCache<Bytes, CacheEntryValue> state)
        {
            if (reason is EvictionReason.Replaced or EvictionReason.None) return;
            
            var binaryWindowKey = SegmentCacheFunction.Key(key).Get;
            var windowedKeyBytes = WindowKeyHelper.FromStoreBytesKey(binaryWindowKey, _windowSize);
            long windowStartTs = windowedKeyBytes.Window.StartMs;
            var binaryKey = windowedKeyBytes.Key;

            if (flushListener != null)
            {
                byte[] rawNewValue = value.Value;
                byte[] rawOldValue = rawNewValue == null || sendOldValue ? wrapped.Fetch(binaryKey, windowStartTs) : null;

                // this is an optimization: if this key did not exist in underlying store and also not in the cache,
                // we can skip flushing to downstream as well as writing to underlying store
                if (rawNewValue != null || rawOldValue != null)
                {
                    var currentContext = context.RecordContext;
                    context.SetRecordMetaData(value.Context);
                    wrapped.Put(binaryKey, rawNewValue, windowStartTs);
                    flushListener(new KeyValuePair<byte[], Change<byte[]>>(
                        binaryWindowKey,
                        new Change<byte[]>(sendOldValue ? rawOldValue : null, rawNewValue)));
                    context.SetRecordMetaData(currentContext);
                }
            }
            else
            {
                var currentContext = context.RecordContext;
                context.SetRecordMetaData(value.Context);
                wrapped.Put(binaryKey, value.Value, windowStartTs);
                context.SetRecordMetaData(currentContext);
            }
            
            totalCacheSizeSensor.Record(cache.Size);
        }
    }
}