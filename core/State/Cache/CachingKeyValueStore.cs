using System;
using System.Collections.Generic;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Metrics.Internal;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.State.Cache.Enumerator;
using Streamiz.Kafka.Net.State.Cache.Internal;
using Streamiz.Kafka.Net.State.Enumerator;
using Streamiz.Kafka.Net.State.Internal;
using Streamiz.Kafka.Net.Table;
using Streamiz.Kafka.Net.Table.Internal;

namespace Streamiz.Kafka.Net.State.Cache
{
    internal class CachingKeyValueStore :
        WrappedStateStore<IKeyValueStore<Bytes, byte[]>>,
        IKeyValueStore<Bytes, byte[]>,
        ICachedStateStore<byte[], byte[]>
    {
        private readonly CacheSize _cacheSize;
        private Action<KeyValuePair<byte[], Change<byte[]>>> flushListener;
        private bool sendOldValue;
        private bool cachingEnabled;

        private Sensor hitRatioSensor = NoRunnableSensor.Empty;
        private Sensor totalCacheSizeSensor = NoRunnableSensor.Empty;

        internal MemoryCache<Bytes, CacheEntryValue> Cache { get; private set; }

        public CachingKeyValueStore(IKeyValueStore<Bytes, byte[]> wrapped, CacheSize cacheSize) 
            : base(wrapped)
        {
            _cacheSize = cacheSize;
        }

        protected virtual void RegisterMetrics()
        {
            if (cachingEnabled)
            {
                hitRatioSensor = CachingMetrics.HitRatioSensor(context.Id, "cache-store", Name, context.Metrics);
                totalCacheSizeSensor =
                    CachingMetrics.TotalCacheSizeBytesSensor(context.Id, "cache-store", Name, context.Metrics);
            }
        }

        public override bool IsCachedStore => cachingEnabled;
        
        public bool SetFlushListener(Action<KeyValuePair<byte[], Change<byte[]>>> listener, bool sendOldChanges)
        {
            flushListener = listener;
            sendOldValue = sendOldChanges;
            return true;
        }

        // Only for testing
        internal void CreateCache(ProcessorContext context)
        {
            cachingEnabled = context.Configuration.DefaultStateStoreCacheMaxBytes > 0 ||
                             _cacheSize is { CacheSizeBytes: > 0 };
            if(cachingEnabled)
                Cache = new MemoryCache<Bytes, CacheEntryValue>(new MemoryCacheOptions {
                    SizeLimit = _cacheSize is { CacheSizeBytes: > 0 } ? _cacheSize.CacheSizeBytes :  context.Configuration.DefaultStateStoreCacheMaxBytes,
                    CompactionPercentage = .20
                }, new BytesComparer());
        }

        private byte[] GetInternal(Bytes key)
        {
            if (cachingEnabled)
            {
                byte[] value;
                
                if (Cache.TryGetValue(key, out CacheEntryValue priorEntry))
                    value = priorEntry.Value;
                else
                {
                    value = wrapped.Get(key);
                    if(value != null)
                        PutInternal(key, new CacheEntryValue(value), true);
                }

                var currentStat = Cache.GetCurrentStatistics();
                hitRatioSensor.Record((double)currentStat.TotalHits / (currentStat.TotalMisses + currentStat.TotalHits));
                
                return value;
            }

            return wrapped.Get(key);
        } 

        public override void Init(ProcessorContext context, IStateStore root)
        {
            base.Init(context, root);
            CreateCache(context);
            RegisterMetrics();
        }
        
        private void UpdateRatioSensor()
        {
            var currentStat = Cache.GetCurrentStatistics();
            hitRatioSensor.Record((double)currentStat.TotalHits / (currentStat.TotalMisses + currentStat.TotalHits));
        }

        private void CacheEntryEviction(Bytes key, CacheEntryValue value, EvictionReason reason, MemoryCache<Bytes, CacheEntryValue> state)
        {
            if (reason is EvictionReason.Replaced or EvictionReason.None) return;
            
            if (flushListener != null)
            {
                byte[] rawNewValue = value.Value;
                byte[] rawOldValue = rawNewValue == null || sendOldValue ? wrapped.Get(key) : null;
                
                // this is an optimization: if this key did not exist in underlying store and also not in the cache,
                // we can skip flushing to downstream as well as writing to underlying store
                if (rawNewValue != null || rawOldValue != null)
                {
                    var currentContext = context.RecordContext;
                    context.SetRecordMetaData(value.Context);
                    wrapped.Put(key, rawNewValue);
                    flushListener(new KeyValuePair<byte[], Change<byte[]>>(
                        key.Get,
                        new Change<byte[]>(sendOldValue ? rawOldValue : null, rawNewValue)));
                    context.SetRecordMetaData(currentContext);
                }
            }
            else
            {
                var currentContext = context.RecordContext;
                context.SetRecordMetaData(value.Context);
                wrapped.Put(key, value.Value);
                context.SetRecordMetaData(currentContext);
            }
            
            totalCacheSizeSensor.Record(Cache.Size);
        }
        
        public override void Flush()
        {
            if (cachingEnabled)
            {
                Cache.Compact(1); // Compact 100% of the cache
                base.Flush();
            }
            else
                wrapped.Flush();
        }

        public byte[] Get(Bytes key)
            => GetInternal(key);
        
        public IKeyValueEnumerator<Bytes, byte[]> Range(Bytes from, Bytes to)
        {
            if (cachingEnabled)
            {
                var storeEnumerator = wrapped.Range(from, to);
                var cacheEnumerator =
                    new CacheEnumerator<Bytes, CacheEntryValue>(
                        Cache.KeyRange(from, to, true, true),
                        Cache,
                        UpdateRatioSensor);

                return new MergedStoredCacheKeyValueEnumerator(cacheEnumerator, storeEnumerator, true);
            }
            return wrapped.Range(from, to);
        }

        public IKeyValueEnumerator<Bytes, byte[]> ReverseRange(Bytes from, Bytes to)
        {
            if (cachingEnabled)
            {
                var storeEnumerator = wrapped.ReverseRange(from, to);
                var cacheEnumerator =
                    new CacheEnumerator<Bytes, CacheEntryValue>(
                        Cache.KeyRange(from, to, true, false),
                        Cache,
                        UpdateRatioSensor);

                return new MergedStoredCacheKeyValueEnumerator(cacheEnumerator, storeEnumerator, false);
            }

            return wrapped.ReverseRange(from, to);
        }

        private IEnumerable<KeyValuePair<Bytes, byte[]>> InternalAll(bool reverse)
        {
            var storeEnumerator = new WrapEnumerableKeyValueEnumerator<Bytes, byte[]>(wrapped.All());
            var cacheEnumerator = new CacheEnumerator<Bytes, CacheEntryValue>(
                Cache.KeySetEnumerable(reverse),
                Cache,
                UpdateRatioSensor);

            var mergedEnumerator = new MergedStoredCacheKeyValueEnumerator(cacheEnumerator, storeEnumerator, reverse);
            while (mergedEnumerator.MoveNext())
                if (mergedEnumerator.Current != null)
                    yield return mergedEnumerator.Current.Value;
        }
        
        public IEnumerable<KeyValuePair<Bytes, byte[]>> All()
        {
            if (cachingEnabled)
                return InternalAll(true);
            return wrapped.All();
        }

        public IEnumerable<KeyValuePair<Bytes, byte[]>> ReverseAll()
        {
            if (cachingEnabled)
                return InternalAll(false);

            return wrapped.ReverseAll();
        }

        public long ApproximateNumEntries() => cachingEnabled ? Cache.Count : wrapped.ApproximateNumEntries();

        public void Put(Bytes key, byte[] value)
        {
            if (cachingEnabled)
            {
                var cacheEntry = new CacheEntryValue(
                    value,
                    context.RecordContext.Headers,
                    context.Offset,
                    context.Timestamp,
                    context.Partition,
                    context.Topic);

                PutInternal(key, cacheEntry);
            }
            else
                wrapped.Put(key, value);
        }

        private void PutInternal(Bytes key, CacheEntryValue entry, bool fromWrappedCache = false)
        {
            long totalSize = key.Get.LongLength + entry.Size;

            var memoryCacheEntryOptions = new MemoryCacheEntryOptions<Bytes, CacheEntryValue>()
                .SetSize(totalSize)
                .RegisterPostEvictionCallback(CacheEntryEviction, Cache);
            
            Cache.Set(key, entry, memoryCacheEntryOptions, fromWrappedCache ? EvictionReason.None : EvictionReason.Setted);
            totalCacheSizeSensor.Record(Cache.Size);
        }

        public byte[] PutIfAbsent(Bytes key, byte[] value)
        {
            if (cachingEnabled)
            {
                var v = GetInternal(key);
                if (v == null)
                    Put(key, value);
                return v;
            }

            return wrapped.PutIfAbsent(key, value);
        }

        public void PutAll(IEnumerable<KeyValuePair<Bytes, byte[]>> entries)
        {
            if (cachingEnabled)
            {
                foreach (var entry in entries)
                    Put(entry.Key, entry.Value);
            }
            else
                wrapped.PutAll(entries);
        }

        public byte[] Delete(Bytes key)
        {
            if (cachingEnabled)
            {
                var rawValue = Get(key);
                Put(key, null);
                return rawValue;
            }

            return wrapped.Delete(key);
        }

        public new void Close()
        {
            if (cachingEnabled)
            {
                Cache.Dispose();
                base.Close();
            }
            else 
                wrapped.Close();
        }
    }
}