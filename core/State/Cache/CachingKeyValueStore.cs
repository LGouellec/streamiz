using System;
using System.Collections.Generic;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.State.Cache.Enumerator;
using Streamiz.Kafka.Net.State.Cache.Internal;
using Streamiz.Kafka.Net.State.Enumerator;
using Streamiz.Kafka.Net.State.Internal;
using Streamiz.Kafka.Net.Table.Internal;

namespace Streamiz.Kafka.Net.State.Cache
{
    //  add option Materialize
    // Add StoreBuilder
    // Check if disabled or not
    // Add new metrics
    // add documentation
    // Check flush and forward messages in downstream
    internal class CachingKeyValueStore :
        WrappedStateStore<IKeyValueStore<Bytes, byte[]>>,
        IKeyValueStore<Bytes, byte[]>, ICachedStateStore<byte[], byte[]>
    {
        private MemoryCache<Bytes, CacheEntryValue> cache;
        private Action<KeyValuePair<byte[], Change<byte[]>>> flushListener;
        private bool sendOldValue;
        
        public CachingKeyValueStore(IKeyValueStore<Bytes, byte[]> wrapped) 
            : base(wrapped)
        {
            
        }
        
        public bool SetFlushListener(Action<KeyValuePair<byte[], Change<byte[]>>> listener, bool sendOldChanges)
        {
            flushListener = listener;
            sendOldValue = sendOldChanges;
            return true;
        }

        // Only for testing
        internal void CreateCache(ProcessorContext context)
        {
            cache = new MemoryCache<Bytes, CacheEntryValue>(new MemoryCacheOptions {
                SizeLimit = context.Configuration.StateStoreCacheMaxBytes,
                CompactionPercentage = .20
            }, new BytesComparer());
        }

        private byte[] GetInternal(Bytes key)
        {
            if (cache.TryGetValue(key, out CacheEntryValue priorEntry))
                return priorEntry.Value;
            
            var rawValue = wrapped.Get(key);
            if (rawValue == null)
                return null;
            PutInternal(key, new CacheEntryValue(rawValue));
            return rawValue;
        } 

        public override void Init(ProcessorContext context, IStateStore root)
        {
            base.Init(context, root);
            CreateCache(context);
        }

        private void CacheEntryEviction(Bytes key, CacheEntryValue value, EvictionReason reason, MemoryCache<Bytes, CacheEntryValue> state)
        {
            if (reason == EvictionReason.Replaced) return;
            
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
        }
        
        public override void Flush()
        {
            cache.Compact(1); // Compact 100% of the cache
            base.Flush();
        }

        public byte[] Get(Bytes key)
            => GetInternal(key);
        
        public IKeyValueEnumerator<Bytes, byte[]> Range(Bytes from, Bytes to)
        {
            var storeEnumerator = wrapped.Range(from, to);
            var cacheEnumerator = new CacheEnumerator<Bytes, CacheEntryValue>(cache.KeyRange(from, to, true, true), cache);
            
            return new MergedStoredCacheKeyValueEnumerator(cacheEnumerator, storeEnumerator, true);
        }

        public IKeyValueEnumerator<Bytes, byte[]> ReverseRange(Bytes from, Bytes to)
        {
            var storeEnumerator = wrapped.ReverseRange(from, to);
            var cacheEnumerator = new CacheEnumerator<Bytes, CacheEntryValue>(cache.KeyRange(from, to, true, false), cache);
            
            return new MergedStoredCacheKeyValueEnumerator(cacheEnumerator, storeEnumerator, false);
        }

        public IEnumerable<KeyValuePair<Bytes, byte[]>> All()
        {
            var storeEnumerator = new WrapEnumerableKeyValueEnumerator<Bytes, byte[]>(wrapped.All());
            var cacheEnumerator = new CacheEnumerator<Bytes, CacheEntryValue>(cache.KeySetEnumerable(true), cache);

            return new WrapKeyValueEnumeratorEnumerable<Bytes, byte[]>(
                new MergedStoredCacheKeyValueEnumerator(cacheEnumerator, storeEnumerator, true));
        }

        public IEnumerable<KeyValuePair<Bytes, byte[]>> ReverseAll()
        {
            var storeEnumerator = new WrapEnumerableKeyValueEnumerator<Bytes, byte[]>(wrapped.ReverseAll());
            var cacheEnumerator = new CacheEnumerator<Bytes, CacheEntryValue>(cache.KeySetEnumerable(false), cache);

            return new WrapKeyValueEnumeratorEnumerable<Bytes, byte[]>(
                new MergedStoredCacheKeyValueEnumerator(cacheEnumerator, storeEnumerator, false));
        }

        public long ApproximateNumEntries() => cache.Count;

        public void Put(Bytes key, byte[] value)
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

        private void PutInternal(Bytes key, CacheEntryValue entry)
        {
            long totalSize = key.Get.LongLength + entry.Size;

            var memoryCacheEntryOptions = new MemoryCacheEntryOptions<Bytes, CacheEntryValue>()
                .SetSize(totalSize)
                .RegisterPostEvictionCallback(CacheEntryEviction, cache);
            
            cache.Set(key, entry, memoryCacheEntryOptions);
        }

        public byte[] PutIfAbsent(Bytes key, byte[] value)
        {
            var v = GetInternal(key);
            if(v == null)
                Put(key, value);
            return v;
        }

        public void PutAll(IEnumerable<KeyValuePair<Bytes, byte[]>> entries)
        {
            foreach(var entry in entries)
                Put(entry.Key, entry.Value);
        }

        public byte[] Delete(Bytes key)
        {
            var rawValue = Get(key);
            Put(key, null);
            return rawValue;
        }

        public new void Close()
        {
            cache.Dispose();
            base.Close();
        }
    }
}