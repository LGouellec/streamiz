using System;
using System.Collections.Generic;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.State.Cache.Internal;
using Streamiz.Kafka.Net.State.Enumerator;
using Streamiz.Kafka.Net.State.Internal;
using Streamiz.Kafka.Net.Table.Internal;

namespace Streamiz.Kafka.Net.State.Cache
{
    internal class CachingKeyValueStore :
        WrappedStateStore<IKeyValueStore<Bytes, byte[]>>,
        IKeyValueStore<Bytes, byte[]>, ICachedStateStore<byte[], byte[]>
    {
        private MemoryCache cache;
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
            cache = new MemoryCache(new MemoryCacheOptions {
                SizeLimit = context.Configuration.StateStoreCacheMaxBytes,
                CompactionPercentage = .20,
                ExpirationScanFrequency = TimeSpan.FromMilliseconds(context.Configuration.CommitIntervalMs)
            });
        }

        public override void Init(ProcessorContext context, IStateStore root)
        {
            base.Init(context, root);
            CreateCache(context);
        }

        private void CacheEntryEviction(object key, object value, EvictionReason reason, object state)
        {
            if (reason == EvictionReason.Replaced) return;
            
            Bytes keyBytes = key as Bytes;
            CacheEntryValue entryValue = value as CacheEntryValue;
            if (flushListener != null)
            {
                byte[] rawNewValue = entryValue.Value;
                byte[] rawOldValue = rawNewValue == null || sendOldValue ? wrapped.Get(keyBytes) : null;
                
                // this is an optimization: if this key did not exist in underlying store and also not in the cache,
                // we can skip flushing to downstream as well as writing to underlying store
                if (rawNewValue != null || rawOldValue != null)
                {
                    var currentContext = context.RecordContext;
                    context.SetRecordMetaData(entryValue.Context);
                    wrapped.Put(keyBytes, rawNewValue);
                    flushListener(new KeyValuePair<byte[], Change<byte[]>>(
                        keyBytes.Get,
                        new Change<byte[]>(sendOldValue ? rawOldValue : null, rawNewValue)));
                    context.SetRecordMetaData(currentContext);
                }
            }
            else
            {
                var currentContext = context.RecordContext;
                context.SetRecordMetaData(entryValue.Context);
                wrapped.Put(keyBytes, entryValue.Value);
                context.SetRecordMetaData(currentContext);
            }
        }
        
        public override void Flush()
        {
            cache.Compact(1); // Compact 100% of the cache
            while (cache.Count > 0) ;
            base.Flush();
        }
        
        public byte[] Get(Bytes key)
        {
            if (cache.TryGetValue(key, out CacheEntryValue priorEntry))
                return priorEntry.Value;
            
            var rawValue = wrapped.Get(key);
            if (rawValue == null)
                return null;
            PutInternal(key, new CacheEntryValue(rawValue));
            return rawValue;
        }
        
        public IKeyValueEnumerator<Bytes, byte[]> Range(Bytes from, Bytes to)
        {
            // finish with Merge enumerator
            throw new System.NotImplementedException();
        }

        public IKeyValueEnumerator<Bytes, byte[]> ReverseRange(Bytes from, Bytes to)
        {
            throw new System.NotImplementedException();
        }

        public IEnumerable<KeyValuePair<Bytes, byte[]>> All()
        {
            throw new System.NotImplementedException();
        }

        public IEnumerable<KeyValuePair<Bytes, byte[]>> ReverseAll()
        {
            throw new System.NotImplementedException();
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

            var memoryCacheEntryOptions = new MemoryCacheEntryOptions()
                .SetPriority(CacheItemPriority.Normal)
                .SetSize(totalSize)
                .RegisterPostEvictionCallback(CacheEntryEviction, cache);
            
            cache.Set(key, entry, memoryCacheEntryOptions);
        }

        public byte[] PutIfAbsent(Bytes key, byte[] value)
        {
            if(!cache.TryGetValue(key, out byte[] priorEntry))
                Put(key, value);
            return priorEntry;
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
            // First impl
            // if (cache.TryGetValue(key, out byte[] priorEntry))
            // {
            //     cache.Remove(key);
            //     wrapped.Delete(key);
            //     return priorEntry;
            // }
            //
            // return wrapped.Delete(key);
        }
    }
}