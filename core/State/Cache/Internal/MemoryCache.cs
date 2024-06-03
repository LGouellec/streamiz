// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// This is a fork from Microsoft.Extensions.Caching.Memory.MemoryCache https://github.com/dotnet/runtime/blob/main/src/libraries/Microsoft.Extensions.Caching.Memory
// The only difference is the compaction process and eviction callback is synchronous whereas the .NET repo is asyncrhonous

using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Streamiz.Kafka.Net.Crosscutting;

namespace Streamiz.Kafka.Net.State.Cache.Internal
{
    /// <summary>
    /// An implementation of <see cref="IMemoryCache"/> using a dictionary to
    /// store its entries.
    /// </summary>
    internal class MemoryCache<K, V> : IMemoryCache<K, V>
    {
        private readonly IComparer<K> _keyComparer;
        internal readonly ILogger _logger;

        private readonly MemoryCacheOptions _options;

        private readonly List<WeakReference<Stats>>? _allStats;
        private readonly Stats? _accumulatedStats;
        private readonly ThreadLocal<Stats>? _stats;
        private CoherentState<K, V> _coherentState;
        private bool _disposed;
        private DateTime _lastExpirationScan;

        /// <summary>
        /// Creates a new <see cref="MemoryCache"/> instance.
        /// </summary>
        /// <param name="optionsAccessor">The options of the cache.</param>
        public MemoryCache(IOptions<MemoryCacheOptions> optionsAccessor, IComparer<K> keyComparer)
            : this(optionsAccessor, keyComparer, NullLoggerFactory.Instance)
        {
        }

        /// <summary>
        /// Creates a new <see cref="MemoryCache"/> instance.
        /// </summary>
        /// <param name="optionsAccessor">The options of the cache.</param>
        /// <param name="loggerFactory">The factory used to create loggers.</param>
        public MemoryCache(IOptions<MemoryCacheOptions> optionsAccessor, IComparer<K> keyComparer, ILoggerFactory loggerFactory)
        {
            if (keyComparer == null)
                throw new ArgumentException();
            if (optionsAccessor == null)
                throw new ArgumentException();
            if (loggerFactory == null)
                throw new ArgumentException();

            _keyComparer = keyComparer;
            _options = optionsAccessor.Value;
            _logger = loggerFactory.CreateLogger<MemoryCache<K, V>>();

            _coherentState = new CoherentState<K, V>();

            if (_options.TrackStatistics)
            {
                _allStats = new List<WeakReference<Stats>>();
                _accumulatedStats = new Stats();
                _stats = new ThreadLocal<Stats>(() => new Stats(this));
            }

            _lastExpirationScan = UtcNow;
        }

        //private DateTime UtcNow => _options.Clock?.UtcNow.UtcDateTime ?? DateTime.UtcNow;
        private DateTime UtcNow => DateTime.UtcNow;

        /// <summary>
        /// Cleans up the background collection events.
        /// </summary>
        ~MemoryCache() => Dispose(false);

        /// <summary>
        /// Gets the count of the current entries for diagnostic purposes.
        /// </summary>
        public int Count => _coherentState.Count;

        /// <summary>
        /// Gets an enumerable of the all the keys in the <see cref="MemoryCache"/>.
        /// </summary>
        public IEnumerable<K> Keys => _coherentState._entries.Keys;

        internal IEnumerable<K> KeySetEnumerable(bool forward)
        {
            return forward ? Keys : Keys.OrderByDescending(k => k, _keyComparer);
        }

        // TODO: Maybe used Keys accessor ..
        internal IEnumerable<K> KeyRange(K from, K to, bool inclusive, bool forward)
        {
            if (from == null && to == null)
                return KeySetEnumerable(forward);
            
            IEnumerable<K> selectedKeys;

            if (from == null)
                selectedKeys = _coherentState._entries
                    .HeadMap(to, inclusive)
                    .Select(kv => kv.Key);
            else if (to == null)
                selectedKeys = _coherentState._entries
                    .TailMap(from, true)
                    .Select(kv => kv.Key);
            else if (_keyComparer.Compare(from, to) > 0)
                selectedKeys = new List<K>();
            else
                selectedKeys = _coherentState._entries
                    .SubMap(from, to, true, inclusive)
                    .Select(kv => kv.Key);
                
            return forward ? selectedKeys : selectedKeys.OrderByDescending(k => k, _keyComparer);
        }

        /// <summary>
        /// Internal accessor for Size for testing only.
        ///
        /// Note that this is only eventually consistent with the contents of the collection.
        /// See comment on <see cref="CoherentState"/>.
        /// </summary>
        internal long Size => _coherentState.Size;

        /// <inheritdoc />
        public ICacheEntry<K, V> CreateEntry(K key)
        {
            CheckDisposed();
            ValidateCacheKey(key);

            return new CacheEntry<K, V>(key, this);
        }

        internal void SetEntry(CacheEntry<K, V> entry)
        {
            if (_disposed)
            {
                // No-op instead of throwing since this is called during CacheEntry.Dispose
                return;
            }

            if (_options.HasSizeLimit && entry.Size < 0)
            {
                throw new InvalidOperationException("Entry size is not correct, please set a size greater or equal to O");
            }

            DateTime utcNow = UtcNow;

            // Initialize the last access timestamp at the time the entry is added
            entry.LastAccessed = utcNow;

            CoherentState<K, V> coherentState = _coherentState; // Clear() can update the reference in the meantime
            if (coherentState._entries.TryGetValue(entry.Key, out CacheEntry<K, V> priorEntry))
            {
                priorEntry.SetExpired(EvictionReason.Replaced);
            }

            if (entry.CheckExpired(utcNow))
            {
                entry.InvokeEvictionCallbacks();
                if (priorEntry != null)
                {
                    coherentState.RemoveEntry(priorEntry, _options);
                }
            }
            else if (!UpdateCacheSizeExceedsCapacity(entry, coherentState))
            {
                bool entryAdded;
                if (priorEntry == null)
                {
                    // Try to add the new entry if no previous entries exist.
                    entryAdded = coherentState._entries.TryAdd(entry.Key, entry);
                }
                else
                {
                    // Try to update with the new entry if a previous entries exist.
                    entryAdded = coherentState._entries.AddOrUpdate(entry.Key, entry);

                    if (entryAdded)
                    {
                        if (_options.HasSizeLimit)
                        {
                            // The prior entry was removed, decrease the by the prior entry's size
                            Interlocked.Add(ref coherentState._cacheSize, -priorEntry.Size);
                        }
                    }
                    else
                    {
                        // The update will fail if the previous entry was removed after retrieval.
                        // Adding the new entry will succeed only if no entry has been added since.
                        // This guarantees removing an old entry does not prevent adding a new entry.
                        entryAdded = coherentState._entries.TryAdd(entry.Key, entry);
                    }
                }

                if (!entryAdded)
                {
                    if (_options.HasSizeLimit)
                    {
                        // Entry could not be added, reset cache size
                        Interlocked.Add(ref coherentState._cacheSize, -entry.Size);
                    }
                    entry.SetExpired(EvictionReason.Replaced);
                    entry.InvokeEvictionCallbacks();
                }

                priorEntry?.InvokeEvictionCallbacks();
            }
            else
            {
                entry.SetExpired(EvictionReason.Capacity);
                TriggerOvercapacityCompaction();
                entry.InvokeEvictionCallbacks();
                if (priorEntry != null)
                {
                    coherentState.RemoveEntry(priorEntry, _options);
                }
            }

            StartScanForExpiredItemsIfNeeded(utcNow);
        }

        /// <inheritdoc />
        public bool TryGetValue(K key, out V result)
        {
            if (key == null)
                throw new ArgumentException();

            CheckDisposed();

            DateTime utcNow = UtcNow;

            CoherentState<K, V> coherentState = _coherentState; // Clear() can update the reference in the meantime
            if (coherentState._entries.TryGetValue(key, out CacheEntry<K, V> tmp))
            {
                CacheEntry<K, V> entry = tmp;
                entry.LastAccessed = utcNow;
                result = entry.Value;
                
                // Hit
                if (_allStats is not null)
                {
                    if (IntPtr.Size == 4)
                        Interlocked.Increment(ref GetStats().Hits);
                    else
                        GetStats().Hits++;
                }

                return true;
            }

            result = default(V);
            // Miss
            if (_allStats is not null)
            {
                if (IntPtr.Size == 4)
                    Interlocked.Increment(ref GetStats().Misses);
                else
                    GetStats().Misses++;
            }

            return false;
        }

        /// <inheritdoc />
        public void Remove(K key)
        {
            if (key == null)
                throw new ArgumentException();

            CheckDisposed();

            CoherentState<K, V> coherentState = _coherentState; // Clear() can update the reference in the meantime
            if (coherentState._entries.Remove(key, out CacheEntry<K, V> entry))
            {
                if (_options.HasSizeLimit)
                {
                    Interlocked.Add(ref coherentState._cacheSize, -entry.Size);
                }

                entry.SetExpired(EvictionReason.Removed);
                entry.InvokeEvictionCallbacks();
            }

            StartScanForExpiredItemsIfNeeded(UtcNow);
        }

        /// <summary>
        /// Removes all keys and values from the cache.
        /// </summary>
        public void Clear()
        {
            CheckDisposed();

            CoherentState<K, V> oldState = Interlocked.Exchange(ref _coherentState, new CoherentState<K, V>());
            foreach (KeyValuePair<K, CacheEntry<K, V>> entry in oldState._entries)
            {
                entry.Value.SetExpired(EvictionReason.Removed);
                entry.Value.InvokeEvictionCallbacks();
            }
        }

        /// <summary>
        /// Gets a snapshot of the current statistics for the memory cache.
        /// </summary>
        /// <returns>Returns <see langword="null"/> if statistics are not being tracked because <see cref="MemoryCacheOptions.TrackStatistics" /> is <see langword="false"/>.</returns>
        public MemoryCacheStatistics? GetCurrentStatistics()
        {
            if (_allStats is not null)
            {
                (long hit, long miss) sumTotal = Sum();
                return new MemoryCacheStatistics()
                {
                    TotalMisses = sumTotal.miss,
                    TotalHits = sumTotal.hit,
                    CurrentEntryCount = Count,
                    CurrentEstimatedSize = _options.SizeLimit.HasValue ? Size : null
                };
            }

            return null;
        }

        internal void EntryExpired(CacheEntry<K, V> entry)
        {
            // TODO: For efficiency consider processing these expirations in batches.
            _coherentState.RemoveEntry(entry, _options);
            StartScanForExpiredItemsIfNeeded(UtcNow);
        }

        // Called by multiple actions to see how long it's been since we last checked for expired items.
        // If sufficient time has elapsed then a scan is initiated on a background task.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void StartScanForExpiredItemsIfNeeded(DateTime utcNow)
        {
            if (_options.ExpirationScanFrequency < utcNow - _lastExpirationScan)
            {
                ScheduleTask(utcNow);
            }

            void ScheduleTask(DateTime utcNow)
            {
                _lastExpirationScan = utcNow;
                ScanForExpiredItems();
            }
        }

        private (long, long) Sum()
        {
            lock (_allStats!)
            {
                long hits = _accumulatedStats!.Hits;
                long misses = _accumulatedStats.Misses;

                foreach (WeakReference<Stats> wr in _allStats)
                {
                    if (wr.TryGetTarget(out Stats? stats))
                    {
                        hits += Interlocked.Read(ref stats.Hits);
                        misses += Interlocked.Read(ref stats.Misses);
                    }
                }

                return (hits, misses);
            }
        }

        private Stats GetStats() => _stats!.Value!;

        internal sealed class Stats
        {
            private readonly MemoryCache<K, V>? _memoryCache;
            public long Hits;
            public long Misses;

            public Stats() { }

            public Stats(MemoryCache<K, V> memoryCache)
            {
                _memoryCache = memoryCache;
                _memoryCache.AddToStats(this);
            }

            ~Stats() => _memoryCache?.RemoveFromStats(this);
        }

        private void RemoveFromStats(Stats current)
        {
            lock (_allStats!)
            {
                for (int i = 0; i < _allStats.Count; i++)
                {
                    if (!_allStats[i].TryGetTarget(out Stats? stats))
                    {
                        _allStats.RemoveAt(i);
                        i--;
                    }
                }

                _accumulatedStats!.Hits += Interlocked.Read(ref current.Hits);
                _accumulatedStats.Misses += Interlocked.Read(ref current.Misses);
                _allStats.TrimExcess();
            }
        }

        private void AddToStats(Stats current)
        {
            lock (_allStats!)
            {
                _allStats.Add(new WeakReference<Stats>(current));
            }
        }

        private void ScanForExpiredItems()
        {
            DateTime utcNow = _lastExpirationScan = UtcNow;

            CoherentState<K, V> coherentState = _coherentState; // Clear() can update the reference in the meantime
            foreach (KeyValuePair<K, CacheEntry<K, V>> item in coherentState._entries)
            {
                CacheEntry<K, V> entry = item.Value;

                if (entry.CheckExpired(utcNow))
                {
                    coherentState.RemoveEntry(entry, _options);
                }
            }
        }

        /// <summary>
        /// Returns true if increasing the cache size by the size of entry would
        /// cause it to exceed any size limit on the cache, otherwise, returns false.
        /// </summary>
        private bool UpdateCacheSizeExceedsCapacity(CacheEntry<K, V> entry, CoherentState<K, V> coherentState)
        {
            long sizeLimit = _options.SizeLimitValue;
            if (sizeLimit < 0)
            {
                return false;
            }

            long sizeRead = coherentState.Size;
            for (int i = 0; i < 100; i++)
            {
                long newSize = sizeRead + entry.Size;

                if ((ulong)newSize > (ulong)sizeLimit)
                {
                    // Overflow occurred, return true without updating the cache size
                    return true;
                }

                long original = Interlocked.CompareExchange(ref coherentState._cacheSize, newSize, sizeRead);
                if (sizeRead == original)
                {
                    return false;
                }
                sizeRead = original;
            }

            return true;
        }

        private void TriggerOvercapacityCompaction()
        {
            if (_logger.IsEnabled(LogLevel.Debug))
                _logger.LogDebug("Overcapacity compaction triggered");

            OvercapacityCompaction();
        }

        private void OvercapacityCompaction()
        {
            CoherentState<K, V> coherentState = _coherentState; // Clear() can update the reference in the meantime
            long currentSize = coherentState.Size;

            if (_logger.IsEnabled(LogLevel.Debug))
                _logger.LogDebug($"Overcapacity compaction executing. Current size {currentSize}");

            long sizeLimit = _options.SizeLimitValue;
            if (sizeLimit >= 0)
            {
                long lowWatermark = sizeLimit - (long)(sizeLimit * _options.CompactionPercentage);
                if (currentSize > lowWatermark)
                {
                    Compact(currentSize - (long)lowWatermark, entry => entry.Size, coherentState);
                }
            }

            if (_logger.IsEnabled(LogLevel.Debug))
                _logger.LogDebug($"Overcapacity compaction executed. New size {coherentState.Size}");
        }

        /// Remove at least the given percentage (0.10 for 10%) of the total entries (or estimated memory?), according to the following policy:
        /// 1. Remove all expired items.
        /// 2. Bucket by CacheItemPriority.
        /// 3. Least recently used objects.
        /// ?. Items with the soonest absolute expiration.
        /// ?. Items with the soonest sliding expiration.
        /// ?. Larger objects - estimated by object graph size, inaccurate.
        public void Compact(double percentage)
        {
            CoherentState<K, V> coherentState = _coherentState; // Clear() can update the reference in the meantime
            int removalCountTarget = (int)(coherentState.Count * percentage);
            Compact(removalCountTarget, _ => 1, coherentState);
        }

        private void Compact(long removalSizeTarget, Func<CacheEntry<K, V>, long> computeEntrySize, CoherentState<K, V> coherentState)
        {
            var entriesToRemove = new List<CacheEntry<K, V>>();
            // cache LastAccessed outside of the CacheEntry so it is stable during compaction
            var lowPriEntries = new List<(CacheEntry<K, V> entry, DateTimeOffset lastAccessed)>();
            var normalPriEntries = new List<(CacheEntry<K, V> entry, DateTimeOffset lastAccessed)>();
            var highPriEntries = new List<(CacheEntry<K, V> entry, DateTimeOffset lastAccessed)>();
            long removedSize = 0;

            // Sort items by expired & priority status
            DateTime utcNow = UtcNow;
            foreach (KeyValuePair<K, CacheEntry<K, V>> item in coherentState._entries)
            {
                CacheEntry<K, V> entry = item.Value;
                if (entry.CheckExpired(utcNow))
                {
                    entriesToRemove.Add(entry);
                    removedSize += computeEntrySize(entry);
                }
                else
                {
                    switch (entry.Priority)
                    {
                        case CacheItemPriority.Low:
                            lowPriEntries.Add((entry, entry.LastAccessed));
                            break;
                        case CacheItemPriority.Normal:
                            normalPriEntries.Add((entry, entry.LastAccessed));
                            break;
                        case CacheItemPriority.High:
                            highPriEntries.Add((entry, entry.LastAccessed));
                            break;
                        case CacheItemPriority.NeverRemove:
                            break;
                        default:
                            throw new NotSupportedException("Not implemented: " + entry.Priority);
                    }
                }
            }

            ExpirePriorityBucket(ref removedSize, removalSizeTarget, computeEntrySize, entriesToRemove, lowPriEntries);
            ExpirePriorityBucket(ref removedSize, removalSizeTarget, computeEntrySize, entriesToRemove, normalPriEntries);
            ExpirePriorityBucket(ref removedSize, removalSizeTarget, computeEntrySize, entriesToRemove, highPriEntries);

            foreach (CacheEntry<K, V> entry in entriesToRemove)
            {
                coherentState.RemoveEntry(entry, _options);
            }

            // Policy:
            // 1. Least recently used objects.
            // ?. Items with the soonest absolute expiration.
            // ?. Items with the soonest sliding expiration.
            // ?. Larger objects - estimated by object graph size, inaccurate.
            static void ExpirePriorityBucket(ref long removedSize, long removalSizeTarget, Func<CacheEntry<K, V>, long> computeEntrySize, List<CacheEntry<K, V>> entriesToRemove, List<(CacheEntry<K, V> Entry, DateTimeOffset LastAccessed)> priorityEntries)
            {
                // Do we meet our quota by just removing expired entries?
                if (removalSizeTarget <= removedSize)
                {
                    // No-op, we've met quota
                    return;
                }

                // Expire enough entries to reach our goal
                // TODO: Refine policy

                // LRU
                priorityEntries.Sort(static (e1, e2) => e1.LastAccessed.CompareTo(e2.LastAccessed));
                foreach ((CacheEntry<K, V> entry, _) in priorityEntries)
                {
                    entry.SetExpired(EvictionReason.Capacity);
                    entriesToRemove.Add(entry);
                    removedSize += computeEntrySize(entry);

                    if (removalSizeTarget <= removedSize)
                    {
                        break;
                    }
                }
            }
        }

        /// <inheritdoc />
        public void Dispose()
        {
            Dispose(true);
        }

        /// <summary>
        /// Dispose the cache and clear all entries.
        /// </summary>
        /// <param name="disposing">Dispose the object resources if true; otherwise, take no action.</param>
        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    _stats?.Dispose();
                    GC.SuppressFinalize(this);
                }

                _disposed = true;
            }
        }

        private void CheckDisposed()
        {
            if (_disposed)
            {
                Throw();
            }

            static void Throw() => throw new ObjectDisposedException(typeof(MemoryCache<K, V>).FullName);
        }

        private static void ValidateCacheKey(object key)
        {
            if (key == null)
                throw new ArgumentException();
        }

        /// <summary>
        /// Wrapper for the memory cache entries collection.
        ///
        /// Entries may have various sizes. If a size limit has been set, the cache keeps track of the aggregate of all the entries' sizes
        /// in order to trigger compaction when the size limit is exceeded.
        ///
        /// For performance reasons, the size is not updated atomically with the collection, but is only made eventually consistent.
        ///
        /// When the memory cache is cleared, it replaces the backing collection entirely. This may occur in parallel with operations
        /// like add, set, remove, and compact which may modify the collection and thus its overall size.
        ///
        /// To keep the overall size eventually consistent, therefore, the collection and the overall size are wrapped in this CoherentState
        /// object. Individual operations take a local reference to this wrapper object while they work, and make size updates to this object.
        /// Clearing the cache simply replaces the object, so that any still in progress updates do not affect the overall size value for
        /// the new backing collection.
        /// </summary>
        private sealed class CoherentState<K, V>
        {
            internal SortedDictionary<K, CacheEntry<K, V>> _entries = new();

            internal long _cacheSize;

            private ICollection<KeyValuePair<K, CacheEntry<K, V>>> EntriesCollection => _entries;

            internal int Count => _entries.Count;

            internal long Size => Volatile.Read(ref _cacheSize);

            internal void RemoveEntry(CacheEntry<K, V> entry, MemoryCacheOptions options)
            {
                if (EntriesCollection.Remove(new KeyValuePair<K, CacheEntry<K, V>>(entry.Key, entry)))
                {
                    if (options.SizeLimit.HasValue)
                    {
                        Interlocked.Add(ref _cacheSize, -entry.Size);
                    }
                    entry.InvokeEvictionCallbacks();
                }
            }
        }
    }
}
