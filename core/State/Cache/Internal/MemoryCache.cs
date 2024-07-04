// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// This is a fork from Microsoft.Extensions.Caching.Memory.MemoryCache https://github.com/dotnet/runtime/blob/main/src/libraries/Microsoft.Extensions.Caching.Memory
// The difference is the compaction process and eviction callback is synchronous whereas the .NET repo is asynchronous

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Streamiz.Kafka.Net.Crosscutting;

namespace Streamiz.Kafka.Net.State.Cache.Internal
{
    /// <summary>
    /// An implementation of <see cref="IMemoryCache{K, V}"/> using a dictionary to
    /// store its entries.
    /// </summary>
    internal sealed class MemoryCache<K, V> : IMemoryCache<K, V>
        where K : class
        where V : class
    {
        private readonly IComparer<K> _keyComparer;
        private readonly IClockTime _clockTime;
        internal readonly ILogger Logger;
        
        private readonly MemoryCacheOptions _options;

        internal long Capacity => _options.SizeLimit;

        private readonly List<WeakReference<Stats>> _allStats;
        private readonly Stats _accumulatedStats;
        private readonly ThreadLocal<Stats> _stats;
        private CoherentState<K, V> _coherentState;
        private bool _disposed;

        /// <summary>
        /// Creates a new <see cref="MemoryCache{K, V}"/> instance.
        /// </summary>
        /// <param name="optionsAccessor">The options of the cache.</param>
        /// <param name="keyComparer">Compare the key</param>
        public MemoryCache(IOptions<MemoryCacheOptions> optionsAccessor, IComparer<K> keyComparer)
            : this(optionsAccessor, keyComparer, NullLoggerFactory.Instance, new ClockSystemTime())
        {
        }
        
        /// <summary>
        /// Creates a new <see cref="MemoryCache{K, V}"/> instance.
        /// </summary>
        /// <param name="optionsAccessor">The options of the cache.</param>
        /// <param name="keyComparer">Compare the key</param>
        public MemoryCache(IOptions<MemoryCacheOptions> optionsAccessor, IComparer<K> keyComparer, IClockTime clockTime)
            : this(optionsAccessor, keyComparer, NullLoggerFactory.Instance, clockTime)
        {
        }

        /// <summary>
        /// Creates a new <see cref="MemoryCache{K, V}"/> instance.
        /// </summary>
        /// <param name="optionsAccessor">The options of the cache.</param>
        /// <param name="keyComparer">Compare the key</param>
        /// <param name="loggerFactory">The factory used to create loggers.</param>
        /// <param name="clockTime">Clock time accessor</param>
        private MemoryCache(
            IOptions<MemoryCacheOptions> optionsAccessor,
            IComparer<K> keyComparer,
            ILoggerFactory loggerFactory,
            IClockTime clockTime)
        {
            Utils.CheckIfNotNull(optionsAccessor, nameof(optionsAccessor));
            Utils.CheckIfNotNull(loggerFactory, nameof(loggerFactory));
            Utils.CheckIfNotNull(keyComparer, nameof(keyComparer));
            
            _keyComparer = keyComparer;
            _clockTime = clockTime;
            _options = optionsAccessor.Value;
            Logger = loggerFactory.CreateLogger<MemoryCache<K, V>>();

            _coherentState = new CoherentState<K, V>();

            _allStats = new List<WeakReference<Stats>>();
            _accumulatedStats = new Stats();
            _stats = new ThreadLocal<Stats>(() => new Stats(this));
        }

        private DateTime UtcNow => _clockTime.GetCurrentTime();

        /// <summary>
        /// Cleans up the background collection events.
        /// </summary>
        ~MemoryCache() => Dispose(false);

        /// <summary>
        /// Gets the count of the current entries for diagnostic purposes.
        /// </summary>
        public int Count => _coherentState.Count;

        /// <summary>
        /// Gets an enumerable of the all the keys in the <see cref="MemoryCache{K, V}"/>.
        /// </summary>
        private IEnumerable<K> Keys => _coherentState.Entries.Keys;

        internal IEnumerable<K> KeySetEnumerable(bool forward)
        {
            return forward ? Keys : Keys.OrderByDescending(k => k, _keyComparer);
        }

        // Maybe used Keys accessor ..
        internal IEnumerable<K> KeyRange(K from, K to, bool inclusive, bool forward)
        {
            if (from == null && to == null)
                return KeySetEnumerable(forward);
            
            IEnumerable<K> selectedKeys;

            if (from == null)
                selectedKeys = _coherentState.Entries
                    .HeadMap(to, inclusive)
                    .Select(kv => kv.Key);
            else if (to == null)
                selectedKeys = _coherentState.Entries
                    .TailMap(from, true)
                    .Select(kv => kv.Key);
            else if (_keyComparer.Compare(from, to) > 0)
                selectedKeys = new List<K>();
            else
                selectedKeys = _coherentState.Entries
                    .SubMap(from, to, true, inclusive)
                    .Select(kv => kv.Key);
                
            return forward ? selectedKeys : selectedKeys.OrderByDescending(k => k, _keyComparer);
        }

        /// <summary>
        /// Internal accessor for Size for testing only.
        ///
        /// Note that this is only eventually consistent with the contents of the collection.
        /// See comment on <see cref="CoherentState{K, V}"/>.
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

            DateTime utcNow = UtcNow;

            // Initialize the last access timestamp at the time the entry is added
            entry.LastAccessed = utcNow;

            CoherentState<K, V> coherentState = _coherentState; // Clear() can update the reference in the meantime
            if (coherentState.Entries.TryGetValue(entry.Key, out CacheEntry<K, V> priorEntry))
                priorEntry.SetExpired(EvictionReason.Replaced);

            if (!UpdateCacheSizeExceedsCapacity(entry, coherentState))
            {
                if (priorEntry == null)
                {
                    // Try to add the new entry if no previous entries exist.
                    coherentState.Entries.TryAdd(entry.Key, entry);
                }
                else
                {
                    // Try to update with the new entry if a previous entries exist.
                    coherentState.Entries.AddOrUpdate(entry.Key, entry);
                    // The prior entry was removed, decrease the by the prior entry's size
                    Interlocked.Add(ref coherentState.CacheSize, -priorEntry.Size);
                }
                
                priorEntry?.InvokeEvictionCallbacks();
            }
            else
            {
                TriggerOvercapacityCompaction();
                coherentState.Entries.TryAdd(entry.Key, entry);
                Interlocked.Add(ref coherentState.CacheSize, entry.Size);
            }
        }

        /// <inheritdoc />
        public bool TryGetValue(K key, out V value)
        {
            if (key == null)
                throw new ArgumentException();

            CheckDisposed();

            DateTime utcNow = UtcNow;

            CoherentState<K, V> coherentState = _coherentState; // Clear() can update the reference in the meantime
            if (coherentState.Entries.TryGetValue(key, out CacheEntry<K, V> tmp))
            {
                CacheEntry<K, V> entry = tmp;
                entry.LastAccessed = utcNow;
                value = entry.Value;
                
                // Hit
                lock (_allStats!)
                {
                    if (_allStats is not null)
                    {
                        if (IntPtr.Size == 4)
                            Interlocked.Increment(ref GetStats().Hits);
                        else
                            GetStats().Hits++;
                    }
                }

                return true;
            }

            value = default(V);
            // Miss
            lock (_allStats!)
            {
                if (_allStats is not null)
                {
                    if (IntPtr.Size == 4)
                        Interlocked.Increment(ref GetStats().Misses);
                    else
                        GetStats().Misses++;
                }
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
            if (coherentState.Entries.Remove(key, out CacheEntry<K, V> entry))
            {
                Interlocked.Add(ref coherentState.CacheSize, -entry.Size);
                entry.SetExpired(EvictionReason.Removed);
                entry.InvokeEvictionCallbacks();
            }
        }

        /// <summary>
        /// Removes all keys and values from the cache.
        /// </summary>
        public void Clear()
        {
            CheckDisposed();

            CoherentState<K, V> oldState = Interlocked.Exchange(ref _coherentState, new CoherentState<K, V>());
            foreach (KeyValuePair<K, CacheEntry<K, V>> entry in oldState.Entries)
            {
                entry.Value.SetExpired(EvictionReason.Removed);
                entry.Value.InvokeEvictionCallbacks();
            }
        }

        /// <summary>
        /// Gets a snapshot of the current statistics for the memory cache.
        /// </summary>
        /// <returns>Returns statistics tracked.</returns>
        public MemoryCacheStatistics GetCurrentStatistics()
        {
            lock (_allStats!)
            {
                if (_allStats is not null)
                {
                    (long hit, long miss) sumTotal = Sum();
                    return new MemoryCacheStatistics()
                    {
                        TotalMisses = sumTotal.miss,
                        TotalHits = sumTotal.hit,
                        CurrentEntryCount = Count,
                        CurrentEstimatedSize = Size
                    };
                }
            }

            return null;
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

        private sealed class Stats
        {
            private readonly MemoryCache<K, V> _memoryCache;
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

        /// <summary>
        /// Returns true if increasing the cache size by the size of entry would
        /// cause it to exceed any size limit on the cache, otherwise, returns false.
        /// </summary>
        private bool UpdateCacheSizeExceedsCapacity(CacheEntry<K, V> entry, CoherentState<K, V> coherentState)
        {
            long sizeRead = coherentState.Size;
            for (int i = 0; i < 100; i++)
            {
                long newSize = sizeRead + entry.Size;

                if ((ulong)newSize > (ulong)_options.SizeLimit)
                {
                    // Overflow occurred, return true without updating the cache size
                    return true;
                }

                long original = Interlocked.CompareExchange(ref coherentState.CacheSize, newSize, sizeRead);
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
            if (Logger.IsEnabled(LogLevel.Debug))
                Logger.LogDebug("Overcapacity compaction triggered");

            OvercapacityCompaction();
        }

        private void OvercapacityCompaction()
        {
            CoherentState<K, V> coherentState = _coherentState; // Clear() can update the reference in the meantime
            long currentSize = coherentState.Size;

            if (Logger.IsEnabled(LogLevel.Debug))
                Logger.LogDebug($"Overcapacity compaction executing. Current size {currentSize}");

            long sizeLimit = _options.SizeLimit;
            if (sizeLimit >= 0)
            {
                long lowWatermark = sizeLimit - (long)(sizeLimit * _options.CompactionPercentage);
                if (currentSize > lowWatermark)
                {
                    Compact(currentSize - (long)lowWatermark, entry => entry.Size, coherentState);
                }
            }

            if (Logger.IsEnabled(LogLevel.Debug))
                Logger.LogDebug($"Overcapacity compaction executed. New size {coherentState.Size}");
        }

        /// Remove at least the given percentage (0.10 for 10%) of the total entries (or estimated memory?), according to the following policy:
        /// 1. Remove all items if the percentage is more than 100%
        /// 2. Least recently used objects.
        public void Compact(double percentage)
        {
            if (percentage >= 1) // clear all the cache
                Flush();
            else
            {
                CoherentState<K, V> coherentState = _coherentState; // Clear() can update the reference in the meantime
                int removalCountTarget = (int)(coherentState.Count * percentage);
                Compact(removalCountTarget, _ => 1, coherentState);
            }
        }

        private void Flush()
        {
            var entriesToRemove = new List<CacheEntry<K, V>>();
            using var enumerator = _coherentState
                .Entries
                .OrderBy(e => e.Value.LastAccessed)
                .GetEnumerator();
            
            while (enumerator.MoveNext())
                entriesToRemove.Add(enumerator.Current.Value);
            
            foreach (CacheEntry<K, V> entry in entriesToRemove)
                _coherentState.RemoveEntry(entry, _options);
        }

        private void Compact(long removalSizeTarget, Func<CacheEntry<K, V>, long> computeEntrySize, CoherentState<K, V> coherentState)
        {
            var entriesToRemove = new List<CacheEntry<K, V>>();
            long removedSize = 0;

            ExpireLruBucket(ref removedSize, removalSizeTarget, computeEntrySize, entriesToRemove, coherentState.Entries);

            foreach (CacheEntry<K, V> entry in entriesToRemove)
                coherentState.RemoveEntry(entry, _options);

            // Expire the least recently used objects.
            static void ExpireLruBucket(ref long removedSize, long removalSizeTarget, Func<CacheEntry<K, V>, long> computeEntrySize, List<CacheEntry<K, V>> entriesToRemove, SortedDictionary<K, CacheEntry<K, V>> priorityEntries)
            {
                // Do we meet our quota by just removing expired entries?
                if (removalSizeTarget <= removedSize)
                {
                    // No-op, we've met quota
                    return;
                }

                // LRU
                foreach (var keyValuePair in priorityEntries.OrderBy(e => e.Value.LastAccessed))
                {
                    keyValuePair.Value.SetExpired(EvictionReason.Capacity);
                    entriesToRemove.Add(keyValuePair.Value);
                    removedSize += computeEntrySize(keyValuePair.Value);

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
        private void Dispose(bool disposing)
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
            where K : class
            where V : class
        {
            internal readonly SortedDictionary<K, CacheEntry<K, V>> Entries = new();
            internal long CacheSize;

            private ICollection<KeyValuePair<K, CacheEntry<K, V>>> EntriesCollection => Entries;

            internal int Count => Entries.Count;

            internal long Size => Volatile.Read(ref CacheSize);

            internal void RemoveEntry(CacheEntry<K, V> entry, MemoryCacheOptions options)
            {
                if (EntriesCollection.Remove(new KeyValuePair<K, CacheEntry<K, V>>(entry.Key, entry)))
                {
                    Interlocked.Add(ref CacheSize, -entry.Size);
                    entry.InvokeEvictionCallbacks();
                }
            }
        }
    }
}
