using System;
using System.Collections;
using System.Collections.Generic;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.State.Enumerator;
using Streamiz.Kafka.Net.State.Helper;

namespace Streamiz.Kafka.Net.State.Cache.Enumerator
{
    internal class CacheEnumeratorWrapper : IKeyValueEnumerator<Bytes, CacheEntryValue>
    {
        private readonly bool forward;
        private readonly long timeTo;
        private readonly Bytes keyTo;
        private readonly CachingWindowStore _windowStore;
        private readonly Bytes keyFrom;
        private readonly long segmentInterval;
        
        private long lastSegmentId;
        private long currentSegmentId;
        private Bytes cacheKeyFrom;
        private Bytes cacheKeyTo;

        private IKeyValueEnumerator<Bytes, CacheEntryValue> current;
        
        public CacheEnumeratorWrapper(CachingWindowStore windowStore, Bytes key, long timeFrom, long timeTo, bool forward)
            : this(windowStore, key, key, timeFrom, timeTo, forward)
        {
        }

        public CacheEnumeratorWrapper(CachingWindowStore windowStore, Bytes keyFrom, Bytes keyTo, long timeFrom, long timeTo, bool forward)
        {
            _windowStore = windowStore;
            this.keyFrom = keyFrom;
            this.keyTo = keyTo;
            this.timeTo = timeTo;
            this.forward = forward;
            
            segmentInterval = windowStore.SegmentCacheFunction.SegmentInterval;

            if (forward)
            {
                lastSegmentId = windowStore.SegmentCacheFunction.SegmentId(Math.Min(timeTo, windowStore.MaxObservedTimestamp));
                currentSegmentId = windowStore.SegmentCacheFunction.SegmentId(timeFrom);

                SetCacheKeyRange(timeFrom, CurrentSegmentLastTime());
                current = windowStore.FetchInternal(cacheKeyFrom, cacheKeyTo, true);
            }
            else
            {
                currentSegmentId = windowStore.SegmentCacheFunction.SegmentId(Math.Min(timeTo, windowStore.MaxObservedTimestamp));
                lastSegmentId = windowStore.SegmentCacheFunction.SegmentId(timeFrom);

                SetCacheKeyRange(CurrentSegmentBeginTime(), Math.Min(timeTo, windowStore.MaxObservedTimestamp));
                current = windowStore.FetchInternal(cacheKeyFrom, cacheKeyTo, false);
            }
        }
        
        #region Private
        
        private void SetCacheKeyRange(long lowerRangeEndTime, long upperRangeEndTime) {
            
            if (_windowStore.SegmentCacheFunction.SegmentId(lowerRangeEndTime) != _windowStore.SegmentCacheFunction.SegmentId(upperRangeEndTime)) {
                throw new ArgumentException("Error iterating over segments: segment interval has changed");
            }

            if (keyFrom != null && keyTo != null && keyFrom.Equals(keyTo)) {
                cacheKeyFrom = _windowStore.SegmentCacheFunction.CacheKey(SegmentLowerRangeFixedSize(keyFrom, lowerRangeEndTime));
                cacheKeyTo = _windowStore.SegmentCacheFunction.CacheKey(SegmentUpperRangeFixedSize(keyTo, upperRangeEndTime));
            } else {
                cacheKeyFrom = keyFrom == null ? null :
                    _windowStore.SegmentCacheFunction.CacheKey(_windowStore.KeySchema.LowerRange(keyFrom, lowerRangeEndTime), currentSegmentId);
                cacheKeyTo = keyTo == null ? null :
                    _windowStore.SegmentCacheFunction.CacheKey(_windowStore.KeySchema.UpperRange(keyTo, timeTo), currentSegmentId);
            }
        }
        
        private Bytes SegmentLowerRangeFixedSize(Bytes key, long segmentBeginTime) {
            return WindowKeyHelper.ToStoreKeyBinary(key, Math.Max(0, segmentBeginTime), 0);
        }

        private Bytes SegmentUpperRangeFixedSize(Bytes key, long segmentEndTime) {
            return WindowKeyHelper.ToStoreKeyBinary(key, segmentEndTime, Int32.MaxValue);
        }
        
        private long CurrentSegmentBeginTime() {
            return currentSegmentId * segmentInterval;
        }

        private long CurrentSegmentLastTime() {
            return Math.Min(timeTo, CurrentSegmentBeginTime() + segmentInterval - 1);
        }

        private void GetNextSegmentIterator() {
            if (forward) {
                ++currentSegmentId;
                // updating as maxObservedTimestamp can change while iterating
                lastSegmentId =
                    _windowStore.SegmentCacheFunction.SegmentId(Math.Min(timeTo, _windowStore.MaxObservedTimestamp));

                if (currentSegmentId > lastSegmentId) {
                    current = null;
                    return;
                }

                SetCacheKeyRange(CurrentSegmentBeginTime(), CurrentSegmentLastTime());

                current.Dispose();

                current = _windowStore.FetchInternal(cacheKeyFrom, cacheKeyTo, true);
            } else {
                --currentSegmentId;

                // last segment id is stable when iterating backward, therefore no need to update
                if (currentSegmentId < lastSegmentId) {
                    current = null;
                    return;
                }

                SetCacheKeyRange(CurrentSegmentBeginTime(), CurrentSegmentLastTime());

                current.Dispose();

                current = _windowStore.FetchInternal(cacheKeyFrom, cacheKeyTo, false);
            }
        }
        
        #endregion
        
        #region IKeyValueEnumerator impl
        
        public Bytes PeekNextKey()
            => current.PeekNextKey();

        public bool MoveNext()
        {
            if (current == null) return false;

            if (current.MoveNext())
                return true;

            while (!current.MoveNext())
            {
                GetNextSegmentIterator();
                if (current == null)
                    return false;
            }

            return true;
        }

        public void Reset() => throw new NotImplementedException();

        public KeyValuePair<Bytes, CacheEntryValue>? Current => current?.Current;

        object IEnumerator.Current => Current;

        public void Dispose()
            => current?.Dispose();
        
        #endregion
    }
}