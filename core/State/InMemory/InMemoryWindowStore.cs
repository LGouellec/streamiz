using log4net;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.State.Enumerator;
using Streamiz.Kafka.Net.Stream;
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

namespace Streamiz.Kafka.Net.State.InMemory
{
    #region InMemory Iterator

    internal abstract class InMemoryWindowStoreIteratorWrapper
    {
        private readonly List<KeyValuePair<long, ConcurrentDictionary<Bytes, byte[]>>> iterator;
        private List<KeyValuePair<Bytes, byte[]>> valueIterator;
        private readonly Bytes keyFrom;
        private readonly Bytes keyTo;
        private readonly Func<InMemoryWindowStoreIteratorWrapper, bool> closingCallback;
        private readonly bool allKeys;

        protected int indexIt = 0;
        protected int valueIndexIt = 0;
        protected KeyValuePair<Bytes, byte[]>? next;

        public long CurrentTime { get; private set; }

        public bool HasNext
        {
            get
            {
                //if (next != null)
                //    return true;

                if (valueIterator == null || (valueIndexIt >= valueIterator.Count && indexIt >= iterator.Count))
                    return false;

                next = Next;
                if (next == null)
                    return false;

                if (next.Value.Key.Equals(keyFrom) || next.Value.Key.Equals(keyTo))
                    return true;
                else
                {
                    next = null;
                    return HasNext;
                }
            }
        }

        protected KeyValuePair<Bytes, byte[]>? Next
        {
            get
            {
                while (valueIterator == null || valueIndexIt >= valueIterator.Count)
                {
                    valueIterator = SetRecordIterator();
                    if (valueIterator == null)
                        return null;
                    else
                        valueIndexIt = 0;
                }

                var e = valueIterator[valueIndexIt];
                ++valueIndexIt;
                return e;
            }
        }

        public InMemoryWindowStoreIteratorWrapper(
            Bytes keyFrom,
            Bytes keyTo,
            List<KeyValuePair<long, ConcurrentDictionary<Bytes, byte[]>>> dataIterator,
            Func<InMemoryWindowStoreIteratorWrapper, bool> closingCallback)
        {
            this.keyFrom = keyFrom;
            this.keyTo = keyTo;
            allKeys = keyFrom == null && keyTo == null;
            iterator = dataIterator;
            this.closingCallback = closingCallback;
        }

        public void Close()
        {
            iterator.Clear();
            valueIterator.Clear();
            closingCallback.Invoke(this);
        }

        public void RemoveExpiredData(long time)
        {
            iterator.RemoveAll((k) => k.Key < time);
        }

        private List<KeyValuePair<Bytes, byte[]>> SetRecordIterator()
        {
            if (indexIt >= iterator.Count)
                return null;

            var current = iterator[indexIt];
            CurrentTime = current.Key;

            if (allKeys)
                return new List<KeyValuePair<Bytes, byte[]>>(current.Value);
            else
                return new List<KeyValuePair<Bytes, byte[]>>(current.Value.Where(kv => kv.Key.Equals(keyFrom) || kv.Key.Equals(keyTo)));
        }

        protected void Reset()
        {
            indexIt = 0;
            valueIndexIt = 0;
            next = null;
            valueIterator = null;
        }
    }

    internal class WrappedInMemoryWindowStoreIterator : InMemoryWindowStoreIteratorWrapper, IWindowStoreEnumerator<byte[]>
    {
        public WrappedInMemoryWindowStoreIterator(Bytes keyFrom, Bytes keyTo, List<KeyValuePair<long, ConcurrentDictionary<Bytes, byte[]>>> dataIterator, Func<InMemoryWindowStoreIteratorWrapper, bool> closingCallback)
            : base(keyFrom, keyTo, dataIterator, closingCallback)
        {
        }

        public KeyValuePair<long, byte[]> Current => next.HasValue ? new KeyValuePair<long, byte[]>(CurrentTime, next.Value.Value) : default;

        object IEnumerator.Current => Current;

        public void Dispose() => base.Close();

        public bool MoveNext() => HasNext;

        public long PeekNextKey()
        {
            throw new NotImplementedException();
        }

        public void Reset() => base.Reset();
    }

    internal class WrappedWindowedKeyValueIterator : InMemoryWindowStoreIteratorWrapper, IKeyValueEnumerator<Windowed<Bytes>, byte[]>
    {
        private readonly long windowSize;

        public WrappedWindowedKeyValueIterator(Bytes keyFrom, Bytes keyTo, List<KeyValuePair<long, ConcurrentDictionary<Bytes, byte[]>>> dataIterator, Func<InMemoryWindowStoreIteratorWrapper, bool> closingCallback, long windowSize)
            : base(keyFrom, keyTo, dataIterator, closingCallback)
        {
            this.windowSize = windowSize;
        }

        public KeyValuePair<Windowed<Bytes>, byte[]> Current
        {
            get
            {
                if (next.HasValue)
                {
                    return new KeyValuePair<Windowed<Bytes>, byte[]>(GetWindowedKey(), next.Value.Value);
                }
                else
                {
                    return default;
                }
            }
        }

        private Windowed<Bytes> GetWindowedKey()
        {
            TimeWindow timeWindow = new TimeWindow(CurrentTime, CurrentTime + windowSize);
            return new Windowed<Bytes>(next.Value.Key, timeWindow);
        }

        object IEnumerator.Current => Current;

        public void Dispose() => base.Close();

        public bool MoveNext() => HasNext;

        public Windowed<Bytes> PeekNextKey()
        {
            throw new NotImplementedException();
        }

        public void Reset() => base.Reset();
    }

    #endregion


    internal class InMemoryWindowStore : WindowStore<Bytes, byte[]>
    {
        private readonly TimeSpan retention;
        private readonly long size;

        private long observedStreamTime = -1;

        private readonly ConcurrentDictionary<long, ConcurrentDictionary<Bytes, byte[]>> map =
            new ConcurrentDictionary<long, ConcurrentDictionary<Bytes, byte[]>>();

        private readonly ISet<InMemoryWindowStoreIteratorWrapper> openIterators = new HashSet<InMemoryWindowStoreIteratorWrapper>();

        private readonly ILog logger = Logger.GetLogger(typeof(InMemoryWindowStore));

        public InMemoryWindowStore(string storeName, TimeSpan retention, long size)
        {
            Name = storeName;
            this.retention = retention;
            this.size = size;
        }

        public string Name { get; }

        public bool Persistent => false;

        public bool IsOpen { get; private set; } = false;

        public IKeyValueEnumerator<Windowed<Bytes>, byte[]> All()
        {
            RemoveExpiredData();
            long minTime = observedStreamTime - (long)retention.TotalMilliseconds;
            return CreateNewWindowedKeyValueIterator(null, null, Tail(minTime));
        }

        public void Close()
        {
            if (openIterators.Count != 0)
            {
                logger.Warn($"Closing {openIterators.Count} open iterators for store {Name}");
                foreach (var it in openIterators)
                    it.Close();
            }

            map.Clear();
            IsOpen = false;
        }

        public byte[] Fetch(Bytes key, long time)
        {
            RemoveExpiredData();

            if (time <= observedStreamTime - retention.TotalMilliseconds)
                return null;

            if (map.ContainsKey(time))
                return map[time].Get(key);
            else
                return null;
        }

        public IWindowStoreEnumerator<byte[]> Fetch(Bytes key, DateTime from, DateTime to)
        {
            RemoveExpiredData();

            long minTime = Math.Max(from.GetMilliseconds(), observedStreamTime - (long)retention.TotalMilliseconds + 1);


            if (to.GetMilliseconds() < minTime)
            {
                // TODO : empty enumerator
                //return WrappedInMemoryWindowStoreIterator.emptyIterator();
                return null;
            }

            return CreateNewWindowStoreIterator(key, SubMap(minTime, to.GetMilliseconds()));
        }

        public IKeyValueEnumerator<Windowed<Bytes>, byte[]> FetchAll(DateTime from, DateTime to)
        {
            RemoveExpiredData();

            long minTime = Math.Max(from.GetMilliseconds(), observedStreamTime - (long)retention.TotalMilliseconds + 1);

            if (to.GetMilliseconds() < minTime)
            {
                return null;
                // TODO
                //return KeyValueIterators.emptyIterator();
            }

            return CreateNewWindowedKeyValueIterator(null, null, SubMap(minTime, to.GetMilliseconds()));
        }

        public void Flush()
        {
        }

        public void Init(ProcessorContext context, IStateStore root)
        {
            if (root != null)
            {
                // register the store
                context.Register(root,
                    (key, value) => Put(key, value, context.Timestamp));
            }

            IsOpen = true;
        }

        public void Put(Bytes key, byte[] value, long windowStartTimestamp)
        {
            RemoveExpiredData();

            observedStreamTime = Math.Max(observedStreamTime, windowStartTimestamp);

            if (windowStartTimestamp <= observedStreamTime - retention.TotalMilliseconds)
            {
                logger.Warn("Skipping record for expired segment.");
            }
            else
            {
                if (value != null)
                {
                    map.AddOrUpdate(windowStartTimestamp,
                        (k) =>
                        {
                            var dic = new ConcurrentDictionary<Bytes, byte[]>();
                            dic.AddOrUpdate(key, (b) => value, (k, d) => value);
                            return dic;
                        },
                        (k, d) =>
                        {
                            d.AddOrUpdate(key, (b) => value, (k, d) => value);
                            return d;
                        });
                }
                else
                {
                    if (map.ContainsKey(windowStartTimestamp))
                    {
                        ConcurrentDictionary<Bytes, byte[]> tmp = null;
                        byte[] d = null;
                        map[windowStartTimestamp].Remove(key, out d);
                        if (map[windowStartTimestamp].Count == 0)
                            map.Remove(windowStartTimestamp, out tmp);
                    }
                }
            }
        }

        #region Private

        private void RemoveExpiredData()
        {
            ConcurrentDictionary<Bytes, byte[]> tmp;
            long minLiveTime = Math.Max(0L, observedStreamTime - (long)retention.TotalMilliseconds + 1);
            foreach (var it in openIterators)
            {
                minLiveTime = Math.Min(minLiveTime, it.CurrentTime);
                it.RemoveExpiredData(minLiveTime);
            }

            foreach (var k in map.Keys.ToList())
            {
                if (k < minLiveTime)
                    map.Remove(k, out tmp);
            }
        }

        private List<KeyValuePair<long, ConcurrentDictionary<Bytes, byte[]>>> SubMap(long time1, long time2)
            => map.Where(kv => time1 >= kv.Key && time2 <= kv.Key).ToList();

        private List<KeyValuePair<long, ConcurrentDictionary<Bytes, byte[]>>> Tail(long time)
            => map.Where(kv => kv.Key < time).ToList();

        private IWindowStoreEnumerator<byte[]> CreateNewWindowStoreIterator(Bytes key, List<KeyValuePair<long, ConcurrentDictionary<Bytes, byte[]>>> enumerator)
        {
            var it = new WrappedInMemoryWindowStoreIterator(key, key, enumerator, openIterators.Remove);
            openIterators.Add(it);
            return it;
        }

        private WrappedWindowedKeyValueIterator CreateNewWindowedKeyValueIterator(Bytes keyFrom, Bytes keyTo, List<KeyValuePair<long, ConcurrentDictionary<Bytes, byte[]>>> enumerator)
        {
            var it =
                  new WrappedWindowedKeyValueIterator(keyFrom,
                                                      keyTo,
                                                      enumerator,
                                                      openIterators.Remove,
                                                      size);
            openIterators.Add(it);
            return it;
        }

        #endregion
    }
}