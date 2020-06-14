using log4net;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.State.Enumerator;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

namespace Streamiz.Kafka.Net.State.InMemory
{
    #region InMemeory Iterator

    internal abstract class InMemoryWindowStoreIteratorWrapper
    {
        private readonly List<KeyValuePair<long, ConcurrentDictionary<Bytes, byte[]>>> iterator;
        private readonly KeyValuePair<Bytes, byte[]> next;
        private readonly Bytes key;
        private readonly Func<InMemoryWindowStoreIteratorWrapper, bool> closingCallback;

        public long CurrentTime { get; private set; }

        public InMemoryWindowStoreIteratorWrapper(
            Bytes key,
            List<KeyValuePair<long, ConcurrentDictionary<Bytes, byte[]>>> dataIterator,
            Func<InMemoryWindowStoreIteratorWrapper, bool> closingCallback)
        {
            this.key = key;
            iterator = dataIterator;
            this.closingCallback = closingCallback;
        }

        public void Close()
        {
            iterator.Clear();
            closingCallback.Invoke(this);
        }

        public void RemoveExpiredData(long time)
        {
            iterator.RemoveAll((k) => k.Key < time);
        }
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
            return null;
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
            return null;
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
            foreach(var it in openIterators)
            {
                minLiveTime = Math.Min(minLiveTime, it.CurrentTime);
                it.RemoveExpiredData(minLiveTime);
            }
            
            foreach(var k in map.Keys.ToList())
            {
                if (k < minLiveTime)
                    map.Remove(k, out tmp);
            }
        }

        private List<KeyValuePair<long, ConcurrentDictionary<Bytes, byte[]>>> SubMap(long time1, long time2)
            => map.Where(kv => time1 >= kv.Key && time2 <= kv.Key).ToList();

        private IWindowStoreEnumerator<byte[]> CreateNewWindowStoreIterator(Bytes key, List<KeyValuePair<long, ConcurrentDictionary<Bytes, byte[]>>> enumerator)
        {
            throw new NotImplementedException();
        }

        #endregion
    }
}
