using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.State.Enumerator;
using Streamiz.Kafka.Net.State.Internal;
using Streamiz.Kafka.Net.State.RocksDb.Internal;
using System;

namespace Streamiz.Kafka.Net.State.RocksDb
{
    internal class RocksDbWindowStore
        : WrappedStateStore<RocksDbSegmentedBytesStore>, IWindowStore<Bytes, byte[]>
    {
        private readonly int seqnum = 0;
        private readonly long windowSize;

        public RocksDbWindowStore(
                    RocksDbSegmentedBytesStore wrapped,
                    long windowSize) 
            : base(wrapped)
        {
            this.windowSize = windowSize;
        }

        public IKeyValueEnumerator<Windowed<Bytes>, byte[]> All()
        {
            var enumerator = wrapped.All();
            return (new WindowStoreEnumeratorWrapper(enumerator, windowSize)).ToKeyValueEnumerator();
        }

        public byte[] Fetch(Bytes key, long time)
            => wrapped.Get(RocksDbWindowKeySchema.ToStoreKeyBinary(key, time, seqnum));

        public IWindowStoreEnumerator<byte[]> Fetch(Bytes key, DateTime from, DateTime to)
            => Fetch(key, from.GetMilliseconds(), to.GetMilliseconds());

        public IWindowStoreEnumerator<byte[]> Fetch(Bytes key, long from, long to)
        {
            var enumerator = wrapped.Fetch(key, from, to);
            return (new WindowStoreEnumeratorWrapper(enumerator, windowSize)).ToWindowStoreEnumerator();
        }

        public IKeyValueEnumerator<Windowed<Bytes>, byte[]> FetchAll(DateTime from, DateTime to)
        {
            var enumerator = wrapped.FetchAll(from.GetMilliseconds(), to.GetMilliseconds());
            return (new WindowStoreEnumeratorWrapper(enumerator, windowSize)).ToKeyValueEnumerator();
        }

        public void Put(Bytes key, byte[] value, long windowStartTimestamp)
            => wrapped.Put(RocksDbWindowKeySchema.ToStoreKeyBinary(key, windowStartTimestamp, seqnum), value);
    }
}