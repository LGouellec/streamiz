using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.State.Enumerator;
using Streamiz.Kafka.Net.State.Helper;
using Streamiz.Kafka.Net.State.Internal;
using Streamiz.Kafka.Net.State.RocksDb.Internal;
using System;

namespace Streamiz.Kafka.Net.State.RocksDb
{
    internal class RocksDbWindowStore
        : WrappedStateStore<RocksDbSegmentedBytesStore>, IWindowStore<Bytes, byte[]>
    {
        private int seqnum = 0;
        private readonly long windowSize;
        private readonly bool retainDuplicates;

        public RocksDbWindowStore(
            RocksDbSegmentedBytesStore wrapped,
            long windowSize,
            bool retainDuplicates)
            : base(wrapped)
        {
            this.windowSize = windowSize;
            this.retainDuplicates = retainDuplicates;
        }

        private void UpdateSeqNumber()
        {
            if (retainDuplicates)
                seqnum = (seqnum + 1) & 0x7FFFFFFF;
        }

        public IKeyValueEnumerator<Windowed<Bytes>, byte[]> All()
        {
            var enumerator = wrapped.All();
            return (new WindowStoreEnumeratorWrapper(enumerator, windowSize)).ToKeyValueEnumerator();
        }

        public byte[] Fetch(Bytes key, long time)
        {
            // Make a test for that
            if (!retainDuplicates)
                return wrapped.Get(WindowKeyHelper.ToStoreKeyBinary(key, time, seqnum));
            
            using var enumerator = Fetch(key, time, time);
            if (enumerator.MoveNext())
                return enumerator.Current.Value.Value;
            return null;
        }

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
        {
            // Skip if value is null and duplicates are allowed since this delete is a no-op
            if (!(value == null && retainDuplicates))
            {
                UpdateSeqNumber();
                wrapped.Put(WindowKeyHelper.ToStoreKeyBinary(key, windowStartTimestamp, seqnum), value);
            }
        }
    }
}