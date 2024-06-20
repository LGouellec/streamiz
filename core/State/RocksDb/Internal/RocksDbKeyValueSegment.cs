using Streamiz.Kafka.Net.State.Internal;
using System;

namespace Streamiz.Kafka.Net.State.Internal
{
    internal class RocksDbKeyValueSegment
        : RocksDbKeyValueStore, IComparable<RocksDbKeyValueSegment>, ISegment
    {
        private readonly long id;

        public RocksDbKeyValueSegment(string segmentName, string windowName, long id)
            : base(segmentName, windowName)
        {
            this.id = id;
            KeyComparator = CompareSegmentedKey;
        }

        internal void OpenDB(ProcessorContext context)
            => OpenDatabase(context);

        public int CompareTo(RocksDbKeyValueSegment other)
            => id.CompareTo(other.id);

        public void Destroy()
            => DbDir.Delete(true);

        protected int CompareSegmentedKey(byte[] key1, byte[] key2)
        {
            var comparer = new WindowKeyBytesComparer();
            var k1 = WindowKeyBytes.Wrap(key1);
            var k2 = WindowKeyBytes.Wrap(key2);
            return comparer.Compare(k1, k2);
        }
    }
}
