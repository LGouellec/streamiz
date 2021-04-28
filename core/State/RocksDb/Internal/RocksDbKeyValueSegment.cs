using Streamiz.Kafka.Net.State.Internal;
using System;

namespace Streamiz.Kafka.Net.State.RocksDb.Internal
{
    internal class RocksDbKeyValueSegment
        : RocksDbKeyValueStore, IComparable<RocksDbKeyValueSegment>, ISegment
    {
        private readonly long id;

        public RocksDbKeyValueSegment(string segmentName, string windowName, long id)
            : base(segmentName, windowName)
        {
            this.id = id;
        }

        internal void OpenDB(ProcessorContext context)
            => OpenDatabase(context);

        public int CompareTo(RocksDbKeyValueSegment other)
            => id.CompareTo(other.id);

        public void Destroy()
            => DbDir.Delete(true);
    }
}
