using Streamiz.Kafka.Net.State.Internal;
using System;
using System.Collections.Generic;
using System.Text;

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

        public int CompareTo(RocksDbKeyValueSegment other)
        {
            throw new NotImplementedException();
        }

        public void Destroy()
        {
            throw new NotImplementedException();
        }
    }
}
