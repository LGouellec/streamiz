using System;
using System.Collections.Generic;
using Streamiz.Kafka.Net.Crosscutting;

namespace Streamiz.Kafka.Net.State.Suppress.Internal
{
    internal class BufferKeyComparer : IComparer<BufferKey>
    {
        public int Compare(BufferKey x, BufferKey y)
            => x.CompareTo(y);
    }
    
    internal class BufferKey : IComparable<BufferKey>
    {
        public long Time { get; }
        public Bytes Key { get; }

        public BufferKey(long time, Bytes key)
        {
            Time = time;
            Key = key;
        }

        public int CompareTo(BufferKey other)
        {
            var compared = Time.CompareTo(other.Time);
            return compared == 0 ? Key.CompareTo(other.Key) : compared;
        }
    }
}