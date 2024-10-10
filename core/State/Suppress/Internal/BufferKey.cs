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
    
    internal class BufferKey : IEquatable<BufferKey>, IComparable<BufferKey>
    {
        public long Time { get; }
        public Bytes Key { get; }

        public BufferKey(long time, Bytes key)
        {
            Time = time;
            Key = key;
        }

        public bool Equals(BufferKey other)
            => Time.Equals(other.Time) && Key.Equals(other.Key);

        public int CompareTo(BufferKey other)
        {
            var compared = other.Time.CompareTo(Time);
            return compared == 0 ? other.Key.CompareTo(Key) : compared;
        }
    }
}