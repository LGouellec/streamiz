using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.State
{
    public class ValueAndTimestamp<V>
    {
        public V Value { get; private set; }
        public long Timestamp { get; private set; }

        private ValueAndTimestamp(long timestamp, V value)
        {
            this.Timestamp = timestamp;
            this.Value = value;
        }

        public static ValueAndTimestamp<V> make(V value, long timestamp)
        {
            return value == null ? null : new ValueAndTimestamp<V>(timestamp, value);
        }
    }
}
