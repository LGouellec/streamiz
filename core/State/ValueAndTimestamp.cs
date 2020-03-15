using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.State
{
    public class ValueAndTimestamp<V>
    {
        public V Value { get; private set; }
        public long Timestamp { get; private set; }

        public ValueAndTimestamp(long timestamp, V value)
        {
            this.Timestamp = timestamp;
            this.Value = value;
        }
    }
}
