using System;
using System.Collections.Generic;
using System.Text;

namespace Streamiz.Kafka.Net.Mock
{
    internal class TestRecord<K, V>
    {
        public K Key { get; set; }
        public V Value { get; set; }
        public DateTime? Timestamp { get; set; }
    }
}
