using System;

namespace Streamiz.Kafka.Net.Mock
{
    public class TestRecord<K, V>
    {
        public K Key { get; set; }
        public V Value { get; set; }
        public DateTime? Timestamp { get; set; }

        public TestRecord()
        {

        }

        public TestRecord(K key, V value, DateTime? timestamp)
        {
            Key = key;
            Value = value;
            Timestamp = timestamp;
        }

        public TestRecord(K key, V value)
        {
            Key = key;
            Value = value;
        }
    }
}
