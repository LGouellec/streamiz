using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using Confluent.Kafka;

namespace Streamiz.Kafka.Net.Stream
{
    public class ExternalRecord<K, V>
    {
        public K Key { get; }
        public V Value { get; }
        public IReadOnlyCollection<IHeader> Headers { get; }
        public long Timestamp { get; }

        public ExternalRecord(
            K key,
            V value,
            Headers headers,
            long timestamp)
        {
            Value = value;
            Timestamp = timestamp;
            Key = key;
            Headers = new ReadOnlyCollection<IHeader>(headers != null ? headers.ToList() : new List<IHeader>());
        }
    }
}