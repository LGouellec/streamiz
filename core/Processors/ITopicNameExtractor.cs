using Kafka.Streams.Net.Processors.Internal;
using System;

namespace Kafka.Streams.Net.Processors
{
    public interface ITopicNameExtractor<K, V>
    {
        String Extract(K key, V value, IRecordContext recordContext);
    }
}
