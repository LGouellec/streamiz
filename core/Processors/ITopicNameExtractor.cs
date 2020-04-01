using kafka_stream_core.Processors.Internal;
using System;

namespace kafka_stream_core.Processors
{
    public interface ITopicNameExtractor<K, V>
    {
        String Extract(K key, V value, IRecordContext recordContext);
    }
}
