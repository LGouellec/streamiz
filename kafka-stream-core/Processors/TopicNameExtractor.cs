using System;

namespace kafka_stream_core.Processors
{
    public interface TopicNameExtractor
    {
        String extract(object key, object value, RecordContext recordContext);
    }

    public interface TopicNameExtractor<K, V> : TopicNameExtractor
    {
        String extract(K key, V value, RecordContext recordContext);
    }
}
