using System;

namespace kafka_stream_core.Processors.Internal
{
    public abstract class AbstractTopicNameExtractor<K, V> : TopicNameExtractor<K, V>
    {
        public abstract string extract(K key, V value, RecordContext recordContext);

        public string extract(object key, object value, RecordContext recordContext)
        {
            if ((key == null || key is K) && (value == null || value is V))
                return this.extract((K)key, (V)value, recordContext);
            else
                throw new InvalidOperationException($"Impossible to extract topic name with [Key type {key.GetType().Name}|Value type {value.GetType().Name}] with {this.GetType().Name}<{typeof(K).Name},{typeof(V).Name}>");
        }
    }
}
