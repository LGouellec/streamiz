namespace kafka_stream_core.Processors.Internal
{
    public class StaticTopicNameExtractor : TopicNameExtractor
    {
        public string TopicName { get; }

        public StaticTopicNameExtractor(string topicName)
        {
            this.TopicName = topicName;
        }

        public string extract(object key, object value, RecordContext recordContext) => this.TopicName;
    }

    public class StaticTopicNameExtractor<K, V> : AbstractTopicNameExtractor<K, V>
    {
        public string TopicName { get; }

        public StaticTopicNameExtractor(string topicName)
        {
            this.TopicName = topicName;
        }

        public override string extract(K key, V value, RecordContext recordContext) => this.TopicName;
    }
}
