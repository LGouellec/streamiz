namespace Kafka.Streams.Net.Processors.Internal
{
    internal class StaticTopicNameExtractor<K, V> : ITopicNameExtractor<K, V>
    {
        public string TopicName { get; }

        public StaticTopicNameExtractor(string topicName)
        {
            this.TopicName = topicName;
        }

        public string Extract(K key, V value, IRecordContext recordContext) => this.TopicName;
    }
}
