
using Confluent.Kafka;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Kafka.Net.Processors
{
    internal interface ISourceProcessor : IProcessor
    {
        string TopicName { get; }
        ITimestampExtractor Extractor { get; }
        object DeserializeKey(string topicName, Headers headers, byte[] data);
        object DeserializeValue(string topicName, Headers headers, byte[] data);
    }

    internal class SourceProcessor<K, V> : AbstractProcessor<K, V>, ISourceProcessor
    {
        private readonly string topicName;

        internal SourceProcessor(string name, string topicName, ISerDes<K> keySerdes, ISerDes<V> valueSerdes, ITimestampExtractor extractor)
            : base(name, keySerdes, valueSerdes)
        {
            this.topicName = topicName;
            Extractor = extractor;
        }

        public string TopicName => topicName;

        public ITimestampExtractor Extractor { get; }

        public override void Init(ProcessorContext context)
        {
            if (Key == null)
            {
                Key = context.Configuration.DefaultKeySerDes;
            }

            if (Value == null)
            {
                Value = context.Configuration.DefaultValueSerDes;
            }

            base.Init(context);
        }

        public override void Process(K key, V value)
        {
            LogProcessingKeyValue(key, value);

            foreach (var n in Next)
            {
                if (n is IProcessor<K, V>)
                {
                    ((IProcessor<K, V>)n).Process(key, value);
                }
            }
        }

        public object DeserializeKey(string topicName, Headers headers, byte[] data)
        {
            return Key.DeserializeObject(data,
                new SerializationContext(MessageComponentType.Key, topicName, headers)
                );
        }

        public object DeserializeValue(string topicName, Headers headers, byte[] data)
        {
            return Value.DeserializeObject(data,
                    new SerializationContext(MessageComponentType.Value, topicName, headers)
            );
        }
    }
}
