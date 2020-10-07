
using Confluent.Kafka;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Kafka.Net.Processors
{
    internal interface ISourceProcessor : IProcessor
    {
        string TopicName { get; }
        ITimestampExtractor Extractor { get; }
        ObjectDeserialized DeserializeKey(ConsumeResult<byte[], byte[]> record);
        ObjectDeserialized DeserializeValue(ConsumeResult<byte[], byte[]> record);
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
    }
}
