
using System.Threading;
using Confluent.Kafka;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Metrics.Internal;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Kafka.Net.Processors
{
    internal interface ISourceProcessor : IProcessor
    {
        string TopicName { get; set; }
        ITimestampExtractor Extractor { get; }
        ObjectDeserialized DeserializeKey(ConsumeResult<byte[], byte[]> record);
        ObjectDeserialized DeserializeValue(ConsumeResult<byte[], byte[]> record);
    }

    internal class SourceProcessor<K, V> : AbstractProcessor<K, V>, ISourceProcessor
    {
        private Sensor processSensor;
        
        internal SourceProcessor(string name, string topicName, ISerDes<K> keySerdes, ISerDes<V> valueSerdes, ITimestampExtractor extractor)
            : base(name, keySerdes, valueSerdes)
        {
            TopicName = topicName;
            Extractor = extractor;
        }

        public string TopicName { get; set; }

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

            Key?.Initialize(context.SerDesContext);
            Value?.Initialize(context.SerDesContext);

            processSensor = ProcessorNodeMetrics.ProcessNodeSensor(
                Thread.CurrentThread.Name,
                context.Id,
                Name,
                context.Metrics);
                
            base.Init(context);
        }

        public override void Process(K key, V value)
        {
            Forward(key, value);
            processSensor.Record(1.0, Context.Timestamp);
        }
    }
}
