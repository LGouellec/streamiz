using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Kafka.Net.Processors
{
    internal class SinkProcessor<K, V> : AbstractProcessor<K, V>
    {
        private readonly ITopicNameExtractor<K, V> topicNameExtractor;

        internal SinkProcessor(string name, ITopicNameExtractor<K, V> topicNameExtractor, ISerDes<K> keySerdes, ISerDes<V> valueSerdes)
            : base(name, keySerdes, valueSerdes)
        {
            this.topicNameExtractor = topicNameExtractor;
        }

        public override void Init(ProcessorContext context)
        {
            base.Init(context);

            if (Key == null)
                Key = context.Configuration.DefaultKeySerDes;

            if (Value == null)
                Value = context.Configuration.DefaultValueSerDes;
        }

        public override void Process(K key, V value)
        {
            LogProcessingKeyValue(key, value);

            long timestamp = Context.Timestamp;
            if (timestamp < 0)
            {
                throw new StreamsException($"Invalid (negative) timestamp of {timestamp } for output record <{key}:{value}>.");
            }

            if (KeySerDes == null || ValueSerDes == null)
            {
                log.Error($"{logPrefix}Impossible to send sink data because keySerdes and/or valueSerdes is not setted ! KeySerdes : {(KeySerDes != null ? KeySerDes.GetType().Name : "NULL")} | ValueSerdes : {(ValueSerDes != null ? ValueSerDes.GetType().Name : "NULL")}.");
                var s = KeySerDes == null ? "key" : "value";
                throw new StreamsException($"{logPrefix}Invalid {s} serdes for this processor. Default {s} serdes is not the same type. Please set a explicit {s} serdes.");
            }

            var topicName = topicNameExtractor.Extract(key, value, Context.RecordContext);
            Context.RecordCollector.Send(topicName, key, value, null, timestamp, KeySerDes, ValueSerDes);
        }
    }
}
