using kafka_stream_core.Errors;
using kafka_stream_core.Processors.Internal;
using kafka_stream_core.SerDes;
using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.Processors
{
    internal class SinkProcessor<K, V> : AbstractProcessor<K, V>
    {
        private readonly ITopicNameExtractor<K, V> topicNameExtractor;

        internal SinkProcessor(string name, IProcessor previous, ITopicNameExtractor<K, V> topicNameExtractor, ISerDes<K> keySerdes, ISerDes<V> valueSerdes)
            : base(name, previous, keySerdes, valueSerdes)
        {
            this.topicNameExtractor = topicNameExtractor;
        }

        public override void Init(ProcessorContext context)
        {
            base.Init(context);

            if (this.Key == null)
                this.Key = context.Configuration.DefaultKeySerDes;

            if (this.Value == null)
                this.Value = context.Configuration.DefaultValueSerDes;
        }

        public override object Clone()
        {
            return new SinkProcessor<K, V>(this.Name, null, this.topicNameExtractor, this.KeySerDes, this.ValueSerDes);
        }

        public override void Process(K key, V value)
        {
            LogProcessingKeyValue(key, value);

            long timestamp = Context.Timestamp;
            if (timestamp < 0)
            {
                throw new StreamsException($"Invalid (negative) timestamp of {timestamp } for output record <{key}:{value}>.");
            }

            var topicName = this.topicNameExtractor.Extract(key, value, this.Context.RecordContext);
            // TODO : TO FINISH
            var partitioner = new DefaultStreamPartitioner<K, V>(KeySerDes, null);
            this.Context.RecordCollector.Send<K, V>(topicName, key, value, null, timestamp, KeySerDes, ValueSerDes, partitioner);
        }
    }
}
