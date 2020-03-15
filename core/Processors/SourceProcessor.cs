using kafka_stream_core.SerDes;
using kafka_stream_core.Stream;
using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.Processors
{
    internal interface ISourceProcessor
    {
        string TopicName { get; }
        TimestampExtractor Extractor { get; }
        Topology.AutoOffsetReset AutoOffsetReset { get; }
    }

    internal class SourceProcessor<K,V> : AbstractProcessor<K, V>, ISourceProcessor
    {
        private readonly string topicName;
        internal SourceProcessor(string name, string topicName, ISerDes<K> keySerdes, ISerDes<V> valueSerdes, TimestampExtractor extractor, Topology.AutoOffsetReset autoOffsetReset)
            : base(name, null, keySerdes, valueSerdes)
        {
            this.topicName = topicName;
            this.Extractor = extractor;
            this.AutoOffsetReset = autoOffsetReset;
        }

        public string TopicName => topicName;

        public TimestampExtractor Extractor { get; }

        public Topology.AutoOffsetReset AutoOffsetReset { get; }

        public override void Process(K key, V value)
        {
            Console.WriteLine("Source Operator => Key: " + key + " | Value : " + value);

            foreach (var n in Next)
                if (n is IProcessor<K, V>)
                    ((IProcessor<K, V>)n).Process(key, value);
        }
    }
}
