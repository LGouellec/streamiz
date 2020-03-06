using kafka_stream_core.SerDes;
using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.Processors
{
    internal interface ISourceProcessor
    {
        string TopicName { get; }
    }

    internal class SourceProcessor<K,V> : AbstractProcessor<K, V>, ISourceProcessor
    {
        private readonly string topicName;
        internal SourceProcessor(string name, string topicName, ISerDes<K> keySerdes, ISerDes<V> valueSerdes)
            : base(name, null, keySerdes, valueSerdes)
        {
            this.topicName = topicName;
        }

        public string TopicName => topicName;

        public override void Process(K key, V value)
        {
            Console.WriteLine("Source Operator => Key: " + key + " | Value : " + value);

            foreach (var n in Next)
                if (n is IProcessor<K, V>)
                    ((IProcessor<K, V>)n).Process(key, value);
        }
    }
}
