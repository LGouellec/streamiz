using kafka_stream_core.SerDes;
using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.Processors
{
    internal class SourceProcessor<K,V> : AbstractProcessor<K, V>
    {
        private readonly string topicName;
        internal SourceProcessor(string name, string topicName, ISerDes<K> keySerdes, ISerDes<V> valueSerdes)
            : base(name, null, keySerdes, valueSerdes)
        {
            this.topicName = topicName;
        }

        public override void Kill()
        {
            context.Client.Unsubscribe<K, V>(topicName);
        }

        public override void Process(K key, V value)
        {
            Console.WriteLine("Source Operator => Key: " + key + " | Value : " + value);

            foreach (var n in Next)
                if (n is IProcessor<K, V>)
                    ((IProcessor<K, V>)n).Process(key, value);
        }

        public override void Start()
        {
            context.Client.Subscribe<K, V>(topicName, this.KeySerDes, this.ValueSerDes, Process);
        }

        public override void Stop()
        {
            context.Client.Unsubscribe<K, V>(topicName);
        }
    }
}
