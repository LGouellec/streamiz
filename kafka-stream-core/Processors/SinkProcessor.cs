using kafka_stream_core.SerDes;
using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.Processors
{
    internal class SinkProcessor<K,V> : AbstractProcessor<K, V>
    {
        private readonly string topicName;

        internal SinkProcessor(string name, IProcessor previous, string topicName, ISerDes<K> keySerdes, ISerDes<V> valueSerdes)
            : base(name, previous, keySerdes, valueSerdes)
        {
            this.topicName = topicName;
        }

        public override void Kill()
        {

        }

        public override void Process(K key, V value)
        {
            context.Client.Publish(key, value, this.KeySerDes, this.ValueSerDes, topicName);
        }

        public override void Start()
        {

        }

        public override void Stop()
        {

        }
    }
}
