using kafka_stream_core.SerDes;
using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.Operators
{
    internal class SinkOperator<K,V> : AbstractOperator<K, V>
    {
        private readonly string topicName;

        internal SinkOperator(string name, IOperator previous, string topicName, ISerDes<K> keySerdes, ISerDes<V> valueSerdes)
            : base(name, previous, keySerdes, valueSerdes)
        {
            this.topicName = topicName;
        }

        public override void Kill()
        {

        }

        public override void Message(K key, V value)
        {
            context.Client.Publish(key, value, topicName);
        }

        public override void Start()
        {

        }

        public override void Stop()
        {

        }
    }
}
