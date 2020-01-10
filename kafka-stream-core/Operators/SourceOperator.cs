using kafka_stream_core.SerDes;
using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.Operators
{
    internal class SourceOperator<K,V> : AbstractOperator<K, V>
    {
        private readonly string topicName;
        internal SourceOperator(string name, string topicName, ISerDes<K> keySerdes, ISerDes<V> valueSerdes)
            : base(name, null, keySerdes, valueSerdes)
        {
            this.topicName = topicName;
        }

        public override void Kill()
        {
            context.Client.Unsubscribe<K, V>(topicName);
        }

        public override void Message(K key, V value)
        {
            Console.WriteLine("Source Operator => Key: " + key + " | Value : " + value);

            foreach (var n in Next)
                if (n is IOperator<K, V>)
                    ((IOperator<K, V>)n).Message(key, value);
        }

        public override void Start()
        {
            context.Client.Subscribe<K, V>(topicName, Message);
        }

        public override void Stop()
        {
            context.Client.Unsubscribe<K, V>(topicName);
        }
    }
}
