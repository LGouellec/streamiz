using kafka_stream_core.SerDes;
using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.Processors
{
    internal class SinkProcessor<K, V> : AbstractProcessor<K, V>
    {
        private readonly string topicName;

        internal SinkProcessor(string name, IProcessor previous, string topicName, ISerDes<K> keySerdes, ISerDes<V> valueSerdes)
            : base(name, previous, keySerdes, valueSerdes)
        {
            this.topicName = topicName;
        }

        public override void Process(K key, V value)
        {
            byte[] k = null, v = null;

            if(key != null)
                k = KeySerDes.Serialize(key);

            if(value != null)
                v = ValueSerDes.Serialize(value);

            Context.Producer.Produce(topicName, new Confluent.Kafka.Message<byte[], byte[]> { Key = k, Value = v });
        }
    }
}
