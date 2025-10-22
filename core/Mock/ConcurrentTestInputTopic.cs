using System.Collections.Generic;
using Streamiz.Kafka.Net.Mock.Pipes;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Kafka.Net.Mock
{
    public class ConcurrentTestInputTopic<K, V> : TestInputTopic<K, V>
    {
        internal ConcurrentTestInputTopic(IPipeInput pipe, IStreamConfig configuration, ISerDes<K> keySerdes, ISerDes<V> valueSerdes) 
            : base(pipe, configuration, keySerdes, valueSerdes)
        {
        }

        public override void PipeInput(TestRecord<K, V> record)
        {
            base.PipeInput(record);
        }

        public override void PipeInputs(IEnumerable<TestRecord<K, V>> records)
        {
            base.PipeInputs(records);
        }
    }
}