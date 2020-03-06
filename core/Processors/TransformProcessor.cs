using System;
using System.Collections.Generic;
using System.Text;
using kafka_stream_core.SerDes;

namespace kafka_stream_core.Processors
{
    internal class TransformProcessor<K, V, K1, V1> : AbstractProcessor<K, V>
    {
        private readonly Func<K, V, KeyValuePair<K1, V1>> transformer;

        public TransformProcessor(string name, IProcessor previous, Func<K, V, KeyValuePair<K1, V1>> transformer) 
            : base(name, previous)
        {
            this.transformer = transformer;
        }

        public override void Process(K key, V value)
        {
            KeyValuePair<K1,V1> kp = transformer.Invoke(key, value);

            foreach (var n in Next)
                if (n is IProcessor<K1, V1>)
                    ((IProcessor<K1, V1>)n).Process(kp.Key, kp.Value);
        }
    }
}
