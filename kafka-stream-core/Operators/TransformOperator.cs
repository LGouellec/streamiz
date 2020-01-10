using System;
using System.Collections.Generic;
using System.Text;
using kafka_stream_core.SerDes;

namespace kafka_stream_core.Operators
{
    internal class TransformOperator<K, V, K1, V1> : AbstractOperator<K, V>
    {
        private readonly Func<K, V, KeyValuePair<K1, V1>> transformer;

        public TransformOperator(string name, IOperator previous, Func<K, V, KeyValuePair<K1, V1>> transformer) 
            : base(name, previous)
        {
            this.transformer = transformer;
        }

        public override void Kill()
        {
            
        }

        public override void Message(K key, V value)
        {
            KeyValuePair<K1,V1> kp = transformer.Invoke(key, value);

            foreach (var n in Next)
                if (n is IOperator<K1, V1>)
                    ((IOperator<K1, V1>)n).Message(kp.Key, kp.Value);
        }

        public override void Start()
        {
            
        }

        public override void Stop()
        {
            
        }
    }
}
