using kafka_stream_core.SerDes;
using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.Operators
{
    internal class FilterOperator<K, V> : AbstractOperator<K, V>
    {
        private readonly Func<K, V, bool> _filterPredicate;

        internal FilterOperator(IOperator previous, string name, Func<K, V, bool> predicate)
            : base(name, previous)
        {
            _filterPredicate = predicate;
        }

        public override void Init(ContextOperator context)
        {
            foreach (var n in Next)
                n.Init(context);
            this.context = context;
        }

        public override void Kill()
        {
        }

        public override void Message(K key, V value)
        {
            if(_filterPredicate.Invoke(key, value))
            {
                foreach (var n in Next)
                    if (n is IOperator<K, V>)
                        ((IOperator<K, V>)n).Message(key, value);
            }
        }

        public override void Start()
        {
        }

        public override void Stop()
        {
        }
    }
}
