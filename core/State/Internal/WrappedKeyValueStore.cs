using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.SerDes;
using System;
using System.Collections.Generic;
using System.Text;

namespace Streamiz.Kafka.Net.State.Internal
{
    internal class WrappedKeyValueStore<K, V> :
        WrappedStateStore<IKeyValueStore<Bytes, byte[]>, K, V>
    {
        protected ISerDes<K> keySerdes;
        protected ISerDes<V> valueSerdes;

        public WrappedKeyValueStore(IKeyValueStore<Bytes, byte[]> wrapped, ISerDes<K> keySerdes, ISerDes<V> valueSerdes) 
            : base(wrapped)
        {
            this.keySerdes = keySerdes;
            this.valueSerdes = valueSerdes;
        }

        public virtual void InitStoreSerDes(ProcessorContext context)
        {
            keySerdes = keySerdes == null ? context.Configuration.DefaultKeySerDes as ISerDes<K> : keySerdes;
            valueSerdes = valueSerdes == null ? context.Configuration.DefaultValueSerDes as ISerDes<V> : valueSerdes;
        }

        public override void Init(ProcessorContext context, IStateStore root)
        {
            base.Init(context, root);
            InitStoreSerDes(context);
        }
    }
}
