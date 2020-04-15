using Kafka.Streams.Net.Crosscutting;
using Kafka.Streams.Net.Processors;
using Kafka.Streams.Net.SerDes;
using System;
using System.Collections.Generic;
using System.Text;

namespace Kafka.Streams.Net.State.Internal
{
    internal class WrappedKeyValueStore<K, V> :
        WrappedStateStore<KeyValueStore<Bytes, byte[]>, K, V>
    {
        protected ISerDes<K> keySerdes;
        protected ISerDes<V> valueSerdes;

        public WrappedKeyValueStore(KeyValueStore<Bytes, byte[]> wrapped, ISerDes<K> keySerdes, ISerDes<V> valueSerdes) 
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
