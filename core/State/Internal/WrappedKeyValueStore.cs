using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State.Cache;
using Streamiz.Kafka.Net.Table.Internal;

namespace Streamiz.Kafka.Net.State.Internal
{
    internal abstract class WrappedKeyValueStore<K, V> :
        WrappedStateStore<IKeyValueStore<Bytes, byte[]>>, ICachedStateStore<K, V>
    {
        protected ISerDes<K> keySerdes;
        protected ISerDes<V> valueSerdes;
        protected bool initStoreSerdes = false;

        public WrappedKeyValueStore(IKeyValueStore<Bytes, byte[]> wrapped, ISerDes<K> keySerdes, ISerDes<V> valueSerdes)
            : base(wrapped)
        {
            this.keySerdes = keySerdes;
            this.valueSerdes = valueSerdes;
        }
        
        public override bool IsCachedStore => wrapped is IWrappedStateStore { IsCachedStore: true };

        public abstract bool SetFlushListener(Action<KeyValuePair<K, Change<V>>> listener, bool sendOldChanges);
        
        public virtual void InitStoreSerDes(ProcessorContext context)
        {
            if (!initStoreSerdes)
            {
                keySerdes ??= context.Configuration.DefaultKeySerDes as ISerDes<K>;
                valueSerdes ??= context.Configuration.DefaultValueSerDes as ISerDes<V>;
                
                keySerdes?.Initialize(new SerDesContext(context.Configuration));
                valueSerdes?.Initialize(new SerDesContext(context.Configuration));

                initStoreSerdes = true;
            }
        }

        public override void Init(ProcessorContext context, IStateStore root)
        {
            base.Init(context, root);
            InitStoreSerDes(context);
        }
    }
}
