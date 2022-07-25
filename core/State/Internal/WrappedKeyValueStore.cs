using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Kafka.Net.State.Internal
{
    internal class WrappedKeyValueStore<K, V> :
        WrappedStateStore<IKeyValueStore<Bytes, byte[]>>
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
