using System.Linq;

namespace Streamiz.Kafka.Net.Table.Internal
{
    internal abstract class AbstractKTableKTableJoinValueGetterSupplier<K, V1, V2, VR> :
        IKTableValueGetterSupplier<K, VR>
    {
        protected readonly IKTableValueGetterSupplier<K, V1> getter1;
        protected readonly IKTableValueGetterSupplier<K, V2> getter2;

        public AbstractKTableKTableJoinValueGetterSupplier(
            IKTableValueGetterSupplier<K, V1> getter1,
            IKTableValueGetterSupplier<K, V2> getter2)
        {
            this.getter1 = getter1;
            this.getter2 = getter2;
        }

        public string[] StoreNames => getter1.StoreNames.Concat(getter2.StoreNames).ToArray();

        public abstract IKTableValueGetter<K, VR> Get();
    }
}
