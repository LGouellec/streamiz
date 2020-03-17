using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.Table.Internal
{
    internal class GenericKTableValueGetterSupplier<K, V> : IKTableValueGetterSupplier<K, V>
    {
        private readonly string[] storeNames;
        private readonly IKTableValueGetter<K, V> getter;

        public GenericKTableValueGetterSupplier(string[] storeName, IKTableValueGetter<K, V> getter)
        {
            this.storeNames = storeName;
            this.getter = getter;
        }

        public string[] StoreNames => storeNames;

        public IKTableValueGetter<K, V> get() => getter;
    }
}
