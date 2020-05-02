using System;
using System.Collections.Generic;
using System.Text;

namespace Streamiz.Kafka.Net.Table.Internal
{
    internal class GlobalKTable<K, V> : IGlobalKTable
    {
        public GlobalKTable(IKTableValueGetterSupplier<K, V> valueGetterSupplier, string queryableStoreName)
        {
            this.ValueGetterSupplier = valueGetterSupplier;
            this.QueryableStoreName = queryableStoreName;
        }

        public IKTableValueGetterSupplier<K, V> ValueGetterSupplier { get; }

        public string QueryableStoreName { get; }
    }
}
