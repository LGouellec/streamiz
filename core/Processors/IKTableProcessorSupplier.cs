using kafka_stream_core.Table.Internal;
using System;

namespace kafka_stream_core.Processors
{
    internal interface IKTableProcessorSupplier<K,V,T> : IProcessorSupplier<K, Change<V>>
    {
        IKTableValueGetterSupplier<K, T> View { get; }

        void enableSendingOldValues();
    }
}
