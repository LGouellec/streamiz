using kafka_stream_core.Table.Internal;
using System;

namespace kafka_stream_core.Processors
{
    internal interface IKTableProcessorSupplier
    {
        void EnableSendingOldValues();
    }

    internal interface IKTableProcessorSupplier<K,V,T> : IKTableProcessorSupplier, IProcessorSupplier<K, Change<V>>
    {
        IKTableValueGetterSupplier<K, T> View { get; }
    }
}
