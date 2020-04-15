using Kafka.Streams.Net.Table.Internal;
using System;

namespace Kafka.Streams.Net.Processors
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
