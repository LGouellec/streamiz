using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Table.Internal;

namespace Streamiz.Kafka.Net.Processors
{
    internal interface IKStreamAggProcessorSupplier<K, RK, V, T> : IProcessorSupplier<K, V>
    {
        IKTableValueGetterSupplier<RK, T> View();

        void EnableSendingOldValues();
    }
}
