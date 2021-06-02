using Confluent.Kafka;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Processors.Internal
{
    internal interface IStateManager
    {
        IEnumerable<string> StateStoreNames { get; }
        void Flush();
        void Register(IStateStore store, StateRestoreCallback callback);
        void Close();
        IStateStore GetStore(string name);
        TopicPartition GetRegisteredChangelogPartitionFor(string name);
    }
}
