using System.Collections.Generic;
using Confluent.Kafka;

namespace Streamiz.Kafka.Net.Processors.Internal
{
    internal interface IChangelogReader : IChangelogRegister
    {
        void Restore(IDictionary<TaskId, ITask> tasks);
        void Clear();
        bool IsEmpty { get; }
        IEnumerable<TopicPartition> CompletedChangelogs { get; }
    }
}
