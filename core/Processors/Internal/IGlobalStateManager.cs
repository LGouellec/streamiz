using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Text;

namespace Streamiz.Kafka.Net.Processors.Internal
{
    internal interface IGlobalStateManager : IStateManager
    {
        ISet<string> Initialize();

        void SetGlobalProcessorContext(ProcessorContext processorContext);

        IDictionary<TopicPartition, long> ChangelogOffsets { get; }
    }
}
