using System.Collections.Generic;
using Confluent.Kafka;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Processors.Internal;

namespace Streamiz.Kafka.Net.Tests.Helpers;

public class MockProcessorContext : ProcessorContext
{
    public MockProcessorContext(TaskId id, StreamConfig config)
        : base(UnassignedStreamTask.Create(), config, new ProcessorStateManager(
            id,
            new List<TopicPartition>(),
            new Dictionary<string, string>(),
            new MockChangelogRegister(),
            new MockOffsetCheckpointManager()), new StreamMetricsRegistry())
    { }
}