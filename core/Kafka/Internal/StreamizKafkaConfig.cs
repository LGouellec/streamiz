using System.Linq;
using Confluent.Kafka;
using Streamiz.Kafka.Net.Processors.Internal;

namespace Streamiz.Kafka.Net.Kafka.Internal
{
    /// <summary>
    /// Wrapper <see cref="ConsumerConfig"/> class for getting <see cref="IStreamConfig"/> instance in <see cref="DefaultKafkaClientSupplier"/>
    /// </summary>
    internal class StreamizConsumerConfig : ConsumerConfig
    {
        public string ThreadId { get; }

        public StreamizConsumerConfig(ConsumerConfig config, string threadId)
            : base(config.ToDictionary(c => c.Key, c => c.Value))
        {
            ThreadId = threadId;
        }
    }
    
    /// <summary>
    /// Wrapper <see cref="ProducerConfig"/> class for getting <see cref="IStreamConfig"/> instance in <see cref="DefaultKafkaClientSupplier"/>
    /// </summary>
    internal class StreamizProducerConfig : ProducerConfig
    {
        public string ThreadId { get; }
        public TaskId Id { get; }

        public StreamizProducerConfig(ProducerConfig config, string threadId, TaskId id = null)
            : base(config.ToDictionary(p => p.Key, p => p.Value))
        {
            ThreadId = threadId;
            Id = id;
        }
    }
    
    /// <summary>
    /// Helper wrapper class
    /// </summary>
    internal static class StreamizWrapHelper
    {
        internal static ConsumerConfig Wrap(this ConsumerConfig config, string threadId)
            => new StreamizConsumerConfig(config, threadId);
        
        internal static ProducerConfig Wrap(this ProducerConfig config, string threadId, TaskId id = null)
            => new StreamizProducerConfig(config, threadId, id);
    }
}