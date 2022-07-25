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
        public IStreamConfig Config { get; }

        public StreamizConsumerConfig(ConsumerConfig config, string threadId)
            : this(config, threadId, null)
        {
        }
        
        public StreamizConsumerConfig(ConsumerConfig config, string threadId, IStreamConfig streamsConfig)
            : base(config.ToDictionary(c => c.Key, c => c.Value))
        {
            ThreadId = threadId;
            Config = streamsConfig;
        }
    }
    
    /// <summary>
    /// Wrapper <see cref="ProducerConfig"/> class for getting <see cref="IStreamConfig"/> instance in <see cref="DefaultKafkaClientSupplier"/>
    /// </summary>
    internal class StreamizProducerConfig : ProducerConfig
    {
        public string ThreadId { get; }
        public TaskId Id { get; }
        public IStreamConfig Config { get; }

        public StreamizProducerConfig(ProducerConfig config, string threadId, TaskId id = null)
            : this(config, threadId, id, null)
        {
        }
        
        public StreamizProducerConfig(ProducerConfig config, string threadId, TaskId id, IStreamConfig streamsConfig)
            : base(config.ToDictionary(c => c.Key, c => c.Value))
        {
            ThreadId = threadId;
            Id = id;
            Config = streamsConfig;
        }

    }
    
    /// <summary>
    /// Helper wrapper class
    /// </summary>
    internal static class StreamizWrapHelper
    {
        internal static ConsumerConfig Wrap(this ConsumerConfig config, string threadId)
            => new StreamizConsumerConfig(config, threadId);
        
        internal static ConsumerConfig Wrap(this ConsumerConfig config, string threadId, IStreamConfig streamConfig)
            => new StreamizConsumerConfig(config, threadId, streamConfig);
        
        internal static ProducerConfig Wrap(this ProducerConfig config, string threadId, TaskId id = null)
            => new StreamizProducerConfig(config, threadId, id);
        
        internal static ProducerConfig Wrap(this ProducerConfig config, string threadId, IStreamConfig streamConfig)
            => new StreamizProducerConfig(config, threadId, null, streamConfig);
    }
}