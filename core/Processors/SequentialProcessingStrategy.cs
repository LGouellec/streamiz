using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.Logging;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Kafka.Internal;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Processors.Internal;

namespace Streamiz.Kafka.Net.Processors
{
    /// <summary>
    /// Sequential processing strategy that maintains current behavior.
    /// Processes records one at a time in order, with no parallelism.
    /// Provides backward compatibility with existing external stream processing.
    /// </summary>
    internal class SequentialProcessingStrategy : IProcessingStrategy
    {
        private readonly string threadId;
        private readonly string logPrefix;
        private readonly IStreamConfig configuration;
        private readonly StreamMetricsRegistry streamMetricsRegistry;
        private readonly IAdminClient adminClient;
        private readonly InternalTopologyBuilder internalTopologyBuilder;
        private readonly StreamsProducer producer;
        private readonly Dictionary<string, ExternalProcessorTopologyExecutor> executors;
        private readonly OffsetTracker offsetTracker;
        private static readonly ILogger log = Logger.GetLogger(typeof(SequentialProcessingStrategy));

        public ProcessingStrategyState State { get; private set; }
        public int InFlightCount => offsetTracker.GetTotalInFlightCount();

        /// <summary>
        /// Creates a new sequential processing strategy.
        /// </summary>
        public SequentialProcessingStrategy(
            string threadId,
            InternalTopologyBuilder internalTopologyBuilder,
            StreamsProducer producer,
            IStreamConfig configuration,
            StreamMetricsRegistry streamMetricsRegistry,
            IAdminClient adminClient)
        {
            this.threadId = threadId ?? throw new ArgumentNullException(nameof(threadId));
            this.internalTopologyBuilder = internalTopologyBuilder ?? throw new ArgumentNullException(nameof(internalTopologyBuilder));
            this.producer = producer ?? throw new ArgumentNullException(nameof(producer));
            this.configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
            this.streamMetricsRegistry = streamMetricsRegistry ?? throw new ArgumentNullException(nameof(streamMetricsRegistry));
            this.adminClient = adminClient ?? throw new ArgumentNullException(nameof(adminClient));

            logPrefix = $"sequential-strategy[{threadId}] ";
            executors = new Dictionary<string, ExternalProcessorTopologyExecutor>();
            offsetTracker = new OffsetTracker();
            State = ProcessingStrategyState.Created;

            log.LogDebug($"{logPrefix}Created sequential processing strategy");
        }

        /// <summary>
        /// Starts the strategy.
        /// </summary>
        public void Start()
        {
            if (State != ProcessingStrategyState.Created)
                throw new InvalidOperationException($"{logPrefix}Cannot start from state {State}");

            State = ProcessingStrategyState.Running;
            log.LogInformation($"{logPrefix}Started");
        }

        /// <summary>
        /// Submits a record for processing.
        /// In sequential mode, processing happens synchronously on the calling thread.
        /// </summary>
        public async Task SubmitAsync(ConsumeResult<byte[], byte[]> record)
        {
            if (State != ProcessingStrategyState.Running)
                throw new InvalidOperationException($"{logPrefix}Cannot submit record in state {State}");

            if (record == null)
            {
                // Null record means we should try to process from buffer
                await ProcessFromBufferAsync();
                return;
            }

            var tpo = new TopicPartitionOffset(record.TopicPartition, record.Offset);
            offsetTracker.RecordDispatched(tpo);

            try
            {
                var executor = GetOrCreateExecutor(record.Topic);
                executor.Process(record);
                offsetTracker.RecordCompleted(tpo);
            }
            catch (Exception ex)
            {
                log.LogError(ex, $"{logPrefix}Error processing record {tpo}");
                // Note: Exception handling is done by ExternalProcessorTopologyExecutor
                // If we reach here, it's a fatal error - record is still completed
                // to prevent blocking (will be reprocessed after restart)
                offsetTracker.RecordCompleted(tpo);
                throw;
            }
        }

        /// <summary>
        /// Attempts to process buffered records from any executor with buffered data.
        /// </summary>
        private async Task ProcessFromBufferAsync()
        {
            var executor = executors.Values
                .Where(e => e.BufferSize > 0)
                .FirstOrDefault();

            if (executor != null)
            {
                // Process one buffered record
                // The executor will handle getting the record from its buffer
                executor.Process(null);
            }

            await Task.CompletedTask;
        }

        /// <summary>
        /// Gets committable offsets based on completed processing.
        /// </summary>
        public IEnumerable<TopicPartitionOffset> GetCommittableOffsets()
        {
            return offsetTracker.GetCommittableOffsets();
        }

        /// <summary>
        /// Clears committed offsets from tracking.
        /// Called after successful commit to Kafka.
        /// </summary>
        public void ClearCommittedOffsets()
        {
            var committable = GetCommittableOffsets().ToList();
            foreach (var tpo in committable)
            {
                offsetTracker.ClearPartition(tpo.TopicPartition);
            }
        }

        /// <summary>
        /// Flushes all executors.
        /// </summary>
        public void Flush()
        {
            foreach (var executor in executors.Values)
            {
                executor.Flush();
            }
        }

        /// <summary>
        /// Closes the strategy and releases resources.
        /// </summary>
        public void Close()
        {
            if (State == ProcessingStrategyState.Closed)
                return;

            log.LogInformation($"{logPrefix}Closing");
            State = ProcessingStrategyState.Closing;

            try
            {
                // Flush all executors
                Flush();

                // Close all executors
                foreach (var executor in executors.Values)
                {
                    executor.Close();
                }

                executors.Clear();
                offsetTracker.ClearAll();
            }
            finally
            {
                State = ProcessingStrategyState.Closed;
                log.LogInformation($"{logPrefix}Closed");
            }
        }

        /// <summary>
        /// Disposes the strategy.
        /// </summary>
        public void Dispose()
        {
            Close();
        }

        /// <summary>
        /// Gets or creates an executor for the given topic.
        /// </summary>
        private ExternalProcessorTopologyExecutor GetOrCreateExecutor(string topic)
        {
            if (executors.TryGetValue(topic, out var executor))
                return executor;

            var taskId = internalTopologyBuilder.GetTaskIdFromPartition(new TopicPartition(topic, Partition.Any));
            var topology = internalTopologyBuilder.BuildTopology(taskId, configuration);

            executor = new ExternalProcessorTopologyExecutor(
                threadId,
                taskId,
                topology.GetSourceProcessor(topic),
                producer,
                configuration,
                streamMetricsRegistry,
                adminClient);

            executors.Add(topic, executor);
            log.LogDebug($"{logPrefix}Created executor for topic {topic}");

            return executor;
        }

        /// <summary>
        /// Clears internal buffers (used during error recovery).
        /// </summary>
        public void ClearBuffers()
        {
            foreach (var executor in executors.Values)
            {
                executor.ClearBuffer();
            }
        }

        /// <summary>
        /// Gets executors (for testing/diagnostics).
        /// </summary>
        internal IReadOnlyDictionary<string, ExternalProcessorTopologyExecutor> Executors => executors;
    }
}
