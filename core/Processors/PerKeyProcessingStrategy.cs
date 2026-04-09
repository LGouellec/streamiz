using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Threading;
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
    /// Per-key processing strategy that processes different keys in parallel
    /// while maintaining ordering for each individual key.
    /// Uses consistent hashing to assign keys to workers.
    /// </summary>
    internal class PerKeyProcessingStrategy : IProcessingStrategy
    {
        private readonly string threadId;
        private readonly string logPrefix;
        private readonly ParallelProcessingConfig config;
        private readonly IStreamConfig streamConfig;
        private readonly StreamMetricsRegistry streamMetricsRegistry;
        private readonly IAdminClient adminClient;
        private readonly InternalTopologyBuilder internalTopologyBuilder;
        private readonly StreamsProducer producer;
        private readonly Dictionary<string, ExternalProcessorTopologyExecutor> executors;
        private readonly OffsetTracker offsetTracker;
        private static readonly ILogger log = Logger.GetLogger(typeof(PerKeyProcessingStrategy));

        // Per-worker queues
        private readonly ConcurrentQueue<RecordWorkItem>[] workerQueues;
        private readonly int workerCount;

        // Concurrency control
        private readonly CancellationTokenSource shutdownCts;
        private readonly List<Task> workerTasks;

        // State tracking
        private int totalQueuedRecords = 0;
        private readonly object queueCountLock = new object();

        public ProcessingStrategyState State { get; private set; }

        public int InFlightCount => offsetTracker.GetTotalInFlightCount();

        public int QueuedRecordsCount => totalQueuedRecords;

        /// <summary>
        /// Creates a new per-key processing strategy.
        /// </summary>
        public PerKeyProcessingStrategy(
            string threadId,
            InternalTopologyBuilder internalTopologyBuilder,
            StreamsProducer producer,
            IStreamConfig streamConfig,
            ParallelProcessingConfig config,
            StreamMetricsRegistry streamMetricsRegistry,
            IAdminClient adminClient)
        {
            this.threadId = threadId ?? throw new ArgumentNullException(nameof(threadId));
            this.internalTopologyBuilder = internalTopologyBuilder ?? throw new ArgumentNullException(nameof(internalTopologyBuilder));
            this.producer = producer ?? throw new ArgumentNullException(nameof(producer));
            this.streamConfig = streamConfig ?? throw new ArgumentNullException(nameof(streamConfig));
            this.config = config ?? throw new ArgumentNullException(nameof(config));
            this.streamMetricsRegistry = streamMetricsRegistry ?? throw new ArgumentNullException(nameof(streamMetricsRegistry));
            this.adminClient = adminClient ?? throw new ArgumentNullException(nameof(adminClient));

            logPrefix = $"per-key-strategy[{threadId}] ";
            executors = new Dictionary<string, ExternalProcessorTopologyExecutor>();
            offsetTracker = new OffsetTracker();
            workerCount = config.MaxConcurrency;
            workerQueues = new ConcurrentQueue<RecordWorkItem>[workerCount];

            // Initialize worker queues
            for (int i = 0; i < workerCount; i++)
            {
                workerQueues[i] = new ConcurrentQueue<RecordWorkItem>();
            }

            shutdownCts = new CancellationTokenSource();
            workerTasks = new List<Task>();
            State = ProcessingStrategyState.Created;

            log.LogDebug($"{logPrefix}Created per-key processing strategy with {workerCount} workers");
        }

        /// <summary>
        /// Starts the strategy and worker pool.
        /// </summary>
        public void Start()
        {
            if (State != ProcessingStrategyState.Created)
                throw new InvalidOperationException($"{logPrefix}Cannot start from state {State}");

            State = ProcessingStrategyState.Running;

            // Start worker tasks
            for (int i = 0; i < workerCount; i++)
            {
                int workerId = i; // Capture for closure
                var workerTask = Task.Run(() => WorkerLoop(workerId), shutdownCts.Token);
                workerTasks.Add(workerTask);
            }

            log.LogInformation($"{logPrefix}Started with {workerCount} workers");
        }

        /// <summary>
        /// Submits a record for processing.
        /// Records with the same key will be routed to the same worker, maintaining key ordering.
        /// </summary>
        public Task SubmitAsync(ConsumeResult<byte[], byte[]> record)
        {
            if (State != ProcessingStrategyState.Running)
                throw new InvalidOperationException($"{logPrefix}Cannot submit record in state {State}");

            if (record == null)
            {
                // Null record means we should process from buffer if needed
                // For per-key strategy, work is always being processed by workers
                return Task.CompletedTask;
            }

            // Check if we're at capacity
            if (totalQueuedRecords >= config.MaxQueuedRecords)
            {
                log.LogWarning($"{logPrefix}Queue capacity reached ({totalQueuedRecords}/{config.MaxQueuedRecords}). This should trigger backpressure.");
            }

            var workItem = new RecordWorkItem(record);
            var tpo = new TopicPartitionOffset(record.TopicPartition, record.Offset);

            // Add to offset tracker
            offsetTracker.RecordDispatched(tpo);

            // Determine which worker should handle this key
            int workerIndex = GetWorkerIndexForKey(record);

            // Enqueue to the appropriate worker
            workerQueues[workerIndex].Enqueue(workItem);

            // Increment queued count
            lock (queueCountLock)
            {
                totalQueuedRecords++;
            }

            log.LogTrace($"{logPrefix}Enqueued record {tpo} to worker {workerIndex}, total queued: {totalQueuedRecords}");

            return Task.CompletedTask;
        }

        /// <summary>
        /// Determines which worker should process a record based on its key.
        /// Uses consistent hashing for key distribution.
        /// Null keys fall back to partition-based assignment.
        /// </summary>
        private int GetWorkerIndexForKey(ConsumeResult<byte[], byte[]> record)
        {
            var key = record.Message.Key;

            // If key is null or empty, use partition number for assignment
            if (key == null || key.Length == 0)
            {
                return record.Partition.Value % workerCount;
            }

            // Hash the key to get a consistent worker assignment
            // Use simple hash for byte arrays
            int hash = ComputeKeyHash(key);
            return Math.Abs(hash % workerCount);
        }

        /// <summary>
        /// Computes a hash code for a byte array key.
        /// </summary>
        private int ComputeKeyHash(byte[] key)
        {
            unchecked
            {
                int hash = 17;
                for (int i = 0; i < key.Length; i++)
                {
                    hash = hash * 31 + key[i];
                }
                return hash;
            }
        }

        /// <summary>
        /// Worker loop that processes records from its assigned queue.
        /// Maintains FIFO ordering, which ensures per-key ordering since
        /// all records with the same key go to the same worker.
        /// </summary>
        private async Task WorkerLoop(int workerId)
        {
            log.LogDebug($"{logPrefix}Worker {workerId} started");

            var myQueue = workerQueues[workerId];

            while (!shutdownCts.Token.IsCancellationRequested)
            {
                try
                {
                    RecordWorkItem workItem = null;

                    // Try to get work from this worker's queue
                    if (myQueue.TryDequeue(out workItem))
                    {
                        // Decrement queued count
                        lock (queueCountLock)
                        {
                            totalQueuedRecords--;
                        }

                        // Process the work item
                        await ProcessWorkItemAsync(workItem, workerId);
                    }
                    else
                    {
                        // No work available, wait a bit
                        await Task.Delay(10, shutdownCts.Token);
                    }
                }
                catch (OperationCanceledException)
                {
                    // Shutting down
                    break;
                }
                catch (Exception ex)
                {
                    log.LogError(ex, $"{logPrefix}Worker {workerId} encountered unexpected error");
                    // Continue processing
                }
            }

            log.LogDebug($"{logPrefix}Worker {workerId} stopped");
        }

        /// <summary>
        /// Processes a single work item.
        /// </summary>
        private async Task ProcessWorkItemAsync(RecordWorkItem workItem, int workerId)
        {
            var record = workItem.Record;
            var tpo = new TopicPartitionOffset(record.TopicPartition, record.Offset);

            try
            {
                log.LogTrace($"{logPrefix}Worker {workerId} processing {tpo}");

                var executor = GetOrCreateExecutor(record.Topic);

                // Process async
                await executor.ProcessAsync(record);

                // Mark as completed
                offsetTracker.RecordCompleted(tpo);
                workItem.Complete();

                log.LogTrace($"{logPrefix}Worker {workerId} completed {tpo}");
            }
            catch (Exception ex)
            {
                log.LogError(ex, $"{logPrefix}Worker {workerId} failed processing {tpo}");

                // Mark as completed even on error to prevent blocking
                // (will be reprocessed after restart due to at-least-once semantics)
                offsetTracker.RecordCompleted(tpo);
                workItem.Fail(ex);
            }
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
                // Signal shutdown
                shutdownCts.Cancel();

                // Wait for workers to complete (with timeout)
                var completedInTime = Task.WaitAll(workerTasks.ToArray(), config.MaxWaitForCompletion);

                if (!completedInTime)
                {
                    log.LogWarning($"{logPrefix}Workers did not complete within timeout ({config.MaxWaitForCompletion}). {InFlightCount} records may be reprocessed on restart.");
                }

                // Flush all executors
                Flush();

                // Close all executors
                foreach (var executor in executors.Values)
                {
                    executor.Close();
                }

                executors.Clear();
                offsetTracker.ClearAll();

                shutdownCts.Dispose();
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
            var topology = internalTopologyBuilder.BuildTopology(taskId, streamConfig);

            executor = new ExternalProcessorTopologyExecutor(
                threadId,
                taskId,
                topology.GetSourceProcessor(topic),
                producer,
                streamConfig,
                streamMetricsRegistry,
                adminClient);

            executors.Add(topic, executor);
            log.LogDebug($"{logPrefix}Created executor for topic {topic}");

            return executor;
        }

        /// <summary>
        /// Gets the number of queued records for a specific worker.
        /// </summary>
        public int GetWorkerQueueDepth(int workerId)
        {
            if (workerId < 0 || workerId >= workerCount)
                return 0;

            return workerQueues[workerId].Count;
        }

        /// <summary>
        /// Checks if the strategy is at capacity and should trigger backpressure.
        /// </summary>
        public bool IsAtCapacity()
        {
            return totalQueuedRecords >= config.MaxQueuedRecords;
        }

        /// <summary>
        /// Gets diagnostics information for monitoring.
        /// </summary>
        public string GetDiagnostics()
        {
            var workerInfo = Enumerable.Range(0, workerCount)
                .Select(i => $"W{i}:{workerQueues[i].Count}")
                .ToList();

            return $"Queued: {totalQueuedRecords}/{config.MaxQueuedRecords}, " +
                   $"InFlight: {InFlightCount}, " +
                   $"Workers: {workerCount}, " +
                   $"[{string.Join(", ", workerInfo)}]";
        }
    }
}
