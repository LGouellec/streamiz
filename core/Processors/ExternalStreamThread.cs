using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.Logging;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Kafka;
using Streamiz.Kafka.Net.Kafka.Internal;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Metrics.Internal;
using Streamiz.Kafka.Net.Processors.Internal;

namespace Streamiz.Kafka.Net.Processors
{
    internal class ExternalStreamThread : IThread
    {
        private static readonly ILogger log = Logger.GetLogger(typeof(ExternalStreamThread));
        private static readonly object @lock = new();
        
        private readonly string clientId;
        private readonly IKafkaSupplier kafkaSupplier;
        private readonly InternalTopologyBuilder internalTopologyBuilder;
        private readonly IEnumerable<string> externalSourceTopics;
        private readonly IDictionary<string, ExternalProcessorTopologyExecutor> externalProcessorTopologies =
            new Dictionary<string, ExternalProcessorTopologyExecutor>();
        private readonly StreamMetricsRegistry streamMetricsRegistry;
        private readonly IStreamConfig configuration;
        private IConsumer<byte[], byte[]> currentConsumer;
        private IAdminClient adminClient;
        private readonly string logPrefix;
        private readonly Thread thread;
        private IProcessingStrategy processingStrategy;
        
        private DateTime lastCommit = DateTime.Now;
        private DateTime lastMetrics = DateTime.Now;

        private DateTime lastCheckoutProcessing = DateTime.Now;
        private TimeSpan intervalCheckoutProcessing = TimeSpan.FromMinutes(1); // 1 minute per default
        private int messageProcessed;
        private Task<List<DeleteRecordsResult>> currentDeleteTask = null;
        
        private readonly Sensor commitSensor;
        private readonly Sensor pollSensor;
        private readonly Sensor processLatencySensor;
        private readonly Sensor processRateSensor;
        private readonly Sensor parallelInFlightRecordsSensor;
        private readonly Sensor parallelQueueDepthSensor;
        private readonly Sensor parallelWorkerCountSensor;
        private StreamsProducer producer;

        public ExternalStreamThread(
            string threadId,
            string clientId,
            IKafkaSupplier kafkaSupplier,
            InternalTopologyBuilder internalTopologyBuilder,
            StreamMetricsRegistry streamMetricsRegistry,
            IStreamConfig configuration)
        {
            this.clientId = clientId;
            this.kafkaSupplier = kafkaSupplier;
            this.internalTopologyBuilder = internalTopologyBuilder;
            externalSourceTopics = internalTopologyBuilder.GetRequestTopics();
            this.streamMetricsRegistry = streamMetricsRegistry;
            this.configuration = configuration;
            
            thread = new Thread(Run)
            {
                Name = threadId
            };
            Name = threadId;
            logPrefix = $"external-stream-thread[{threadId}] ";
            intervalCheckoutProcessing = this.configuration.LogProcessingSummary;
            
            State = ThreadState.CREATED;

            commitSensor = ThreadMetrics.CommitSensor(threadId, streamMetricsRegistry);
            pollSensor = ThreadMetrics.PollSensor(threadId, streamMetricsRegistry);
            processLatencySensor = ThreadMetrics.ProcessLatencySensor(threadId, streamMetricsRegistry);
            processRateSensor = ThreadMetrics.ProcessRateSensor(threadId, streamMetricsRegistry);
            parallelInFlightRecordsSensor = ThreadMetrics.ParallelInFlightRecordsSensor(threadId, streamMetricsRegistry);
            parallelQueueDepthSensor = ThreadMetrics.ParallelQueueDepthSensor(threadId, streamMetricsRegistry);
            parallelWorkerCountSensor = ThreadMetrics.ParallelWorkerCountSensor(threadId, streamMetricsRegistry);
        }
        
        public void Dispose() => CloseThread();
        public int Id => thread.ManagedThreadId;
        public ThreadState State { get; private set; }
        public bool IsDisposable { get; private set; } = false;
        public string Name { get; }
        public bool IsRunning { get; private set; } = false;
        
        public void Run()
        {
            Exception exception = null;
            try
            {
                SetState(ThreadState.RUNNING);

                while (State.IsRunning())
                {
                    if (exception != null)
                    {
                        ExceptionHandlerResponse response = TreatException(exception);
                        if (response == ExceptionHandlerResponse.FAIL)
                            break;
                        if (response == ExceptionHandlerResponse.CONTINUE)
                        {
                            exception = null;
                            HandleInnerException();
                        }
                    }
                    
                    long now = DateTime.Now.GetMilliseconds();

                    var consumer = GetConsumer();
                    ConsumeResult<byte[], byte[]> result = null;
                    long pollLatency = ActionHelper.MeasureLatency(() =>
                        result = consumer.Consume(TimeSpan.FromMilliseconds(configuration.PollMs)));

                    pollSensor.Record(pollLatency, now);

                    try
                    {
                        // Submit record to processing strategy
                        if (result != null || processingStrategy.InFlightCount > 0)
                        {
                            now = DateTime.Now.GetMilliseconds();
                            long processLatency = ActionHelper.MeasureLatency(() =>
                                processingStrategy.SubmitAsync(result).GetAwaiter().GetResult());

                            if (result != null)
                            {
                                log.LogDebug($"Submitted record to processing strategy in {processLatency} ms");
                                ++messageProcessed;
                                processLatencySensor.Record(processLatency, now);
                                processRateSensor.Record(1, now);
                            }

                            // Backpressure: check if strategy is at capacity
                            bool isAtCapacity = false;
                            if (processingStrategy is PerPartitionProcessingStrategy perPartitionStrategy)
                            {
                                isAtCapacity = perPartitionStrategy.IsAtCapacity();
                            }
                            else if (processingStrategy is PerKeyProcessingStrategy perKeyStrategy)
                            {
                                isAtCapacity = perKeyStrategy.IsAtCapacity();
                            }
                            else if (processingStrategy is UnorderedProcessingStrategy unorderedStrategy)
                            {
                                isAtCapacity = unorderedStrategy.IsAtCapacity();
                            }

                            if (isAtCapacity && result != null)
                            {
                                var assignmentTopic = consumer.Assignment
                                    .Where(a => a.Topic.Equals(result.Topic))
                                    .ToList();
                                consumer.Pause(assignmentTopic);
                                log.LogWarning($"{logPrefix}Strategy at capacity, paused topic {result.Topic}");
                            }
                            else if (!isAtCapacity)
                            {
                                // Resume all paused partitions if we're under capacity
                                var pausedPartitions = consumer.Assignment.Except(consumer.Assignment).ToList();
                                if (pausedPartitions.Any())
                                {
                                    consumer.Resume(pausedPartitions);
                                    log.LogInformation($"{logPrefix}Resumed paused partitions");
                                }
                            }
                        }

                        now = DateTime.Now.GetMilliseconds();
                        if (now >= lastCheckoutProcessing.Add(intervalCheckoutProcessing).GetMilliseconds())
                        {
                            log.LogInformation(
                                $"{logPrefix}Processed {messageProcessed} total records in {intervalCheckoutProcessing.TotalMilliseconds}ms");
                            lastCheckoutProcessing = DateTime.Now;
                            messageProcessed = 0;
                        }

                        bool committed = false;
                        long commitLatency = ActionHelper.MeasureLatency(() => committed = Commit());
                        if(committed)
                            commitSensor.Record(commitLatency, now);
                    }
                    catch (Exception e)
                    {
                        if (e is NoneRetryableException or NotEnoughtTimeException)
                        {
                            log.LogError(e,  $"{logPrefix}Encountered one retryable exception because number retry is exceed or you have not enough time to process the record regarding the max.poll.interval.ms configuration." +
                                             $" Your retry policy behavior is failed, so the external stream thread will be stopped");
                            break;
                        }

                        log.LogError(e, $"{logPrefix}Encountered the following unexpected Kafka exception during processing, this usually indicate Streams internal errors:");
                        exception = e;
                    }
                    
                    if (lastMetrics.Add(TimeSpan.FromMilliseconds(configuration.MetricsIntervalMs)) <
                        DateTime.Now)
                    {
                        // Record parallel processing metrics
                        RecordParallelProcessingMetrics();

                        MetricUtils.ExportMetrics(streamMetricsRegistry, configuration, Name);
                        lastMetrics = DateTime.Now;
                    }
                }
            }
            finally
            {
                CompleteShutdown();
            }
        }

        private void CompleteShutdown()
        {
            try
            {
                if (!IsDisposable)
                {
                    IsRunning = false;

                    if (State != ThreadState.PENDING_SHUTDOWN)
                        SetState(ThreadState.PENDING_SHUTDOWN);

                    CommitOffsets(true);

                    var consumer = GetConsumer();
                    var consumerName = consumer.Name;
                    consumer.Unsubscribe();
                    consumer.Close();
                    consumer.Dispose();

                    // Close the processing strategy
                    if (processingStrategy != null)
                    {
                        processingStrategy.Close();
                        processingStrategy.Dispose();
                    }

                    externalProcessorTopologies
                        .Values
                        .ForEach(e => e.Close());

                    // if one delete request is in progress, we wait the result before closing the manager
                    if (currentDeleteTask is {IsCompleted: false})
                        currentDeleteTask.GetAwaiter().GetResult();

                    adminClient?.Dispose();

                    externalProcessorTopologies.Clear();
                    streamMetricsRegistry.RemoveThreadSensors(Name);
                    streamMetricsRegistry.RemoveLibrdKafkaSensors(Name, consumerName);
                    log.LogInformation($"{logPrefix}Shutdown complete");
                    IsDisposable = true;
                }
            }
            catch (Exception e)
            {
                log.LogError(e,
                    "{LogPrefix}Failed to close external stream thread due to the following error:", logPrefix);
            }
            finally
            {
                SetState(ThreadState.DEAD);
            }
        }

        private bool CommitOffsets(bool clearBuffer)
        {
            // Flush the processing strategy
            processingStrategy.Flush();

            // For sequential strategy, also handle buffer clearing
            if (clearBuffer && processingStrategy is SequentialProcessingStrategy sequentialStrategy)
            {
                sequentialStrategy.ClearBuffers();
            }

            // Get committable offsets from the strategy
            var offsets = processingStrategy.GetCommittableOffsets().ToList();

            if (offsets.Any())
            {
                var consumer = GetConsumer();
                consumer.Commit(offsets);

                // purge records offsets
                if (currentDeleteTask == null || currentDeleteTask.IsCompleted)
                {
                    if (currentDeleteTask != null && currentDeleteTask.IsFaulted)
                        log.LogDebug(
                            $"{logPrefix}Previous delete-records request has failed. Try sending the new request now.");

                    currentDeleteTask = adminClient.DeleteRecordsAsync(offsets);
                    log.LogDebug($"Sent delete-records request: {string.Join(",", offsets)}");
                }

                // Clear committed offsets from strategy tracking
                if (processingStrategy is SequentialProcessingStrategy seq)
                {
                    seq.ClearCommittedOffsets();
                }
                else if (processingStrategy is PerPartitionProcessingStrategy perPart)
                {
                    perPart.ClearCommittedOffsets();
                }
                else if (processingStrategy is PerKeyProcessingStrategy perKey)
                {
                    perKey.ClearCommittedOffsets();
                }
                else if (processingStrategy is UnorderedProcessingStrategy unordered)
                {
                    unordered.ClearCommittedOffsets();
                }

                return true;
            }

            return false;
        }

        private bool Commit()
        {
            if (DateTime.Now - lastCommit > TimeSpan.FromMilliseconds(configuration.CommitIntervalMs))
            {
                DateTime beginCommit = DateTime.Now;
                log.LogDebug($"Committing all topic/partitions since {(DateTime.Now - lastCommit).TotalMilliseconds}ms has elapsed (commit interval is {configuration.CommitIntervalMs}ms)");
                bool committed = CommitOffsets(false);
                log.LogDebug($"Committed all topic/partitions in {(DateTime.Now - beginCommit).TotalMilliseconds}ms");
                lastCommit = DateTime.Now;
                return committed;
            }
            return false;
        }
        
        private void CloseThread()
        {
            try
            {
                log.LogInformation($"{logPrefix}Shutting down");
                SetState(ThreadState.PENDING_SHUTDOWN);
                
                thread.Join();
            }
            catch (Exception e)
            {
                log.LogError(e,
                    "{LogPrefix}Failed to close external stream thread due to the following error:", logPrefix);
            }
        }

        public void Start(CancellationToken token)
        {
            log.LogInformation("{LogPrefix}Starting", logPrefix);
            if (SetState(ThreadState.STARTING) == null)
            {
                log.LogInformation($"{logPrefix}StreamThread already shutdown. Not running");
                IsRunning = false;
                return;
            }
            
            IsRunning = true;
            
            if(configuration.Guarantee == ProcessingGuarantee.EXACTLY_ONCE)
                log.LogWarning($"Be carefully the processing guarantee 'EXACTLY_ONCE' is not guarantee with an external service. This behavior is use for processing topic-to-topic. Downstreams consumer must be idempotent.");
            
            currentConsumer = GetConsumer();
            adminClient = kafkaSupplier.GetAdmin(configuration.ToAdminConfig(clientId));

            producer = new StreamsProducer(configuration, Name, Guid.NewGuid(), kafkaSupplier, logPrefix);

            //producer = kafkaSupplier.GetProducer(configuration.ToExternalProducerConfig($"{thread.Name}-producer").Wrap(Name, configuration));

            // Create the processing strategy based on configuration
            processingStrategy = CreateProcessingStrategy();
            if (processingStrategy is PerPartitionProcessingStrategy perPartitionStrategy)
            {
                perPartitionStrategy.Start();
            }
            else if (processingStrategy is PerKeyProcessingStrategy perKeyStrategy)
            {
                perKeyStrategy.Start();
            }
            else if (processingStrategy is UnorderedProcessingStrategy unorderedStrategy)
            {
                unorderedStrategy.Start();
            }
            else if (processingStrategy is SequentialProcessingStrategy sequentialStrategy)
            {
                sequentialStrategy.Start();
            }

            SetState(ThreadState.PARTITIONS_ASSIGNED);
            thread.Start();     
        }

        private ThreadState SetState(ThreadState newState)
        {
            var oldState = State;
            if (State.IsValidTransition(newState))
                State = newState;
            else
                throw new StreamsException($"{logPrefix}Unexpected state transition from {State} to {newState}");
            
            StateChanged?.Invoke(this, oldState, State);
            
            return State;
        }

        public IEnumerable<ITask> ActiveTasks => throw new NotSupportedException();

        public event ThreadStateListener StateChanged;

        private ExternalProcessorTopologyExecutor GetExternalProcessorTopology(string topic)
        {
            if (externalProcessorTopologies.TryGetValue(topic, out var processorTopology))
                return processorTopology;

            var taskId = internalTopologyBuilder.GetTaskIdFromPartition(new TopicPartition(topic, Partition.Any));
            var topology = internalTopologyBuilder.BuildTopology(taskId, configuration);

            ExternalProcessorTopologyExecutor externalProcessorTopologyExecutor = new ExternalProcessorTopologyExecutor(
                Name,
                taskId,
                topology.GetSourceProcessor(topic),
                producer,
                configuration,
                streamMetricsRegistry,
                adminClient);
            externalProcessorTopologies.Add(topic, externalProcessorTopologyExecutor);

            return externalProcessorTopologyExecutor;
        }

        private IProcessingStrategy CreateProcessingStrategy()
        {
            var config = configuration.ExternalProcessingConfig;

            // Validate configuration
            config?.Validate();

            switch (config?.Mode ?? ParallelProcessingMode.SEQUENTIAL)
            {
                case ParallelProcessingMode.SEQUENTIAL:
                    log.LogInformation($"{logPrefix}Using SEQUENTIAL processing strategy");
                    return new SequentialProcessingStrategy(
                        Name,
                        internalTopologyBuilder,
                        producer,
                        configuration,
                        streamMetricsRegistry,
                        adminClient);

                case ParallelProcessingMode.PER_PARTITION:
                    log.LogInformation($"{logPrefix}Using PER_PARTITION processing strategy (max concurrency: {config.MaxConcurrency})");
                    return new PerPartitionProcessingStrategy(
                        Name,
                        internalTopologyBuilder,
                        producer,
                        configuration,
                        config,
                        streamMetricsRegistry,
                        adminClient);

                case ParallelProcessingMode.PER_KEY:
                    log.LogInformation($"{logPrefix}Using PER_KEY processing strategy (max concurrency: {config.MaxConcurrency})");
                    return new PerKeyProcessingStrategy(
                        Name,
                        internalTopologyBuilder,
                        producer,
                        configuration,
                        config,
                        streamMetricsRegistry,
                        adminClient);

                case ParallelProcessingMode.UNORDERED:
                    log.LogInformation($"{logPrefix}Using UNORDERED processing strategy (max concurrency: {config.MaxConcurrency})");
                    return new UnorderedProcessingStrategy(
                        Name,
                        internalTopologyBuilder,
                        producer,
                        configuration,
                        config,
                        streamMetricsRegistry,
                        adminClient);

                default:
                    throw new ArgumentException($"{logPrefix}Unknown processing mode: {config.Mode}");
            }
        }

        private void RecordParallelProcessingMetrics()
        {
            if (processingStrategy == null)
                return;

            var now = DateTime.Now.GetMilliseconds();

            // Record in-flight records
            parallelInFlightRecordsSensor.Record(processingStrategy.InFlightCount, now);

            // Record queue depth and worker count for parallel strategies
            if (processingStrategy is PerPartitionProcessingStrategy perPartitionStrategy)
            {
                parallelQueueDepthSensor.Record(perPartitionStrategy.QueuedRecordsCount, now);
                parallelWorkerCountSensor.Record(configuration.ExternalProcessingConfig.MaxConcurrency, now);
            }
            else if (processingStrategy is PerKeyProcessingStrategy perKeyStrategy)
            {
                parallelQueueDepthSensor.Record(perKeyStrategy.QueuedRecordsCount, now);
                parallelWorkerCountSensor.Record(configuration.ExternalProcessingConfig.MaxConcurrency, now);
            }
            else if (processingStrategy is UnorderedProcessingStrategy unorderedStrategy)
            {
                parallelQueueDepthSensor.Record(unorderedStrategy.QueuedRecordsCount, now);
                parallelWorkerCountSensor.Record(configuration.ExternalProcessingConfig.MaxConcurrency, now);
            }
            else
            {
                // For sequential strategy, queue depth and workers are always 0/1
                parallelQueueDepthSensor.Record(0, now);
                parallelWorkerCountSensor.Record(1, now);
            }
        }
        
        private ExceptionHandlerResponse TreatException(Exception exception)
        {
            if (exception is DeserializationException || exception is ProductionException)
            {
                return ExceptionHandlerResponse.FAIL;
            }
            var response = configuration.InnerExceptionHandler(exception);
            return response;
        }

        private IConsumer<byte[], byte[]> GetConsumer()
        {
            lock (@lock)
            {
                if (currentConsumer == null)
                {
                    var consumerConfig = configuration.ToExternalConsumerConfig($"{thread.Name}-consumer").Wrap(Name, configuration);
                    currentConsumer = kafkaSupplier.GetConsumer(consumerConfig, null);
                    currentConsumer.Subscribe(externalSourceTopics);
                }

                return currentConsumer;
            }
        }
        
        private void HandleInnerException()
        {
            log.LogWarning($"{logPrefix}Detected that the thread throw an inner exception. Your configuration manager has decided to continue running stream processing. So will close out all assigned tasks and rejoin the consumer group");

            CommitOffsets(true);

            var consumer = GetConsumer();
            var librdkafkaClientId = consumer.Name;

            consumer.Unsubscribe();
            consumer.Close();
            consumer.Dispose();
            currentConsumer = null;

            // Close and recreate the processing strategy
            if (processingStrategy != null)
            {
                processingStrategy.Close();
                processingStrategy.Dispose();
                processingStrategy = CreateProcessingStrategy();
                if (processingStrategy is PerPartitionProcessingStrategy perPartitionStrategy)
                {
                    perPartitionStrategy.Start();
                }
                else if (processingStrategy is PerKeyProcessingStrategy perKeyStrategy)
                {
                    perKeyStrategy.Start();
                }
                else if (processingStrategy is UnorderedProcessingStrategy unorderedStrategy)
                {
                    unorderedStrategy.Start();
                }
                else if (processingStrategy is SequentialProcessingStrategy sequentialStrategy)
                {
                    sequentialStrategy.Start();
                }
            }

            streamMetricsRegistry.RemoveLibrdKafkaSensors(Name, librdkafkaClientId);
        }
    }
}