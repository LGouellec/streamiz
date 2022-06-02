using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Kafka;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Processors.Internal;

namespace Streamiz.Kafka.Net.Processors
{
    // TODO : delete records offsets
    internal class ExternalStreamThread : IThread
    {
        private static readonly ILogger log = Logger.GetLogger(typeof(ExternalStreamThread));
        private static readonly object @lock = new();
        
        private readonly string clientId;
        private readonly IKafkaSupplier kafkaSupplier;
        private readonly InternalTopologyBuilder internalTopologyBuilder;
        private readonly IEnumerable<string> externalSourceTopics;
        private readonly IDictionary<string, ExternalProcessorTopology> externalProcessorTopologies =
            new Dictionary<string, ExternalProcessorTopology>();
        private readonly StreamMetricsRegistry streamMetricsRegistry;
        private readonly IStreamConfig configuration;
        private IConsumer<byte[], byte[]> currentConsumer;
        private readonly string logPrefix;
        private readonly Thread thread;
        private CancellationToken token;
        private DateTime lastCommit = DateTime.Now;
        
        private DateTime lastCheckoutProcessing = DateTime.Now;
        private readonly TimeSpan intervalCheckoutProcessing = TimeSpan.FromMinutes(1); // hard code for now
        private int messageProcessed;
        
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
            
            State = ThreadState.CREATED;

            // Todo : sensors
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
                while (!token.IsCancellationRequested)
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

                    var consumer = GetConsumer();
                    var result = consumer.Consume(TimeSpan.FromMilliseconds(configuration.PollMs));
                    
                    try
                    {
                        ExternalProcessorTopology processor = null;

                        if (result != null)
                            processor = GetExternalProcessorTopology(result.Topic);
                        else
                            processor = externalProcessorTopologies
                                .Values
                                .Where(e => e.BufferSize > 0)
                                .Random();

                        if (processor != null)
                        {
                            long processLatency = ActionHelper.MeasureLatency(() => processor.Process(result));
                            log.LogDebug($"Process one record into {processLatency} ms");
                            ++messageProcessed;

                            if (processor.State == ExternalProcessorTopology.ExternalProcessorTopologyState.BUFFER_FULL)
                            {
                                var assignmentTopic = consumer.Assignment.Where(a => a.Topic.Equals(result.Topic))
                                    .ToList();
                                consumer.Pause(assignmentTopic);
                                processor.State = ExternalProcessorTopology.ExternalProcessorTopologyState.PAUSED;
                                processor.PreviousAssignment = assignmentTopic;
                            }
                            else if (processor.State ==
                                     ExternalProcessorTopology.ExternalProcessorTopologyState.RESUMED)
                            {
                                consumer.Resume(processor.PreviousAssignment);
                                processor.State = ExternalProcessorTopology.ExternalProcessorTopologyState.RUNNING;
                                processor.PreviousAssignment = null;
                            }
                        }

                        if (DateTime.Now >= lastCheckoutProcessing.Add(intervalCheckoutProcessing))
                        {
                            log.LogInformation($"{logPrefix}Processed {messageProcessed} total records in {intervalCheckoutProcessing.TotalMilliseconds}ms");
                            lastCheckoutProcessing = DateTime.Now;
                            messageProcessed = 0;
                        }
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

                    Commit();
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
                    log.LogInformation($"{logPrefix}Shutting down");

                    SetState(ThreadState.PENDING_SHUTDOWN);

                    IsRunning = false;

                    CommitOffsets(true);

                    var consumer = GetConsumer();
                    consumer.Unsubscribe();
                    consumer.Close();
                    consumer.Dispose();

                    //  streamMetricsRegistry.RemoveThreadSensors(threadId);
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

        private void CommitOffsets(bool clearBuffer)
        {
            externalProcessorTopologies
                .Values
                .ForEach(e =>
                {
                    e.Flush();
                    if(clearBuffer)
                        e.ClearBuffer();
                });
            
            var offsets = externalProcessorTopologies
                .Values
                .SelectMany(e => e.GetCommitOffsets());
            
            var consumer = GetConsumer();
            consumer.Commit(offsets);

            externalProcessorTopologies
                .Values
                .ForEach(e => e.ClearOffsets());
        }

        private void Commit()
        {
            if (DateTime.Now - lastCommit > TimeSpan.FromMilliseconds(configuration.CommitIntervalMs))
            {
                DateTime beginCommit = DateTime.Now;
                log.LogDebug($"Committing all topic/partitions since {(DateTime.Now - lastCommit).TotalMilliseconds}ms has elapsed (commit interval is {configuration.CommitIntervalMs}ms)");
                CommitOffsets(false);
                log.LogDebug($"Committed all topic/partitions in {(DateTime.Now - beginCommit).TotalMilliseconds}ms");
                lastCommit = DateTime.Now;
            }
        }
        
        private void CloseThread()
        {
            try
            {
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

            this.token = token;
            IsRunning = true;
            
            if(configuration.Guarantee == ProcessingGuarantee.EXACTLY_ONCE)
                log.LogWarning($"Be carefully the processing guarantee 'EXACTLY_ONCE' is not guarantee with an external service. This behavior is use for processing topic-to-topic. Downstreams consumer must be idempotent.");
            
            currentConsumer = GetConsumer();
            SetState(ThreadState.PARTITIONS_ASSIGNED);
            thread.Start();     
        }

        private ThreadState SetState(ThreadState newState)
        {
            if (State.IsValidTransition(newState))
                State = newState;
            else
                throw new StreamsException($"{logPrefix}Unexpected state transition from {State} to {newState}");
            
            return State;
        }

        public IEnumerable<ITask> ActiveTasks => throw new NotSupportedException();

        public event ThreadStateListener StateChanged;

        private ExternalProcessorTopology GetExternalProcessorTopology(string topic)
        {
            if (externalProcessorTopologies.ContainsKey(topic))
                return externalProcessorTopologies[topic];
            
            var taskId = internalTopologyBuilder.GetTaskIdFromPartition(new TopicPartition(topic, 0));
            var topology = internalTopologyBuilder.BuildTopology(taskId);

            var producer = kafkaSupplier.GetProducer(configuration.ToProducerConfig($"{thread.Name}-producer-{topic}"));
            
            ExternalProcessorTopology externalProcessorTopology = new ExternalProcessorTopology(
                taskId,
                topology.GetSourceProcessor(topic),
                producer,
                configuration,
                streamMetricsRegistry);
            externalProcessorTopologies.Add(topic, externalProcessorTopology);
                
            return externalProcessorTopology;
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
                    currentConsumer = kafkaSupplier.GetConsumer(configuration.ToConsumerConfig($"{thread.Name}-consumer"), null);
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
            
            consumer.Unsubscribe();
            consumer.Close();
            consumer.Dispose();
            currentConsumer = null;
        }
    }
}