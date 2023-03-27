using Confluent.Kafka;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Kafka;
using Streamiz.Kafka.Net.Kafka.Internal;
using Streamiz.Kafka.Net.Processors.Internal;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Metrics.Internal;

namespace Streamiz.Kafka.Net.Processors
{
    internal class StreamThread : IThread
    {
        #region Static 

        public static string GetTaskProducerClientId(string threadClientId, TaskId taskId)
        {
            return threadClientId + "-" + taskId + "-streamiz-producer";
        }

        public static string GetThreadProducerClientId(string threadClientId)
        {
            return threadClientId + "-streamiz-producer";
        }

        public static string GetConsumerClientId(string threadClientId)
        {
            return threadClientId + "-streamiz-consumer";
        }

        public static string GetRestoreConsumerClientId(string threadClientId)
        {
            return threadClientId + "-streamiz-restore-consumer";
        }

        // currently admin client is shared among all threads
        public static string GetSharedAdminClientId(string clientId)
        {
            return clientId + "-streamiz-admin";
        }

        internal static IThread Create(string threadId, string clientId, InternalTopologyBuilder builder,
            StreamMetricsRegistry streamMetricsRegistry, IStreamConfig configuration, IKafkaSupplier kafkaSupplier,
            IAdminClient adminClient, int threadInd)
        {
            string logPrefix = $"stream-thread[{threadId}] ";
            var log = Logger.GetLogger(typeof(StreamThread));
            var customerID = $"{clientId}-StreamThread-{threadInd}";
            IProducer<byte[], byte[]> producer = null;

            // TODO : remove this limitations depends version of Kafka Cluster
            // Due to limitations outlined in KIP-447 (which KIP-447 overcomes), it is
            // currently necessary to use a separate producer per input partition. The
            // producerState dictionary is used to keep track of these, and the current
            // consumed offset.
            // https://cwiki.apache.org/confluence/display/KAFKA/KIP-447%3A+Producer+scalability+for+exactly+once+semantics
            // IF Guarantee is AT_LEAST_ONCE, producer is the same of all StreamTasks in this thread, 
            // ELSE one producer by StreamTask.
            if (configuration.Guarantee == ProcessingGuarantee.AT_LEAST_ONCE)
            {
                log.LogInformation("{LogPrefix}Creating shared producer client", logPrefix);
                producer = kafkaSupplier.GetProducer(configuration.ToProducerConfig(GetThreadProducerClientId(threadId)).Wrap(threadId));
            }

            var restoreConfig = configuration.ToRestoreConsumerConfig(GetRestoreConsumerClientId(customerID));
            var restoreConsumer = kafkaSupplier.GetRestoreConsumer(restoreConfig);

            var storeChangelogReader = new StoreChangelogReader(
                configuration,
                restoreConsumer,
                threadId,
                streamMetricsRegistry);

            var taskCreator = new TaskCreator(builder, configuration, threadId, kafkaSupplier, producer, storeChangelogReader, streamMetricsRegistry);
            var manager = new TaskManager(builder, taskCreator, adminClient, storeChangelogReader);

            var listener = new StreamsRebalanceListener(manager);

            log.LogInformation("{LogPrefix}Creating consumer client", logPrefix);
            var consumer = kafkaSupplier.GetConsumer(configuration.ToConsumerConfig(GetConsumerClientId(customerID)).Wrap(threadId), listener);
            manager.Consumer = consumer;

            var thread = new StreamThread(threadId, customerID, manager, consumer, builder, storeChangelogReader, streamMetricsRegistry, configuration);
            listener.Thread = thread;
            listener.ExceptionOnAssignment += thread.ManageExceptionDuringAssignment;

            return thread;
        }

        #endregion

        public ThreadState State { get; private set; }

        private readonly IChangelogReader changelogReader;
        private readonly StreamMetricsRegistry streamMetricsRegistry;
        private readonly IStreamConfig streamConfig;
        private readonly ILogger log = Logger.GetLogger(typeof(StreamThread));
        private readonly Thread thread;
        private readonly IConsumer<byte[], byte[]> consumer;
        private readonly TaskManager manager;
        private readonly InternalTopologyBuilder builder;
        private readonly TimeSpan consumeTimeout;
        private readonly string threadId;
        private readonly string clientId;
        private readonly string logPrefix;
        private readonly long commitTimeMs = 0;
        private CancellationToken token;
        private DateTime lastCommit = DateTime.Now;
        private DateTime lastMetrics = DateTime.Now;
        private DateTime lastSummaryMs;
        private long summaryProcessed;
        private long summaryPunctuated;
        private long summaryComitted;

        private int numIterations = 1;
        private long lastPollMs;

        private readonly object stateLock = new object();
        
        private readonly Sensor commitSensor;
        private readonly Sensor pollSensor;
        private readonly Sensor punctuateSensor;
        private readonly Sensor pollRecordsSensor;
        private readonly Sensor pollRatioSensor;
        private readonly Sensor processLatencySensor;
        private readonly Sensor processRecordsSensor;
        private readonly Sensor processRateSensor;
        private readonly Sensor processRatioSensor;
        private readonly Sensor punctuateRatioSensor;
        private readonly Sensor commitRatioSensor;
        private Exception exception;

        public event ThreadStateListener StateChanged;

        private StreamThread(string threadId, string clientId, TaskManager manager, IConsumer<byte[], byte[]> consumer,
            InternalTopologyBuilder builder, IChangelogReader storeChangelogReader,
            StreamMetricsRegistry streamMetricsRegistry, IStreamConfig configuration)
            : this(threadId, clientId, manager, consumer, builder, storeChangelogReader, streamMetricsRegistry, TimeSpan.FromMilliseconds(configuration.PollMs), configuration.CommitIntervalMs)
        {
            streamConfig = configuration;
        }

        private StreamThread(string threadId, string clientId, TaskManager manager, IConsumer<byte[], byte[]> consumer, InternalTopologyBuilder builder, IChangelogReader storeChangelogReader, StreamMetricsRegistry streamMetricsRegistry, TimeSpan timeSpan, long commitInterval)
        {
            this.manager = manager;
            this.consumer = consumer;
            this.builder = builder;
            consumeTimeout = timeSpan;
            this.threadId = threadId;
            this.clientId = clientId;
            logPrefix = $"stream-thread[{threadId}] ";
            commitTimeMs = commitInterval;
            changelogReader = storeChangelogReader;
            this.streamMetricsRegistry = streamMetricsRegistry;

            thread = new Thread(Run);
            thread.Name = this.threadId;
            Name = this.threadId;

            State = ThreadState.CREATED;

            commitSensor = ThreadMetrics.CommitSensor(threadId, streamMetricsRegistry);
            pollSensor = ThreadMetrics.PollSensor(threadId, streamMetricsRegistry);
            punctuateSensor = ThreadMetrics.PunctuateSensor(threadId, streamMetricsRegistry);
            pollRecordsSensor = ThreadMetrics.PollRecordsSensor(threadId, streamMetricsRegistry);
            pollRatioSensor = ThreadMetrics.PollRatioSensor(threadId, streamMetricsRegistry);
            processLatencySensor = ThreadMetrics.ProcessLatencySensor(threadId, streamMetricsRegistry);
            processRecordsSensor = ThreadMetrics.ProcessRecordsSensor(threadId, streamMetricsRegistry);
            processRateSensor = ThreadMetrics.ProcessRateSensor(threadId, streamMetricsRegistry);
            processRatioSensor = ThreadMetrics.ProcessRatioSensor(threadId, streamMetricsRegistry);
            commitRatioSensor = ThreadMetrics.CommitRatioSensor(threadId, streamMetricsRegistry);
            punctuateRatioSensor = ThreadMetrics.PunctuateRatioSensor(threadId, streamMetricsRegistry);
        }

        #region IThread Impl

        public string Name { get; }

        public bool IsRunning { get; private set; } = false;

        public bool IsDisposable { get; private set; } = false;

        public int Id => thread.ManagedThreadId;

        public void Dispose() => CloseThread();
        
        public void Run()
        {
            exception = null;
            lastSummaryMs = DateTime.Now;
            long totalProcessLatency = 0, totalCommitLatency = 0, totalPunctuateLatency = 0;

            try
            {
                if (IsRunning)
                {
                    while (!token.IsCancellationRequested)
                    {
                        #region Treat Exception

                        /*
                         * exception variable is set in try { ... }catch {..} just after.
                         * TreatException(..) close consumer, taskManager, flush all state stores and return an enumeration (FAIL if thread must stop, CONTINUE if developer want to continue stream processing).
                         * IF response == ExceptionHandlerResponse.FAIL, break infinte thread loop.
                         * ELSE IF response == ExceptionHandlerResponse.CONTINUE, reset exception variable, flush all states, revoked partitions, and re-subscribe source topics to resume processing.
                         * exception behavior is implemented in the begin of infinite loop to be more readable, catch block just setted exception variable and log content to track it.
                        */
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

                        #endregion

                        try
                        {
                            if (!manager.RebalanceInProgress)
                            {
                                RestorePhase();

                                long now = DateTime.Now.GetMilliseconds();
                                long startMs = now;
                                IEnumerable<ConsumeResult<byte[], byte[]>> records =
                                    new List<ConsumeResult<byte[], byte[]>>();

                                long pollLatency = ActionHelper.MeasureLatency(() =>
                                {
                                    records = PollRequest(GetTimeout());
                                });
                                pollSensor.Record(pollLatency, now);

                                DateTime n = DateTime.Now;
                                var count = AddToTasks(records);
                                if (count > 0)
                                {
                                    log.LogDebug($"Add {count} records in tasks in {DateTime.Now - n}");
                                    pollRecordsSensor.Record(count, now);
                                }

                                n = DateTime.Now;
                                int processed = 0, totalProcessed = 0;
                                long timeSinceLastPoll = 0;
                                do
                                {
                                    processed = 0;
                                    now = DateTime.Now.GetMilliseconds();
                                    for (int i = 0; i < numIterations; ++i)
                                    {
                                        long processLatency = 0;

                                        if (!manager.RebalanceInProgress)
                                            processLatency = ActionHelper.MeasureLatency(() =>
                                            {
                                                processed = manager.Process(now);
                                            });
                                        else
                                            processed = 0;

                                        totalProcessed += processed;
                                        summaryProcessed += processed;
                                        totalProcessLatency += processLatency;

                                        if (processed == 0)
                                            break;

                                        processLatencySensor.Record((double) processLatency / processed, now);
                                        processRateSensor.Record(processed, now);

                                        // not available now, when the user request a commit, it will commit for the next occurence
                                        //if (processed > 0)
                                        //    manager.MaybeCommitPerUserRequested();
                                        //else
                                        //    break;
                                    }

                                    int punctuated = 0;
                                    var punctuateLatency = ActionHelper.MeasureLatency(() =>  {
                                        punctuated = manager.Punctuate();
                                    });
                                    totalPunctuateLatency += punctuateLatency;
                                    summaryPunctuated += punctuated;
                                    if (punctuated > 0) {
                                        punctuateSensor.Record(punctuateLatency / (double) punctuated, now);
                                    }
                                    log.LogDebug($"{punctuated} punctuators ran.");
                                    
                                    timeSinceLastPoll = Math.Max(DateTime.Now.GetMilliseconds() - lastPollMs, 0);

                                    int commited = 0;
                                    long commitLatency = ActionHelper.MeasureLatency(() => commited = Commit());
                                    totalCommitLatency += commitLatency;
                                    if (commited > 0)
                                    {
                                        summaryComitted += commited;
                                        commitSensor.Record(commitLatency / (double) commited, now);
                                        numIterations = numIterations > 1 ? numIterations / 2 : numIterations;
                                    }
                                    else if (timeSinceLastPoll > streamConfig.MaxPollIntervalMs.Value / 2)
                                    {
                                        numIterations = numIterations > 1 ? numIterations / 2 : numIterations;
                                        break;
                                    }
                                    else if (processed > 0)
                                    {
                                        numIterations++;
                                    }

                                } while (processed > 0);

                                if (State == ThreadState.RUNNING)
                                    totalCommitLatency += ActionHelper.MeasureLatency(() => Commit());

                                if (State == ThreadState.PARTITIONS_ASSIGNED)
                                    SetState(ThreadState.RUNNING);

                                now = DateTime.Now.GetMilliseconds();
                                double runOnceLatency = (double) now - startMs;

                                if (totalProcessed > 0)
                                    log.LogDebug($"Processing {totalProcessed} records in {DateTime.Now - n}");

                                processRecordsSensor.Record(totalProcessed, now);
                                processRatioSensor.Record(totalProcessLatency / runOnceLatency, now);
                                pollRatioSensor.Record(pollLatency / runOnceLatency, now);
                                commitRatioSensor.Record(totalCommitLatency / runOnceLatency, now);
                                punctuateRatioSensor.Record(totalPunctuateLatency / runOnceLatency, now);
                                
                                totalProcessLatency = 0;
                                totalCommitLatency = 0;
                                totalPunctuateLatency = 0;

                                var dt = DateTime.Now;
                                if (lastMetrics.Add(TimeSpan.FromMilliseconds(streamConfig.MetricsIntervalMs)) < dt)
                                {
                                    MetricUtils.ExportMetrics(streamMetricsRegistry, streamConfig, Name);
                                    lastMetrics = dt;
                                }

                                if (lastSummaryMs.Add(streamConfig.LogProcessingSummary) < dt)
                                {
                                    log.LogInformation(
                                        $"Processed {summaryProcessed} total records, ran {summaryPunctuated} punctuators and committed {summaryComitted} total tasks since the last update");
                                    summaryProcessed = 0;
                                    summaryComitted = 0;
                                    summaryPunctuated = 0;
                                    lastSummaryMs = dt;
                                }
                                    

                            }
                            else
                                Thread.Sleep((int) consumeTimeout.TotalMilliseconds);
                        }
                        catch (TaskMigratedException e)
                        {
                            HandleTaskMigrated(e);
                        }
                        catch (KafkaException e)
                        {
                            exception = e;
                            log.LogError(e,

                                "{LogPrefix}Encountered the following unexpected Kafka exception during processing, this usually indicate Streams internal errors:",
                                logPrefix);
                        }
                        catch (Exception e)
                        {
                            exception = e;
                            log.LogError(exception, "{LogPrefix}Encountered the following error during processing:",
                                logPrefix);
                        }
                    }
                }
            }
            finally
            {
                CompleteShutdown();
            }
        }
        
        private void RestorePhase()
        {
            if(State == ThreadState.PARTITIONS_ASSIGNED || State == ThreadState.RUNNING && manager.NeedRestoration())
            {
                log.LogDebug($"{logPrefix} State is {State}, initializing and restoring tasks if necessary");

                if (manager.TryToCompleteRestoration())
                {
                    log.LogInformation($"Restoration took {DateTime.Now.GetMilliseconds() - LastPartitionAssignedTime}ms for all tasks {string.Join(",", manager.ActiveTaskIds)}");
                    if(State == ThreadState.PARTITIONS_ASSIGNED)
                        SetState(ThreadState.RUNNING);
                }

                changelogReader.Restore();
            }
        }

        private TimeSpan GetTimeout()
        {
            if (State == ThreadState.PARTITIONS_ASSIGNED || State == ThreadState.PARTITIONS_REVOKED || State == ThreadState.PENDING_SHUTDOWN)
                return TimeSpan.Zero;
            if (State == ThreadState.RUNNING || State == ThreadState.STARTING)
                return consumeTimeout;

            log.LogError("{LogPrefix}Unexpected state {State} during normal iteration", logPrefix, State);
            throw new StreamsException($"Unexpected state {State} during normal iteration");
        }

        private int AddToTasks(IEnumerable<ConsumeResult<byte[], byte[]>> records)
        {
            int count = 0;
            foreach (var record in records)
            {
                count++;
                var task = manager.ActiveTaskFor(record.TopicPartition);
                if(task != null)
                {
                    if (task.IsClosed)
                    {
                        log.LogInformation(
                            
                                "Stream task {TaskId} is already closed, probably because it got unexpectedly migrated to another thread already. Notifying the thread to trigger a new rebalance immediately",
                                task.Id);
                        // TODO gesture this behaviour
                        //throw new TaskMigratedException(task);
                    }
                    else
                        task.AddRecord(record);
                }
                else if (consumer.Assignment.Contains(record.TopicPartition))
                {
                    log.LogError(
                        "Unable to locate active task for received-record partition {TopicPartition}. Current tasks: {TaskIDs}. Current Consumer Assignment : {Assignment}",
                        record.TopicPartition, string.Join(",", manager.ActiveTaskIds),
                        string.Join(",", consumer.Assignment.Select(t => $"{t.Topic}-[{t.Partition}]")));
                    throw new NullReferenceException($"Task was unexpectedly missing for partition {record.TopicPartition}");
                }                
            }
            return count;
        }

        private ExceptionHandlerResponse TreatException(Exception exception)
        {
            if (exception is DeserializationException || exception is ProductionException)
            {
                return ExceptionHandlerResponse.FAIL;
            }
            var response = streamConfig.InnerExceptionHandler(exception);
            return response;
        }

        public void Start(CancellationToken token)
        {
            log.LogInformation("{LogPrefix}Starting", logPrefix);
            if (SetState(ThreadState.STARTING) == null)
            {
                log.LogInformation("{LogPrefix}StreamThread already shutdown. Not running", logPrefix);
                IsRunning = false;
                return;
            }

            this.token = token;
            IsRunning = true;
            consumer.Subscribe(builder.GetSourceTopics());
            thread.Start();
            
            ThreadMetrics.CreateStartThreadSensor(threadId, DateTime.Now.GetMilliseconds(), streamMetricsRegistry);
        }

        public IEnumerable<ITask> ActiveTasks => manager.ActiveTasks;

        public long LastPartitionAssignedTime { get; internal set; }

        #endregion

        private void ManageExceptionDuringAssignment(Exception e, IEnumerable<TopicPartition> partitions)
        {
            log.LogError(e,

                $"{logPrefix}Encountered the following unexpected Kafka exception during assignment partitions ({string.Join(",", partitions)}), this usually indicate Streams internal errors");
            exception = e;
        }
        
        private void HandleTaskMigrated(TaskMigratedException e)
        {
            log.LogWarning(e,
                
                    "{LogPrefix}Detected that the thread is being fenced. This implies that this thread missed a rebalance and dropped out of the consumer group. Will close out all assigned tasks and rejoin the consumer group",
                    logPrefix);

            manager.HandleLostAll();
            consumer.Unsubscribe();
            consumer.Subscribe(builder.GetSourceTopics());
        }

        private void HandleInnerException()
        {
            log.LogWarning(
                
                    "{LogPrefix}Detected that the thread throw an inner exception. Your configuration manager has decided to continue running stream processing. So will close out all assigned tasks and rejoin the consumer group",
                    logPrefix);

            manager.HandleLostAll();
            consumer.Unsubscribe();
            consumer.Subscribe(builder.GetSourceTopics());
        }

        private int Commit()
        {
            int committed = 0;
            if (DateTime.Now - lastCommit > TimeSpan.FromMilliseconds(commitTimeMs))
            {
                DateTime beginCommit = DateTime.Now;
                log.LogDebug(
                    "Committing all active tasks {TaskIDs} since {DateTime}ms has elapsed (commit interval is {CommitTime}ms)",
                    string.Join(",", manager.ActiveTaskIds), (DateTime.Now - lastCommit).TotalMilliseconds,
                    commitTimeMs);
                committed = manager.CommitAll();
                if (committed > 0)
                    log.LogDebug("Committed all active tasks {TaskIDs} in {TimeElapsed}ms",
                        string.Join(",", manager.ActiveTaskIds), (DateTime.Now - beginCommit).TotalMilliseconds);

                if (committed == -1)
                {
                    log.LogDebug("Unable to commit as we are in the middle of a rebalance, will try again when it completes");
                }
                else
                {
                    lastCommit = DateTime.Now;
                }
            }

            return committed;
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
                    "{LogPrefix}Failed to close stream thread due to the following error:", logPrefix);
            }
        }

        private void CompleteShutdown()
        {
            try
            {
                if (!IsDisposable)
                {
                    log.LogInformation("{LogPrefix}Shutting down", logPrefix);

                    SetState(ThreadState.PENDING_SHUTDOWN);

                    IsRunning = false;

                    manager.Close();

                    consumer.Unsubscribe();
                    consumer.Close();
                    consumer.Dispose();

                    streamMetricsRegistry.RemoveThreadSensors(threadId);
                    log.LogInformation($"{logPrefix}Shutdown complete");
                    IsDisposable = true;
                }
            }
            catch (Exception e)
            {
                log.LogError(e,
                    "{LogPrefix}Failed to close stream thread due to the following error:", logPrefix);
            }
            finally
            {
                SetState(ThreadState.DEAD);
            }
        }
        
        private IEnumerable<ConsumeResult<byte[], byte[]>> PollRequest(TimeSpan ts)
        {
            lastPollMs = DateTime.Now.GetMilliseconds();
            return consumer.ConsumeRecords(ts, streamConfig.MaxPollRecords);
        }

        internal ThreadState SetState(ThreadState newState)
        {
            ThreadState oldState;

            lock (stateLock)
            {
                oldState = State;

                if (State == ThreadState.PENDING_SHUTDOWN && newState != ThreadState.DEAD)
                {
                    log.LogDebug(
                        "{LogPrefix}Ignoring request to transit from PENDING_SHUTDOWN to {NewState}: only DEAD state is a valid next state",
                        logPrefix, newState);
                    // when the state is already in PENDING_SHUTDOWN, all other transitions will be
                    // refused but we do not throw exception here
                    return null;
                }
                else if (State == ThreadState.DEAD)
                {
                    log.LogDebug(
                        "{LogPrefix}Ignoring request to transit from DEAD to {NewState}: no valid next state after DEAD", logPrefix,
                        newState);
                    // when the state is already in NOT_RUNNING, all its transitions
                    // will be refused but we do not throw exception here
                    return null;
                }
                else if (!State.IsValidTransition(newState))
                {
                    string logPrefix = "";
                    log.LogError("{LogPrefix}Unexpected state transition from {OldState} to {NewState}", logPrefix, oldState,
                        newState);
                    throw new StreamsException($"{logPrefix}Unexpected state transition from {oldState} to {newState}");
                }
                else
                {
                    log.LogInformation("{LogPrefix}State transition from {OldState} to {NewState}", logPrefix, oldState,
                        newState);
                }

                State = newState;
            }

            StateChanged?.Invoke(this, oldState, State);

            return oldState;
        }

        // FOR TEST
        internal IEnumerable<TopicPartitionOffset> GetCommittedOffsets(IEnumerable<TopicPartition> partitions, TimeSpan timeout)
            => consumer.Committed(partitions, timeout);
    }
}