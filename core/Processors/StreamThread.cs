using Confluent.Kafka;
using kafka_stream_core.Crosscutting;
using kafka_stream_core.Errors;
using kafka_stream_core.Kafka;
using kafka_stream_core.Kafka.Internal;
using kafka_stream_core.Processors.Internal;
using System;
using System.Collections.Generic;
using System.Threading;

namespace kafka_stream_core.Processors
{
    internal class StreamThread : IThread
    {
        #region Static 

        private static string GetTaskProducerClientId(string threadClientId, TaskId taskId)
        {
            return threadClientId + "-" + taskId + "-producer";
        }

        private static string GetThreadProducerClientId(string threadClientId)
        {
            return threadClientId + "-producer";
        }

        private static string GetConsumerClientId(string threadClientId)
        {
            return threadClientId + "-consumer";
        }

        private static string GetRestoreConsumerClientId(string threadClientId)
        {
            return threadClientId + "-restore-consumer";
        }

        // currently admin client is shared among all threads
        public static string GetSharedAdminClientId(string clientId)
        {
            return clientId + "-admin";
        }

        internal static IThread Create(string threadId, string clientId, InternalTopologyBuilder builder, IStreamConfig configuration, IKafkaSupplier kafkaSupplier, IAdminClient adminClient, int threadInd)
        {
            var customerID = $"{clientId}-StreamThread-{threadInd}";
            var producer = kafkaSupplier.GetProducer(configuration.ToProducerConfig());

            var taskCreator = new TaskCreator(builder, configuration, threadId, kafkaSupplier, producer);
            var manager = new TaskManager(taskCreator, adminClient);

            var listener = new StreamsRebalanceListener(manager);
            var consumer = kafkaSupplier.GetConsumer(configuration.ToConsumerConfig(customerID), listener);
            manager.Consumer = consumer;

            var thread = new StreamThread(threadId, customerID, manager, consumer, builder, TimeSpan.FromMilliseconds(1000));
            listener.Thread = thread;

            return thread;
        }

        #endregion

        public ThreadState State { get; private set; }

        private readonly Thread thread;
        private readonly IConsumer<byte[], byte[]> consumer;
        private readonly TaskManager manager;
        private readonly InternalTopologyBuilder builder;
        private readonly TimeSpan consumeTimeout;
        private readonly string threadId;
        private readonly string clientId;
        private CancellationToken token;

        private readonly object stateLock = new object();

        public event ThreadStateListener StateChanged;

        private StreamThread(string threadId, string clientId, TaskManager manager, IConsumer<byte[], byte[]> consumer, InternalTopologyBuilder builder, TimeSpan timeSpan)
        {
            this.manager = manager;
            this.consumer = consumer;
            this.builder = builder;
            this.consumeTimeout = timeSpan;
            this.threadId = threadId;
            this.clientId = clientId;

            this.thread = new Thread(this.Run);
            this.thread.Name = this.threadId;

            State = ThreadState.CREATED;
        }

        #region IThread Impl

        public string Name { get; }

        public bool IsRunning { get; private set; } = false;

        public bool IsDisposable { get; private set; } = false;

        public int Id => thread.ManagedThreadId;

        public void Dispose()
        {
            IsRunning = false;
            consumer.Unsubscribe();
            thread.Join();
            manager.Close();
            consumer.Dispose();
            IsDisposable = true;
        }

        public void Run()
        {
            while (!token.IsCancellationRequested)
            {
                ConsumeResult<byte[], byte[]> record = null;

                if (State == ThreadState.PARTITIONS_ASSIGNED)
                {
                    record = PollRequest(TimeSpan.Zero);
                }
                else if (State == ThreadState.PARTITIONS_REVOKED)
                {
                    record = PollRequest(TimeSpan.Zero);
                }
                else if (State == ThreadState.RUNNING || State == ThreadState.STARTING)
                {
                    record = PollRequest(consumeTimeout);
                }
                else
                {
                    throw new StreamsException("Unexpected state " + State + " during normal iteration");
                }
                
                if (record != null)
                {
                    var task = manager.ActiveTaskFor(record.TopicPartition);
                    if (task != null)
                        task.AddRecords(record.TopicPartition, new List<ConsumeResult<byte[], byte[]>> { record });
                }

                foreach (var t in manager.ActiveTasks)
                {
                    if (t.CanProcess)
                    {
                        bool b = t.Process();
                        if (b && t.CommitNeeded)
                            t.Commit();
                    }
                }

                if (State == ThreadState.PARTITIONS_ASSIGNED)
                {
                    SetState(ThreadState.RUNNING);
                }
            }
        }

        public void Start(CancellationToken token)
        {
            this.token = token;
            IsRunning = true;
            consumer.Subscribe(builder.GetSourceTopics());
            SetState(ThreadState.STARTING);
            thread.Start();
        }

        #endregion

        private ConsumeResult<byte[], byte[]> PollRequest(TimeSpan ts)
        {
            return consumer.Consume(ts);
        }

        internal ThreadState SetState(ThreadState newState)
        {
            ThreadState oldState;

            lock (stateLock)
            {
                oldState = State;

                if (State == ThreadState.PENDING_SHUTDOWN && newState != ThreadState.DEAD)
                {
                    // TODO
                    //log.debug("Ignoring request to transit from PENDING_SHUTDOWN to {}: " +
                    //              "only DEAD state is a valid next state", newState);
                    // when the state is already in PENDING_SHUTDOWN, all other transitions will be
                    // refused but we do not throw exception here
                    return null;
                }
                else if (State == ThreadState.DEAD)
                {
                    //log.debug("Ignoring request to transit from DEAD to {}: " +
                    //              "no valid next state after DEAD", newState);
                    // when the state is already in NOT_RUNNING, all its transitions
                    // will be refused but we do not throw exception here
                    return null;
                }
                else if (!State.IsValidTransition(newState))
                {
                    string logPrefix = "";
                    //log.error("Unexpected state transition from {} to {}", oldState, newState);
                    throw new StreamsException($"{logPrefix}Unexpected state transition from {oldState} to {newState}");
                }
                else
                {
                    //log.info("State transition from {} to {}", oldState, newState);
                }

                State = newState;
                //if (newState == State.RUNNING)
                //{
                //    updateThreadMetadata(taskManager.activeTasks(), taskManager.standbyTasks());
                //}
                //else
                //{
                //    updateThreadMetadata(Collections.emptyMap(), Collections.emptyMap());
                //}
            }

            StateChanged?.Invoke(this, oldState, State);

            return oldState;
        }
    }
}
