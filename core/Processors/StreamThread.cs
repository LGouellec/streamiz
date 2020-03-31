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
    public class StreamThread : IThread
    {
        #region State Thread
        /**
         * Stream thread states are the possible states that a stream thread can be in.
         * A thread must only be in one state at a time
         * The expected state transitions with the following defined states is:
         *
         * <pre>
         *                 +-------------+
         *          +<---- | Created (0) |
         *          |      +-----+-------+
                 *          |            |
                 *          |            v
                 *          |      +-----+-------+
                 *          +<---- | Starting (1)|----->+
                 *          |      +-----+-------+      |
                 *          |            |              |
                 *          |            |              |
                 *          |            v              |
                 *          |      +-----+-------+      |
                 *          +<---- | Partitions  |      |
                 *          |      | Revoked (2) | <----+
                 *          |      +-----+-------+      |
                 *          |           |  ^            |
                 *          |           |  |            |
                 *          |           v  |            |
                 *          |      +-----+-------+      |
                 *          +<---- | Partitions  |      |
                 *          |      | Assigned (3)| <----+
                 *          |      +-----+-------+      |
                 *          |            |              |
                 *          |            |              |
                 *          |            v              |
                 *          |      +-----+-------+      |
                 *          |      | Running (4) | ---->+
                 *          |      +-----+-------+
                 *          |            |
                 *          |            |
                 *          |            v
                 *          |      +-----+-------+
                 *          +----> | Pending     |
                 *                 | Shutdown (5)|
                 *                 +-----+-------+
                 *                       |
                 *                       v
                 *                 +-----+-------+
                 *                 | Dead (6)    |
                 *                 +-------------+
                 * </pre>
                 *
                 * Note the following:
                 * <ul>
                 *     <li>Any state can go to PENDING_SHUTDOWN. That is because streams can be closed at any time.</li>
                 *     <li>
                 *         State PENDING_SHUTDOWN may want to transit to some other states other than DEAD,
                 *         in the corner case when the shutdown is triggered while the thread is still in the rebalance loop.
                 *         In this case we will forbid the transition but will not treat as an error.
                 *     </li>
                 *     <li>
                 *         State PARTITIONS_REVOKED may want transit to itself indefinitely, in the corner case when
                 *         the coordinator repeatedly fails in-between revoking partitions and assigning new partitions.
                 *         Also during streams instance start up PARTITIONS_REVOKED may want to transit to itself as well.
                 *         In this case we will allow the transition but it will be a no-op as the set of revoked partitions
                 *         should be empty.
                 *     </li>
                 * </ul>
             */
        internal class State : ThreadStateTransitionValidator
        {
            public static State CREATED = new State(1, 5).Order(0).Name("CREATED");
            public static State STARTING = new State(2, 3, 5).Order(1).Name("STARTING");
            public static State PARTITIONS_REVOKED = new State(2, 3, 5).Order(2).Name("PARTITIONS_REVOKED");
            public static State PARTITIONS_ASSIGNED = new State(2, 3, 4, 5).Order(3).Name("PARTITIONS_ASSIGNED");
            public static State RUNNING = new State(2, 3, 5).Order(4).Name("RUNNING");
            public static State PENDING_SHUTDOWN = new State(6).Order(5).Name("PENDING_SHUTDOWN");
            public static State DEAD = new State().Order(6).Name("DEAD");

            private ISet<int> validTransitions = new HashSet<int>();
            private int ordinal = 0;
            private string name;

            private State(params int[] validTransitions)
            {
                this.validTransitions.AddRange(validTransitions);
            }

            private State Order(int ordinal)
            {
                this.ordinal = ordinal;
                return this;
            }

            private State Name(string name)
            {
                this.name = name;
                return this;
            }

            public bool IsRunning()
            {
                return this.Equals(RUNNING) || this.Equals(STARTING) || this.Equals(PARTITIONS_REVOKED) || this.Equals(PARTITIONS_ASSIGNED);
            }

            public bool IsValidTransition(ThreadStateTransitionValidator newState)
            {
                return validTransitions.Contains(((State)newState).ordinal);
            }

            public static bool operator ==(State a, State b) => a?.ordinal == b?.ordinal;
            public static bool operator !=(State a, State b) => a?.ordinal != b?.ordinal;

            public override bool Equals(object obj)
            {
                return obj is State && ((State)obj).ordinal.Equals(this.ordinal);
            }

            public override int GetHashCode()
            {
                return this.ordinal.GetHashCode();
            }

            public override string ToString()
            {
                return $"State Thread : {this.name}";
            }
        }

        #endregion

        #region Static 

        private static String getTaskProducerClientId(String threadClientId, TaskId taskId)
        {
            return threadClientId + "-" + taskId + "-producer";
        }

        private static String getThreadProducerClientId(String threadClientId)
        {
            return threadClientId + "-producer";
        }

        private static String getConsumerClientId(String threadClientId)
        {
            return threadClientId + "-consumer";
        }

        private static String getRestoreConsumerClientId(String threadClientId)
        {
            return threadClientId + "-restore-consumer";
        }

        // currently admin client is shared among all threads
        public static String getSharedAdminClientId(String clientId)
        {
            return clientId + "-admin";
        }

        internal static IThread Create(string threadId, string clientId, InternalTopologyBuilder builder, IStreamConfig configuration, IKafkaSupplier kafkaSupplier, IAdminClient adminClient)
        {
            var producer = kafkaSupplier.GetProducer(configuration.ToProducerConfig());

            var taskCreator = new TaskCreator(builder, configuration, threadId, kafkaSupplier, producer);
            var manager = new TaskManager(taskCreator, adminClient);

            var listener = new StreamsRebalanceListener(manager);
            var consumer = kafkaSupplier.GetConsumer(configuration.ToConsumerConfig(clientId), listener);

            manager.UseConsumer(consumer);

            var thread = new StreamThread(threadId, clientId, manager, consumer, builder, TimeSpan.FromMilliseconds(1000));
            listener.UseStreamThread(thread);

            return thread;
        }

        #endregion

        internal State StateThread { get; private set; }

        private readonly Thread thread;
        private readonly IConsumer<byte[], byte[]> consumer;
        private readonly TaskManager manager;
        private readonly InternalTopologyBuilder builder;
        private readonly TimeSpan consumeTimeout;
        private readonly string threadId;
        private readonly string clientId;

        private readonly object stateLock = new object();

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

            StateThread = State.CREATED;
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
            manager.Close();
            thread.Join();
            IsDisposable = true;
        }

        public void Run()
        {
            while (IsRunning)
            {
                ConsumeResult<byte[], byte[]> record = null;

                if (StateThread == State.PARTITIONS_ASSIGNED)
                {
                    record = PollRequest(TimeSpan.Zero);
                }
                else if (StateThread == State.PARTITIONS_REVOKED)
                {
                    record = PollRequest(TimeSpan.Zero);
                }
                else if (StateThread == State.RUNNING || StateThread == State.STARTING)
                {
                    record = PollRequest(consumeTimeout);
                }
                else
                {
                    throw new StreamsException("Unexpected state " + StateThread + " during normal iteration");
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

                if (StateThread == State.PARTITIONS_ASSIGNED)
                {
                    SetState(State.RUNNING);
                }
            }
        }

        public void Start()
        {
            IsRunning = true;
            consumer.Subscribe(builder.GetSourceTopics());
            SetState(State.STARTING);
            thread.Start();
        }

        #endregion

        private ConsumeResult<byte[], byte[]> PollRequest(TimeSpan ts)
        {
            return consumer.Consume(ts);
        }

        internal State SetState(State newState)
        {
            State oldState;

            lock (stateLock)
            {
                oldState = StateThread;

                if (StateThread == State.PENDING_SHUTDOWN && newState != State.DEAD)
                {
                    // TODO
                    //log.debug("Ignoring request to transit from PENDING_SHUTDOWN to {}: " +
                    //              "only DEAD state is a valid next state", newState);
                    // when the state is already in PENDING_SHUTDOWN, all other transitions will be
                    // refused but we do not throw exception here
                    return null;
                }
                else if (StateThread == State.DEAD)
                {
                    //log.debug("Ignoring request to transit from DEAD to {}: " +
                    //              "no valid next state after DEAD", newState);
                    // when the state is already in NOT_RUNNING, all its transitions
                    // will be refused but we do not throw exception here
                    return null;
                }
                else if (!StateThread.IsValidTransition(newState))
                {
                    string logPrefix = "";
                    //log.error("Unexpected state transition from {} to {}", oldState, newState);
                    throw new StreamsException($"{logPrefix}Unexpected state transition from {oldState} to {newState}");
                }
                else
                {
                    //log.info("State transition from {} to {}", oldState, newState);
                }

                StateThread = newState;
                //if (newState == State.RUNNING)
                //{
                //    updateThreadMetadata(taskManager.activeTasks(), taskManager.standbyTasks());
                //}
                //else
                //{
                //    updateThreadMetadata(Collections.emptyMap(), Collections.emptyMap());
                //}
            }

            // TODO :
            //Listener?.onChange(this, StateThread, oldState);

            return oldState;
        }
    }
}
