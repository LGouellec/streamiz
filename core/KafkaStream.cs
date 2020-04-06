using Confluent.Kafka;
using kafka_stream_core.Crosscutting;
using kafka_stream_core.Errors;
using kafka_stream_core.Kafka;
using kafka_stream_core.Kafka.Internal;
using kafka_stream_core.Processors;
using kafka_stream_core.Stream;
using kafka_stream_core.Stream.Internal;
using System;
using System.Collections.Generic;
using System.Threading;

namespace kafka_stream_core
{
    public delegate void StateListener(KafkaStream.State oldState, KafkaStream.State newState);

    public class KafkaStream
    {
        #region State Stream

        /**
         * Kafka Streams states are the possible state that a Kafka Streams instance can be in.
         * An instance must only be in one state at a time.
         * The expected state transition with the following defined states is:
         *
         * <pre>
         *                 +--------------+
         *         +&lt;----- | Created (0)  |
         *         |       +-----+--------+
         *         |             |
         *         |             v
         *         |       +----+--+------+
         *         |       | Re-          |
         *         +&lt;----- | Balancing (1)| --------&gt;+
         *         |       +-----+-+------+          |
         *         |             | ^                 |
         *         |             v |                 |
         *         |       +--------------+          v
         *         |       | Running (2)  | --------&gt;+
         *         |       +------+-------+          |
         *         |              |                  |
         *         |              v                  |
         *         |       +------+-------+     +----+-------+
         *         +-----&gt; | Pending      |&lt;--- | Error (5)  |
         *                 | Shutdown (3) |     +------------+
         *                 +------+-------+
         *                        |
         *                        v
         *                 +------+-------+
         *                 | Not          |
         *                 | Running (4)  |
         *                 +--------------+
         *
         *
         * </pre>
         * Note the following:
         * - RUNNING state will transit to REBALANCING if any of its threads is in PARTITION_REVOKED or PARTITIONS_ASSIGNED state
         * - REBALANCING state will transit to RUNNING if all of its threads are in RUNNING state
         * - Any state except NOT_RUNNING can go to PENDING_SHUTDOWN (whenever close is called)
         * - Of special importance: If the global stream thread dies, or all stream threads die (or both) then
         *   the instance will be in the ERROR state. The user will need to close it.
     */
        public class State
        {
            public static State CREATED = new State(1, 3).Order(0).Name("CREATED");
            public static State REBALANCING = new State(2, 3, 5).Order(1).Name("REBALANCING");
            public static State RUNNING = new State(1, 2, 3, 5).Order(2).Name("RUNNING");
            public static State PENDING_SHUTDOWN = new State(4).Order(3).Name("PENDING_SHUTDOWN");
            public static State NOT_RUNNING = new State().Order(4).Name("NOT_RUNNING");
            public static State ERROR = new State(3).Order(5).Name("ERROR");

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
                return this.Equals(RUNNING) || this.Equals(REBALANCING);
            }

            internal bool IsValidTransition(State newState)
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
                return $"{this.name}";
            }
        }

        #endregion

        private readonly Topology topology;
        private readonly IStreamConfig configuration;
        private readonly IKafkaSupplier kafkaSupplier;
        private readonly IThread[] threads;
        private readonly ProcessorTopology processorTopology;
        private readonly IAdminClient adminClient;
        private readonly string clientId;

        private readonly object stateLock = new object();

        internal State StreamState { get; private set; }

        public event StateListener StateChanged;

        public KafkaStream(Topology topology, IStreamConfig configuration)
        {
            this.topology = topology;
            this.configuration = configuration;
            this.kafkaSupplier = new DefaultKafkaClientSupplier();

            var processID = Guid.NewGuid();
            clientId = string.IsNullOrEmpty(configuration.ClientId) ? $"{this.configuration.ApplicationId.ToLower()}-{processID}" : configuration.ClientId;

            // sanity check
            this.processorTopology = topology.Builder.BuildTopology();

            this.threads = new IThread[this.configuration.NumStreamThreads];
            var threadState = new Dictionary<long, Processors.ThreadState>();

            for (int i = 0; i < this.configuration.NumStreamThreads; ++i)
            {
                var threadId = $"{this.configuration.ApplicationId.ToLower()}-stream-thread-{i}";

                adminClient = this.kafkaSupplier.GetAdmin(configuration.ToAdminConfig(StreamThread.GetSharedAdminClientId(clientId)));

                this.threads[i] = StreamThread.Create(
                    threadId,
                    clientId,
                    this.topology.Builder,
                    configuration,
                    this.kafkaSupplier,
                    adminClient, 
                    i);

                threadState.Add(this.threads[i].Id, this.threads[i].State);
            }
            
            var manager = new StreamStateManager(this, threadState);
            foreach (var t in threads)
                t.StateChanged += manager.OnChange;

            StreamState = State.CREATED;
        }

        public void Start(CancellationToken token = default)
        {
            if (SetState(State.REBALANCING))
            {
                //log.debug("Starting Streams client");

                foreach (var t in threads)
                    t.Start(token);
            }
        }

        public void Close()
        {
            if (!SetState(State.PENDING_SHUTDOWN))
            {
                // if transition failed, it means it was either in PENDING_SHUTDOWN
                // or NOT_RUNNING already; just check that all threads have been stopped
                //log.info("Already in the pending shutdown state, wait to complete shutdown");
            }
            else
            {
                foreach (var t in threads)
                    t.Dispose();

                SetState(State.NOT_RUNNING);
                //log.info("Streams client stopped completely");
            }
        }

        #region Privates

        private bool WaitOnState(State targetState, long waitMs)
        {
            long begin = DateTime.Now.Millisecond;
            lock (stateLock)
            {
                long elapsedMs = 0L;
                while (this.StreamState != targetState)
                {
                    if (waitMs > elapsedMs)
                    {
                        long remainingMs = waitMs - elapsedMs;
                        Thread.Sleep((int)remainingMs);
                    }
                    else
                    {
                        //log.debug($"Cannot transit to {targetState} within {waitMs}ms");StreamState
                        return false;
                    }
                    elapsedMs = DateTime.Now.Millisecond - begin;
                }
                return true;
            }
        }

        internal bool SetState(State newState)
        {
            State oldState;

            lock(stateLock) {
                oldState = StreamState;

                if (StreamState == State.PENDING_SHUTDOWN && newState != State.NOT_RUNNING)
                {
                    // when the state is already in PENDING_SHUTDOWN, all other transitions than NOT_RUNNING (due to thread dying) will be
                    // refused but we do not throw exception here, to allow appropriate error handling
                    return false;
                }
                else if (StreamState == State.NOT_RUNNING && (newState == State.PENDING_SHUTDOWN || newState == State.NOT_RUNNING))
                {
                    // when the state is already in NOT_RUNNING, its transition to PENDING_SHUTDOWN or NOT_RUNNING (due to consecutive close calls)
                    // will be refused but we do not throw exception here, to allow idempotent close calls
                    return false;
                }
                else if (StreamState == State.REBALANCING && newState == State.REBALANCING)
                {
                    // when the state is already in REBALANCING, it should not transit to REBALANCING again
                    return false;
                }
                else if (StreamState == State.ERROR && newState == State.ERROR)
                {
                    // when the state is already in ERROR, it should not transit to ERROR again
                    return false;
                }
                else if (!StreamState.IsValidTransition(newState))
                {
                    throw new IllegalStateException($"Stream-client {clientId}: Unexpected state transition from {oldState} to {newState}");
                }
                else
                {
                    //log.info("State transition from {} to {}", oldState, newState);
                }
                StreamState = newState;
            }

            StateChanged?.Invoke(oldState, newState);

            return true;
        }
        
        #endregion
    }
}
