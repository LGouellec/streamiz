using Confluent.Kafka;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Kafka;
using Streamiz.Kafka.Net.Kafka.Internal;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Stream.Internal;
using log4net;
using System;
using System.Collections.Generic;
using System.Threading;
using Streamiz.Kafka.Net.State.Internal;

namespace Streamiz.Kafka.Net
{
    /// <summary>
    /// Listen to <see cref="KafkaStream"/> change events.
    /// </summary>
    /// <param name="oldState">previous statenew</param>
    /// <param name="newState">new state</param>
    public delegate void StateListener(KafkaStream.State oldState, KafkaStream.State newState);

    /// <summary>
    /// A Kafka client that allows for performing continuous computation on input coming from one or more input topics and
    /// sends output to zero, one, or more output topics.
    /// 
    /// The computational logic can be specified using the <see cref="StreamBuilder"/> which provides the high-level DSL to define
    /// transformations.
    ///
    /// One <see cref="KafkaStream"/> instance can contain one or more threads specified in the configs for the processing work.
    /// 
    /// A <see cref="KafkaStream"/> instance can co-ordinate with any other instances with the same
    /// <see cref="IStreamConfig.ApplicationId"/> (whether in the same process, on other processes on this
    /// machine, or on remote machines) as a single (possibly distributed) stream processing application.
    /// These instances will divide up the work based on the assignment of the input topic partitions so that all partitions
    /// are being consumed.
    /// If instances are added or fail, all (remaining) instances will rebalance the partition assignment among themselves
    /// to balance processing load and ensure that all input topic partitions are processed.
    /// Internally a <see cref="KafkaStream"/> instance contains a normal producer and consumer instance
    /// that is used for reading input and writing output.
    /// <example>
    /// A simple example might look like this:
    /// <code>
    /// CancellationTokenSource source = new CancellationTokenSource();
    /// var config = new StreamConfig&lt;StringSerDes, StringSerDes&gt;();
    /// config.ApplicationId = "test-kstream-app";
    /// config.BootstrapServers = "192.168.56.1:9092";
    /// 
    /// StreamBuilder builder = new StreamBuilder();
    /// builder.Stream&lt;string, string&gt;("test").FilterNot((k, v) => v.Contains("test")).To("test-output");
    /// 
    /// KafkaStream stream = new KafkaStream(builder.Build(), config);
    /// 
    /// Console.CancelKeyPress += (o, e) => {
    ///     source.Cancel();
    ///     stream.Close();
    /// };
    ///      
    /// stream.Start(source.Token);
    /// </code>
    /// </example>
    /// </summary>
    public class KafkaStream
    {
        #region State Stream

        /// <summary>
        /// Kafka Streams states are the possible state that a Kafka Streams instance can be in.
        /// An instance must only be in one state at a time.
        /// The expected state transition with the following defined states is:
        /// 
        ///                 +--------------+
        ///         +&lt;----- | Created (0)  |
        ///         |       +-----+--------+
        ///         |             |
        ///         |             v
        ///         |       +----+--+------+
        ///         |       | Re-          |
        ///         +&lt;----- | Balancing (1)| --------&gt;+
        ///         |       +-----+-+------+          |
        ///         |             | ^                 |
        ///         |             v |                 |
        ///         |       +--------------+          v
        ///         |       | Running (2)  | --------&gt;+
        ///         |       +------+-------+          |
        ///         |              |                  |
        ///         |              v                  |
        ///         |       +------+-------+     +----+-------+
        ///         +-----&gt; | Pending      |&lt;--- | Error (5)  |
        ///                 | Shutdown (3) |     +------------+
        ///                 +------+-------+
        ///                        |
        ///                        v
        ///                 +------+-------+
        ///                 | Not          |
        ///                 | Running (4)  |
        ///                 +--------------+
        /// 
        /// 
        /// Note the following:
        /// - RUNNING state will transit to REBALANCING if any of its threads is in PARTITION_REVOKED or PARTITIONS_ASSIGNED state
        /// - REBALANCING state will transit to RUNNING if all of its threads are in RUNNING state
        /// - Any state except NOT_RUNNING can go to PENDING_SHUTDOWN (whenever close is called)
        /// - Of special importance: If the global stream thread dies, or all stream threads die (or both) then
        ///   the instance will be in the ERROR state. The user will need to close it.
        /// </summary>
        public class State
        {
            /// <summary>
            /// Static CREATED State of a <see cref="KafkaStream"/> instance.
            /// </summary>
            public static State CREATED = new State(1, 3).Order(0).Name("CREATED");
            /// <summary>
            /// Static REBALANCING State of a <see cref="KafkaStream"/> instance.
            /// </summary>
            public static State REBALANCING = new State(2, 3, 5).Order(1).Name("REBALANCING");
            /// <summary>
            /// Static RUNNING State of a <see cref="KafkaStream"/> instance.
            /// </summary>
            public static State RUNNING = new State(1, 2, 3, 5).Order(2).Name("RUNNING");
            /// <summary>
            /// Static PENDING_SHUTDOWN State of a <see cref="KafkaStream"/> instance.
            /// </summary>
            public static State PENDING_SHUTDOWN = new State(4).Order(3).Name("PENDING_SHUTDOWN");
            /// <summary>
            /// Static NOT_RUNNING State of a <see cref="KafkaStream"/> instance.
            /// </summary>
            public static State NOT_RUNNING = new State().Order(4).Name("NOT_RUNNING");
            /// <summary>
            /// Static ERROR State of a <see cref="KafkaStream"/> instance.
            /// </summary>
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

            /// <summary>
            /// Return true if the state is a running state, false otherwise
            /// </summary>
            /// <returns>Return true if the state is a running state, false otherwise</returns>
            public bool IsRunning()

            {
                return this.Equals(RUNNING) || this.Equals(REBALANCING);
            }

            internal bool IsValidTransition(State newState)
            {
                return validTransitions.Contains(((State)newState).ordinal);
            }

            /// <summary>
            /// == operator between two <see cref="State"/>
            /// </summary>
            /// <param name="a"></param>
            /// <param name="b"></param>
            /// <returns></returns>
            public static bool operator ==(State a, State b) => a?.ordinal == b?.ordinal;

            /// <summary>
            /// != operator between two <see cref="State"/>
            /// </summary>
            /// <param name="a"></param>
            /// <param name="b"></param>
            /// <returns></returns>
            public static bool operator !=(State a, State b) => a?.ordinal != b?.ordinal;

            /// <summary>
            /// Override Equals method
            /// </summary>
            /// <param name="obj"></param>
            /// <returns></returns>
            public override bool Equals(object obj)
            {
                return obj is State && ((State)obj).ordinal.Equals(this.ordinal);
            }

            /// <summary>
            /// Override GetHashCode method
            /// </summary>
            /// <returns></returns>
            public override int GetHashCode()
            {
                return this.ordinal.GetHashCode();
            }

            /// <summary>
            /// Override ToString method
            /// </summary>
            /// <returns></returns>
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
        private readonly ILog logger = Logger.GetLogger(typeof(KafkaStream));
        private readonly string logPrefix = "";
        private readonly object stateLock = new object();
        private readonly QueryableStoreProvider queryableStoreProvider;

        internal State StreamState { get; private set; }

        /// <summary>
        /// An app can subscribe to <see cref="StateListener"/> and he is notified when state changes.
        /// </summary>
        public event StateListener StateChanged;

        /// <summary>
        /// Create a <see cref="KafkaStream"/> instance.
        /// Please DO NOT FORGET to call Close to avoid resources leak !
        /// </summary>
        /// <param name="topology">the topology specifying the computational logic</param>
        /// <param name="configuration">configuration about this stream</param>
        public KafkaStream(Topology topology, IStreamConfig configuration)
        {
            this.topology = topology;
            this.configuration = configuration;
            this.kafkaSupplier = new DefaultKafkaClientSupplier(new KafkaLoggerAdapter(configuration));

            var processID = Guid.NewGuid();
            clientId = string.IsNullOrEmpty(configuration.ClientId) ? $"{this.configuration.ApplicationId.ToLower()}-{processID}" : configuration.ClientId;
            logPrefix = $"stream-application[{configuration.ApplicationId}] ";

            // re-write the physical topology according to the config
            topology.Builder.RewriteTopology(configuration);

            // sanity check
            this.processorTopology = topology.Builder.BuildTopology();

            this.threads = new IThread[this.configuration.NumStreamThreads];
            var threadState = new Dictionary<long, Processors.ThreadState>();

            List<StreamThreadStateStoreProvider> stateStoreProviders = new List<StreamThreadStateStoreProvider>();
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

                stateStoreProviders.Add(new StreamThreadStateStoreProvider(this.threads[i], this.topology.Builder));
            }
            
            var manager = new StreamStateManager(this, threadState);
            foreach (var t in threads)
                t.StateChanged += manager.OnChange;

            this.queryableStoreProvider = new QueryableStoreProvider(stateStoreProviders);

            StreamState = State.CREATED;
        }

        /// <summary>
        /// Start the <see cref="KafkaStream"/> instance by starting all its threads.
        /// This function is expected to be called only once during the life cycle of the client.
        /// Because threads are started in the background, this method does not block.
        /// </summary>
        /// <param name="token">Token for propagates notification that the stream should be canceled.</param>
        public void Start(CancellationToken token = default)
        {
            if (SetState(State.REBALANCING))
            {
                logger.Debug($"{logPrefix}Starting Streams client");

                foreach (var t in threads)
                    t.Start(token);
            }
        }

        /// <summary>
        /// Shutdown this <see cref="KafkaStream"/> instance by signaling all the threads to stop, and then wait for them to join.
        /// This will block until all threads have stopped.
        /// </summary>
        public void Close()
        {
            if (!SetState(State.PENDING_SHUTDOWN))
            {
                // if transition failed, it means it was either in PENDING_SHUTDOWN
                // or NOT_RUNNING already; just check that all threads have been stopped
                logger.Info($"{logPrefix}Already in the pending shutdown state, wait to complete shutdown");
            }
            else
            {
                foreach (var t in threads)
                    t.Dispose();

                SetState(State.NOT_RUNNING);
                logger.Info($"{logPrefix}Streams client stopped completely");
            }
        }

        /// <summary>
        /// Get a facade wrapping the local <see cref="IStateStore"/> instances with the provided <see cref="StoreQueryParameters{T}"/>
        /// The returned object can be used to query the {@link StateStore} instances.
        /// </summary>
        /// <typeparam name="T">return type</typeparam>
        /// <param name="storeQueryParameters">the parameters used to fetch a queryable store</param>
        /// <returns>A facade wrapping the local <see cref="IStateStore"/> instances</returns>
        /// <exception cref="InvalidStateStoreException ">if Kafka Streams is (re-)initializing or a store with <code>storeName</code> } and
        /// <code>queryableStoreType</code> doesn't exist </exception>
        public T Store<T>(StoreQueryParameters<T> storeQueryParameters) where T : class
        {
            this.ValidateIsRunning();
            return this.queryableStoreProvider.GetStore(storeQueryParameters);
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
                        logger.Debug($"{logPrefix}Cannot transit to {targetState} within {waitMs}ms");
                        return false;
                    }
                    elapsedMs = DateTime.Now.Millisecond - begin;
                }
                return true;
            }
        }

        /// <summary>
        /// Set internal state. This method is thread safe.
        /// </summary>
        /// <param name="newState">New state</param>
        /// <returns>Return true if <paramref name="newState"/> was setted, false otherwise</returns>
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
                    logger.Info($"{logPrefix}State transition from {oldState} to {newState}");
                }
                StreamState = newState;
            }

            StateChanged?.Invoke(oldState, newState);

            return true;
        }

        private void ValidateIsRunning()
        {
            bool isRunning;
            lock (stateLock)
            {
                isRunning = StreamState.IsRunning();
            }
            if (!isRunning)
            {
                throw new IllegalStateException($"KafkaStreams is not running. State is {StreamState}.");
            }
        }
        
        #endregion
    }
}
