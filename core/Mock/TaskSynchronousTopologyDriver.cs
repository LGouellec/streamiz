using Confluent.Kafka;
using Streamiz.Kafka.Net.Kafka;
using Streamiz.Kafka.Net.Mock.Pipes;
using Streamiz.Kafka.Net.Mock.Sync;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.SerDes;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Kafka.Internal;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Stream.Internal;

namespace Streamiz.Kafka.Net.Mock
{
    internal sealed class TaskSynchronousTopologyDriver : IBehaviorTopologyTestDriver
    {
        private readonly IStreamConfig configuration;
        private readonly IStreamConfig topicConfiguration;
        private readonly InternalTopologyBuilder builder;
        private readonly CancellationToken token;
        private readonly IKafkaSupplier supplier;
        private readonly IDictionary<TaskId, StreamTask> tasks = new Dictionary<TaskId, StreamTask>();
        private readonly GlobalStateUpdateTask globalTask;
        private readonly IDictionary<string, ExternalProcessorTopologyExecutor> externalProcessorTopologies =
            new Dictionary<string, ExternalProcessorTopologyExecutor>();
        private readonly GlobalProcessorContext globalProcessorContext;
        private readonly StreamMetricsRegistry metricsRegistry;

        private readonly IDictionary<TaskId, IList<TopicPartition>> partitionsByTaskId =
            new Dictionary<TaskId, IList<TopicPartition>>();

        private readonly StreamsProducer producer = null;
        private readonly bool hasGlobalTopology = false;
        private ITopicManager internalTopicManager;
        private readonly IConsumer<byte[],byte[]> repartitionConsumerForwarder;
        private readonly MockWallClockTimeProvider wallClockTimeProvider;

        public bool IsRunning { get; private set; }

        public bool IsStopped => !IsRunning;

        public bool IsError { get; private set; } = false;

        public TaskSynchronousTopologyDriver(string clientId, InternalTopologyBuilder topologyBuilder,
            IStreamConfig configuration, IStreamConfig topicConfiguration, CancellationToken token)
            : this(clientId, topologyBuilder, configuration, topicConfiguration, null, token)
        {
        }

        public TaskSynchronousTopologyDriver(string clientId, InternalTopologyBuilder topologyBuilder,
            IStreamConfig configuration, IStreamConfig topicConfiguration, IKafkaSupplier supplier,
            CancellationToken token)
        {
            this.configuration = configuration;
            this.configuration.ClientId = clientId;
            this.topicConfiguration = topicConfiguration;
            metricsRegistry = new StreamMetricsRegistry(clientId, MetricsRecordingLevel.DEBUG);
            
            this.token = token;
            builder = topologyBuilder;
            this.supplier = supplier ?? new SyncKafkaSupplier();
            this.supplier.MetricsRegistry = metricsRegistry;

            producer = new StreamsProducer(
                configuration,
                clientId, Guid.NewGuid(), this.supplier, string.Empty);

            foreach (var sourceTopic in builder
                .GetSourceTopics())
            {
                var part = new TopicPartition(sourceTopic, 0);
                var taskId = builder.GetTaskIdFromPartition(part);
                if (partitionsByTaskId.ContainsKey(taskId))
                    partitionsByTaskId[taskId].Add(part);
                else
                    partitionsByTaskId.Add(taskId, new List<TopicPartition> {part});
            }
            
            var adminClient = this.supplier.GetAdmin(configuration.ToAdminConfig($"{clientId}-admin"));
            ProcessorTopology globalTaskTopology = topologyBuilder.BuildGlobalStateTopology(configuration);
            hasGlobalTopology = globalTaskTopology != null;
            if (hasGlobalTopology)
            {
                var globalConsumer =
                    this.supplier.GetGlobalConsumer(configuration.ToGlobalConsumerConfig($"{clientId}-global-consumer"));
                var stateManager =
                    new GlobalStateManager(globalConsumer, globalTaskTopology, adminClient, new StatestoreRestoreManager(null), configuration);
                globalProcessorContext = new GlobalProcessorContext(configuration, stateManager, metricsRegistry);
                stateManager.SetGlobalProcessorContext(globalProcessorContext);
                globalTask = new GlobalStateUpdateTask(stateManager, globalTaskTopology, globalProcessorContext);
                
                globalTask.Initialize();
            }

            foreach (var requestTopic in topologyBuilder.GetRequestTopics())
            {
                var taskId = topologyBuilder.GetTaskIdFromPartition(new TopicPartition(requestTopic, Partition.Any));
                externalProcessorTopologies.Add(
                    requestTopic, 
                    new ExternalProcessorTopologyExecutor(
                        "ext-thread-0",
                        taskId,
                        topologyBuilder.BuildTopology(taskId, configuration).GetSourceProcessor(requestTopic),
                        producer,
                        configuration,
                        metricsRegistry,
                        adminClient));
            }
            
            repartitionConsumerForwarder = this.supplier.GetConsumer(topicConfiguration.ToConsumerConfig("consumer-repartition-forwarder"),
                null);

            // Initialize mock wall clock time provider for controlling time in tests
            wallClockTimeProvider = new MockWallClockTimeProvider();
        }

        internal StreamTask GetTask(string topicName)
        {
            StreamTask task = null;
            var id = builder.GetTaskIdFromPartition(new Confluent.Kafka.TopicPartition(topicName, 0));
            if (tasks.ContainsKey(id))
                task = tasks[id];
            else
            {
                if (builder.GetSourceTopics().Contains(topicName))
                {
                    // Make sure partitionsByTaskId has this task's partitions
                    if (!partitionsByTaskId.ContainsKey(id))
                    {
                        var part = new TopicPartition(topicName, 0);
                        partitionsByTaskId.Add(id, new List<TopicPartition> {part});
                    }

                    task = new StreamTask("thread-0",
                        id,
                        partitionsByTaskId[id],
                        builder.BuildTopology(id, configuration),
                        supplier.GetConsumer(configuration.ToConsumerConfig(), null),
                        configuration,
                        supplier,
                        producer,
                        new MockChangelogRegister(),
                        metricsRegistry,
                        wallClockTimeProvider);
                    task.InitializeStateStores();
                    task.InitializeTopology();
                    task.RestorationIfNeeded();
                    task.CompleteRestoration();
                    tasks.Add(id, task);
                }
            }

            return task;
        }

        private void InitializeInternalTopicManager()
        {
            // Create internal topics (changelogs & repartition) if need
            var adminClientInternalTopicManager = supplier.GetAdmin(configuration.ToAdminConfig(
                StreamThread.GetSharedAdminClientId(
                    $"{configuration.ApplicationId.ToLower()}-admin-internal-topic-manager")));
            internalTopicManager = new DefaultTopicManager(configuration, adminClientInternalTopicManager);

            InternalTopicManagerUtils
                .New(configuration)
                .CreateSourceTopics(builder, supplier)
                .CreateInternalTopicsAsync(internalTopicManager, builder, configuration.AllowAutoCreateTopics ?? false)
                .GetAwaiter()
                .GetResult();
        }

        private void ForwardRepartitionTopic(string topic)
        {
            ForwardTopicRecordsIfAny(topic);
        }

        /// <summary>
        /// Forwards any pending records from an internal topic to downstream tasks.
        /// Returns true if any records were forwarded, false otherwise.
        /// </summary>
        private bool ForwardTopicRecordsIfAny(string topic)
        {
            var records = new List<ConsumeResult<byte[], byte[]>>();

            repartitionConsumerForwarder.Subscribe(topic);

            ConsumeResult<byte[], byte[]> record = null;
            do
            {
                record = repartitionConsumerForwarder.Consume();
                if (record != null)
                {
                    records.Add(record);
                }
            } while (record != null);

            if (records.Any())
            {
                var task = GetTask(topic);

                var pipe = CreateBuilder(topic).Input(topic, configuration);
                foreach(var r in records)
                    pipe.Pipe(r.Message.Key, r.Message.Value, r.Message.Timestamp.UnixTimestampMs.FromMilliseconds(), r.Message.Headers);
                pipe.Flush();
                pipe.Dispose();

                repartitionConsumerForwarder.Commit(records.Last());
                repartitionConsumerForwarder.Unsubscribe();
                return true;
            }

            repartitionConsumerForwarder.Unsubscribe();
            return false;
        }

        /// <summary>
        /// Gets all topics that are used for internal communication within the topology.
        /// These are topics that are both written to (sink) and read from (source) within the topology.
        /// </summary>
        private HashSet<string> GetAllInternalCommunicationTopics()
        {
            var topicGroups = builder.MakeInternalTopicGroups();
            var allSinks = topicGroups.Values.SelectMany(t => t.SinkTopics).ToHashSet();
            var allSources = topicGroups.Values.SelectMany(t => t.SourceTopics).ToHashSet();

            // Internal topics are those that are both written and read within the topology
            allSinks.IntersectWith(allSources);
            return allSinks;
        }

        private SyncPipeBuilder CreateBuilder(string topicName)
        {
            var task = GetTask(topicName);

            if (task == null)
            {
                if(hasGlobalTopology && builder.GetGlobalTopics().Contains(topicName))
                    return new SyncPipeBuilder(globalTask);
                if (builder.GetRequestTopics().Contains(topicName))
                    return new SyncPipeBuilder(externalProcessorTopologies[topicName]);
            }

            return new SyncPipeBuilder(task, wallClockTimeProvider);
        }
        
        #region IBehaviorTopologyTestDriver

        public TestMultiInputTopic<K, V> CreateMultiInputTopic<K, V>(string[] topics, ISerDes<K> keySerdes = null,
            ISerDes<V> valueSerdes = null)
        {
            Dictionary<string, IPipeInput> pipes = new Dictionary<string, IPipeInput>();

            foreach (var topic in topics)
            {
                var builder = CreateBuilder(topic);
                var pipeInput = builder.Input(topic, configuration);

                var topicsLink = new List<string>();
                this.builder.GetLinkTopics(topic, topicsLink);
                var consumer =
                    supplier.GetConsumer(topicConfiguration.ToConsumerConfig("consumer-repartition-forwarder"),
                        null);

                foreach (var topicLink in topicsLink)
                    pipeInput.Flushed += () => ForwardRepartitionTopic(topicLink);

                pipes.Add(topic, pipeInput);
            }

            return new TestMultiInputTopic<K, V>(pipes, configuration, keySerdes, valueSerdes);
        }

        public TestInputTopic<K, V> CreateInputTopic<K, V>(string topicName, ISerDes<K> keySerdes,
            ISerDes<V> valueSerdes)
        {
            var pipeBuilder = CreateBuilder(topicName);
            var pipeInput = pipeBuilder.Input(topicName, configuration);

            var topicsLink = new List<string>();
            builder.GetLinkTopics(topicName, topicsLink);
            
            foreach (var topic in topicsLink)
            {
                GetTask(topic);
                pipeInput.Flushed += () => ForwardRepartitionTopic(topic);
            }

            return new TestInputTopic<K, V>(pipeInput, configuration, keySerdes, valueSerdes);
        }

        public TestOutputTopic<K, V> CreateOutputTopic<K, V>(string topicName, TimeSpan consumeTimeout,
            ISerDes<K> keySerdes = null, ISerDes<V> valueSerdes = null)
        {
            var pipeBuilder = new SyncPipeBuilder(producer.Producer as SyncProducer);
            var pipeOutput = pipeBuilder.Output(topicName, consumeTimeout, configuration, token);
            return new TestOutputTopic<K, V>(pipeOutput, topicConfiguration, keySerdes, valueSerdes);
        }

        public void Dispose()
        {
            internalTopicManager?.Dispose();

            foreach (var t in tasks.Values)
            {
                bool persistent = t.IsPersistent;
                t.Suspend();
                t.Close(false);

                // Remove local state store for this task
                if (persistent)
                    Directory.Delete(t.Context.StateDir, true);
            }

            if (hasGlobalTopology)
            {
                globalTask.FlushState();
                globalTask.Close();
            }

            foreach (var ext in externalProcessorTopologies)
            {
                ext.Value.Flush();
                ext.Value.Close();
            }

            IsRunning = false;
        }

        public void StartDriver()
        {
            InitializeInternalTopicManager();

            IsRunning = true;
        }

        public IStateStore GetStateStore<K, V>(string name)
        {
            var task = tasks.Values.FirstOrDefault(t => t.Context.GetStateStore(name) != null);
            
            if(task != null)
                return task.Context.GetStateStore(name);

            return hasGlobalTopology ? globalProcessorContext.GetStateStore(name) : null;
        }

        public void TriggerCommit()
        {
            foreach(var extProcessor in externalProcessorTopologies.Values)
                extProcessor.Flush();

            foreach (var task in tasks.Values)
            {
                // var consumer = supplier.GetConsumer(topicConfiguration.ToConsumerConfig("consumer-repartition-forwarder"),null);
                var offsets = task.PrepareCommit();
                task.PostCommit(false);
            }

            globalTask?.FlushState();
        }

        public void AdvanceWallClockTime(TimeSpan advance)
        {
            // Advance the mock wall clock once at the beginning
            wallClockTimeProvider.Advance(advance);

            // Get all topics used for internal communication within the topology
            var internalTopics = GetAllInternalCommunicationTopics();

            // Process until no more progress (like Java's completeAllProcessableWork)
            // We advance time incrementally inside the loop to allow downstream buffers to flush.
            // This is needed because buffers schedule punctuations relative to current time,
            // so buffers created during forwarding need additional time to flush.
            bool madeProgress;
            int iterations = 0;
            const int maxIterations = 100; // Prevent infinite loops
            do
            {
                madeProgress = false;
                iterations++;

                // 1. FIRST forward records from all internal topics to ensure buffers have data
                // Keep forwarding until no more records found in any topic
                bool forwardedAny = false;
                bool forwardedThisPass;
                do
                {
                    forwardedThisPass = false;
                    foreach (var topic in internalTopics)
                    {
                        if (ForwardTopicRecordsIfAny(topic))
                        {
                            madeProgress = true;
                            forwardedThisPass = true;
                            forwardedAny = true;
                        }
                    }
                } while (forwardedThisPass);

                // 2. THEN trigger PROCESSING_TIME punctuations for all tasks (including newly created ones)
                bool punctuatedAny = false;
                foreach (var task in tasks.Values)
                {
                    if (task.PunctuateSystemTime())
                    {
                        madeProgress = true;
                        punctuatedAny = true;
                    }
                }

                // 3. If we forwarded records but no punctuations fired, we need to advance
                // time significantly so downstream buffers can flush. Use the full advance
                // amount since buffer punctuations may be scheduled far apart.
                // If punctuations fired or no records were forwarded, use a smaller increment.
                if (madeProgress)
                {
                    if (forwardedAny && !punctuatedAny)
                    {
                        // Records went to downstream buffers but their punctuations didn't fire yet
                        // Advance by full amount to ensure downstream punctuations can fire
                        wallClockTimeProvider.Advance(advance);
                    }
                    else
                    {
                        // Normal progress - advance by small amount
                        wallClockTimeProvider.Advance(TimeSpan.FromMilliseconds(advance.TotalMilliseconds / 10));
                    }
                }

            } while (madeProgress && iterations < maxIterations);
        }

        #endregion
    }
}