using Confluent.Kafka;
using Streamiz.Kafka.Net.Kafka;
using Streamiz.Kafka.Net.Mock.Pipes;
using Streamiz.Kafka.Net.Mock.Sync;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.SerDes;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data.SqlTypes;
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

        private readonly SyncProducer producer = null;
        private readonly bool hasGlobalTopology = false;
        private ITopicManager internalTopicManager;

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
            producer = this.supplier.GetProducer(configuration.ToProducerConfig()) as SyncProducer;

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
            
            ProcessorTopology globalTaskTopology = topologyBuilder.BuildGlobalStateTopology();
            hasGlobalTopology = globalTaskTopology != null;
            if (hasGlobalTopology)
            {
                var globalConsumer =
                    this.supplier.GetGlobalConsumer(configuration.ToGlobalConsumerConfig($"{clientId}-global-consumer"));
                var adminClient = this.supplier.GetAdmin(configuration.ToAdminConfig($"{clientId}-admin"));
                var stateManager =
                    new GlobalStateManager(globalConsumer, globalTaskTopology, adminClient, configuration);
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
                        topologyBuilder.BuildTopology(taskId).GetSourceProcessor(requestTopic),
                        this.supplier.GetProducer(configuration.ToProducerConfig($"ext-thread-producer-{requestTopic}")),
                        configuration,
                        metricsRegistry));
            }
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
                    task = new StreamTask("thread-0",
                        id,
                        partitionsByTaskId[id],
                        builder.BuildTopology(id),
                        supplier.GetConsumer(configuration.ToConsumerConfig(), null),
                        configuration,
                        supplier,
                        producer,
                        new MockChangelogRegister(),
                        metricsRegistry);
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
                .New()
                .CreateSourceTopics(builder, supplier)
                .CreateInternalTopicsAsync(internalTopicManager, builder)
                .GetAwaiter()
                .GetResult();
        }

        private void ForwardRepartitionTopic(IConsumer<byte[], byte[]> consumer, string topic)
        {
            var records = new List<ConsumeResult<byte[], byte[]>>();
            consumer.Subscribe(topic);
            ConsumeResult<byte[], byte[]> record = null;
            do
            {
                record = consumer.Consume();
                if (record != null)
                    records.Add(record);
            } while (record != null);


            if (records.Any())
            {
                var pipe = CreateBuilder(topic).Input(topic, configuration);
                foreach(var r in records)
                    pipe.Pipe(r.Message.Key, r.Message.Value, r.Message.Timestamp.UnixTimestampMs.FromMilliseconds(), r.Message.Headers);
                pipe.Flush();
                pipe.Dispose();

                consumer.Commit(records.Last());
            }

            consumer.Unsubscribe();
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

            return new SyncPipeBuilder(task);
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
                    pipeInput.Flushed += () => ForwardRepartitionTopic(consumer, topicLink);

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
            var consumer = supplier.GetConsumer(topicConfiguration.ToConsumerConfig("consumer-repartition-forwarder"),
                null);

            foreach (var topic in topicsLink)
                pipeInput.Flushed += () => ForwardRepartitionTopic(consumer, topic);

            return new TestInputTopic<K, V>(pipeInput, configuration, keySerdes, valueSerdes);
        }

        public TestOutputTopic<K, V> CreateOutputTopic<K, V>(string topicName, TimeSpan consumeTimeout,
            ISerDes<K> keySerdes = null, ISerDes<V> valueSerdes = null)
        {
            var pipeBuilder = new SyncPipeBuilder(producer);
            var pipeOutput = pipeBuilder.Output(topicName, consumeTimeout, configuration, token);
            return new TestOutputTopic<K, V>(pipeOutput, topicConfiguration, keySerdes, valueSerdes);
        }

        public void Dispose()
        {
            internalTopicManager?.Dispose();

            foreach (var t in tasks.Values)
            {
                t.Close();

                // Remove local state store for this task
                if (t.IsPersistent)
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

        #endregion
    }
}