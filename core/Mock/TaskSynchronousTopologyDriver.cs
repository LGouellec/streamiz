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
        private readonly IDictionary<TaskId, IList<TopicPartition>> partitionsByTaskId = new Dictionary<TaskId, IList<TopicPartition>>();
        private readonly SyncProducer producer = null;
        private ITopicManager internalTopicManager;

        public bool IsRunning { get; private set; }

        public bool IsStopped => !IsRunning;

        public bool IsError { get; private set; } = false;

        public TaskSynchronousTopologyDriver(string clientId, InternalTopologyBuilder topologyBuilder, IStreamConfig configuration, IStreamConfig topicConfiguration, CancellationToken token)
            : this(clientId, topologyBuilder, configuration, topicConfiguration, null, token)
        {
        }

        public TaskSynchronousTopologyDriver(string clientId, InternalTopologyBuilder topologyBuilder, IStreamConfig configuration, IStreamConfig topicConfiguration, IKafkaSupplier supplier, CancellationToken token)
        {
            this.configuration = configuration;
            this.configuration.ClientId = clientId;
            this.topicConfiguration = topicConfiguration;

            this.token = token;
            builder = topologyBuilder;
            this.supplier = supplier ?? new SyncKafkaSupplier();
            producer = this.supplier.GetProducer(configuration.ToProducerConfig()) as SyncProducer;

            foreach (var sourceTopic in builder
                .GetSourceTopics()
                .Union(builder.GetGlobalTopics()))
            {
                var part = new TopicPartition(sourceTopic, 0);
                var taskId = builder.GetTaskIdFromPartition(part);
                if (partitionsByTaskId.ContainsKey(taskId))
                    partitionsByTaskId[taskId].Add(part);
                else
                    partitionsByTaskId.Add(taskId, new List<TopicPartition> { part });
            }
        }

        internal StreamTask GetTask(string topicName)
        {
            StreamTask task;
            var id = builder.GetTaskIdFromPartition(new Confluent.Kafka.TopicPartition(topicName, 0));
            if (tasks.ContainsKey(id))
                task = tasks[id];
            else
            {
                task = new StreamTask("thread-0",
                    id,
                    partitionsByTaskId[id],
                    builder.BuildTopology(id),
                    supplier.GetConsumer(configuration.ToConsumerConfig(), null),
                    configuration,
                    supplier,
                    producer,
                    new MockChangelogRegister());
                task.InitializeStateStores();
                task.InitializeTopology();
                task.RestorationIfNeeded();
                task.CompleteRestoration();
                tasks.Add(id, task);
            }
            return task;
        }

        private void InitializeInternalTopicManager()
        {
            // Create internal topics (changelogs & repartition) if need
            var adminClientInternalTopicManager = supplier.GetAdmin(configuration.ToAdminConfig(StreamThread.GetSharedAdminClientId($"{configuration.ApplicationId.ToLower()}-admin-internal-topic-manager")));
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
                if(record != null)
                    records.Add(record);
            } while (record != null);
                    

            if (records.Any())
            {
                long now = DateTime.Now.GetMilliseconds();
                var task = GetTask(topic);
                task.AddRecords(records);
                while (task.CanProcess(now))
                    task.Process();

                consumer.Commit(records.Last());
            }
            
            consumer.Unsubscribe();
        }
        
        #region IBehaviorTopologyTestDriver

        public TestMultiInputTopic<K, V> CreateMultiInputTopic<K, V>(string[] topics, ISerDes<K> keySerdes = null, ISerDes<V> valueSerdes = null)
        {
            Dictionary<string, IPipeInput> pipes = new Dictionary<string, IPipeInput>();
            
            foreach(var topic in topics)
            {
                var builder = new SyncPipeBuilder(GetTask(topic));
                var pipeInput = builder.Input(topic, configuration);
                
                if (this.builder.IsRepartitionTopology())
                {
                    var topicsLink = new List<string>();
                    this.builder.GetRepartitionLinkTopics(topic, topicsLink);
                    var consumer = supplier.GetConsumer(topicConfiguration.ToConsumerConfig("consumer-repartition-forwarder"), null);
                
                    foreach (var topicLink in topicsLink)
                        pipeInput.Flushed += () => ForwardRepartitionTopic(consumer, topicLink);
                }
                
                pipes.Add(topic, pipeInput);
            }

            return new TestMultiInputTopic<K, V>(pipes, configuration, keySerdes, valueSerdes);
        }

        public TestInputTopic<K, V> CreateInputTopic<K, V>(string topicName, ISerDes<K> keySerdes, ISerDes<V> valueSerdes)
        {
            var pipeBuilder = new SyncPipeBuilder(GetTask(topicName));
            var pipeInput = pipeBuilder.Input(topicName, configuration);
            
            if (builder.IsRepartitionTopology())
            {
                var topicsLink = new List<string>();
                builder.GetRepartitionLinkTopics(topicName, topicsLink);
                var consumer = supplier.GetConsumer(topicConfiguration.ToConsumerConfig("consumer-repartition-forwarder"), null);
                
                foreach (var topic in topicsLink)
                    pipeInput.Flushed += () => ForwardRepartitionTopic(consumer, topic);
            }
            
            return new TestInputTopic<K, V>(pipeInput, configuration, keySerdes, valueSerdes);
        }

        public TestOutputTopic<K, V> CreateOutputTopic<K, V>(string topicName, TimeSpan consumeTimeout, ISerDes<K> keySerdes = null, ISerDes<V> valueSerdes = null)
        {
            var pipeBuilder = new SyncPipeBuilder(null, producer);
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
            return task != null ? task.Context.GetStateStore(name) : null;
        }

        #endregion
    }
}
