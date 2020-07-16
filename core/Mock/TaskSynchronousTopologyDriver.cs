using Confluent.Kafka;
using Streamiz.Kafka.Net.Kafka;
using Streamiz.Kafka.Net.Mock.Sync;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.SerDes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

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
        private int id = 0;

        public TaskSynchronousTopologyDriver(string clientId, InternalTopologyBuilder topologyBuilder, IStreamConfig configuration, IStreamConfig topicConfiguration, CancellationToken token)
        {
            this.configuration = configuration;
            this.configuration.ClientId = clientId;
            this.topicConfiguration = topicConfiguration;

            this.token = token;
            builder = topologyBuilder;
            supplier = new SyncKafkaSupplier();
            producer = supplier.GetProducer(configuration.ToProducerConfig()) as SyncProducer;

            foreach(var sourceTopic in builder.GetSourceTopics())
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
                    producer);
                task.InitializeStateStores();
                task.InitializeTopology();
                tasks.Add(id, task);
            }
            return task;
        }

        #region IBehaviorTopologyTestDriver

        public TestInputTopic<K, V> CreateInputTopic<K, V>(string topicName, ISerDes<K> keySerdes, ISerDes<V> valueSerdes)
        {
            var pipeBuilder = new SyncPipeBuilder(GetTask(topicName));
            var pipeInput = pipeBuilder.Input(topicName, configuration);
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
            foreach (var t in tasks.Values)
                t.Close();
        }

        public void StartDriver()
        {
            // NOTHING
        }

        public IStateStore GetStateStore<K, V>(string name)
        {
            var task = tasks.Values.FirstOrDefault(t => t.Context.GetStateStore(name) != null);
            return task != null ? task.Context.GetStateStore(name) : null;
        }

        #endregion
    }
}
