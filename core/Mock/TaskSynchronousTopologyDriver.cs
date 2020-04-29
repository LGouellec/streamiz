using Streamiz.Kafka.Net.Kafka;
using Streamiz.Kafka.Net.Mock.Kafka;
using Streamiz.Kafka.Net.Mock.Pipes;
using Streamiz.Kafka.Net.Mock.Sync;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream.Internal;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace Streamiz.Kafka.Net.Mock
{
    internal class TaskSynchronousTopologyDriver : IBehaviorTopologyTestDriver
    {
        private readonly IStreamConfig configuration;
        private readonly IStreamConfig topicConfiguration;
        private readonly StreamTask taskTopology = null;
        private readonly InternalTopologyBuilder builder;
        private readonly CancellationToken token;
        private readonly IKafkaSupplier supplier;
        private readonly IDictionary<string, StreamTask> tasks = new Dictionary<string, StreamTask>();
        private int id = 0;

        public TaskSynchronousTopologyDriver(string clientId, InternalTopologyBuilder topologyBuilder, IStreamConfig configuration, IStreamConfig topicConfiguration, CancellationToken token)
        {
            // SAME CONSUME ORDER LIKE PRODUCE ORDER
            MockCluster.DEFAULT_NUMBER_PARTITIONS = 1;
            this.configuration = configuration;
            this.configuration.ClientId = clientId;
            this.topicConfiguration = topicConfiguration;

            this.token = token;
            builder = topologyBuilder;
            supplier = new SyncKafkaSupplier();
        }

        private StreamTask GetTask(string topicName)
        {
            StreamTask task;
            if (tasks.ContainsKey(topicName))
                task = tasks[topicName];
            else
            {
                var consumer = supplier.GetConsumer(this.configuration.ToConsumerConfig(), null);
                consumer.Subscribe(topicName);
                task = new StreamTask("thread-0",
                    new TaskId { Id = id++, Partition = 0, Topic = topicName },
                    new Confluent.Kafka.TopicPartition(topicName, 0),
                    builder.BuildTopology(topicName),
                    consumer,
                    this.configuration,
                    supplier,
                    supplier.GetProducer(this.configuration.ToProducerConfig()));
                task.InitializeStateStores();
                task.InitializeTopology();
                tasks.Add(topicName, task);
            }
            return task;
        }

        #region IBehaviorTopologyTestDriver

        public TestInputTopic<K, V> CreateInputTopic<K, V>(string topicName, ISerDes<K> keySerdes, ISerDes<V> valueSerdes)
        {
            var builder = new SyncPipeBuilder(GetTask(topicName), supplier);
            var pipeInput = builder.Input(topicName, configuration);
            return new TestInputTopic<K, V>(pipeInput, configuration, keySerdes, valueSerdes);
        }

        public TestOutputTopic<K, V> CreateOutputTopic<K, V>(string topicName, TimeSpan consumeTimeout, ISerDes<K> keySerdes = null, ISerDes<V> valueSerdes = null)
        {
            var builder = new SyncPipeBuilder(null, supplier);
            var pipeOutput = builder.Output(topicName, consumeTimeout, configuration, this.token);
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

        #endregion
    }
}
