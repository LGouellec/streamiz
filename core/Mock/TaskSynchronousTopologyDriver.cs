using Streamiz.Kafka.Net.Errors;
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
    internal class TaskSynchronousTopologyDriver : IBehaviorTopologyTestDriver
    {
        private readonly IStreamConfig configuration;
        private readonly IStreamConfig topicConfiguration;
        private readonly InternalTopologyBuilder builder;
        private readonly CancellationToken token;
        private readonly IKafkaSupplier supplier;
        private readonly IDictionary<string, StreamTask> tasks = new Dictionary<string, StreamTask>();
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
        }

        private StreamTask GetTask(string topicName)
        {
            StreamTask task;
            if (tasks.ContainsKey(topicName))
                task = tasks[topicName];
            else
            {
                task = new StreamTask("thread-0",
                    new TaskId { Id = id++, Partition = 0, Topic = topicName },
                    new Confluent.Kafka.TopicPartition(topicName, 0),
                    builder.BuildTopology(topicName),
                    supplier.GetConsumer(configuration.ToConsumerConfig(), null),
                    configuration,
                    supplier,
                    producer);
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
            var builder = new SyncPipeBuilder(null, supplier, producer);
            var pipeOutput = builder.Output(topicName, consumeTimeout, configuration, token);
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
