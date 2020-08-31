using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Kafka;
using Streamiz.Kafka.Net.Mock.Kafka;
using Streamiz.Kafka.Net.Mock.Pipes;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.SerDes;
using System;
using System.Collections.Generic;
using System.Threading;

namespace Streamiz.Kafka.Net.Mock
{
    internal sealed class ClusterInMemoryTopologyDriver : IBehaviorTopologyTestDriver
    {
        private readonly IStreamConfig configuration;
        private readonly IStreamConfig topicConfiguration;
        private readonly IPipeBuilder pipeBuilder = null;
        private readonly IThread threadTopology = null;
        private readonly IKafkaSupplier kafkaSupplier = null;
        private readonly CancellationToken token;
        private readonly TimeSpan startTimeout;

        public ClusterInMemoryTopologyDriver(string clientId, InternalTopologyBuilder topologyBuilder, IStreamConfig configuration, IStreamConfig topicConfiguration, CancellationToken token)
            : this(clientId, topologyBuilder, configuration, topicConfiguration, TimeSpan.FromSeconds(30), token)
        {
        }

        public ClusterInMemoryTopologyDriver(string clientId, InternalTopologyBuilder topologyBuilder, IStreamConfig configuration, IStreamConfig topicConfiguration, TimeSpan startTimeout, CancellationToken token)
        {
            this.startTimeout = startTimeout;
            this.configuration = configuration;
            this.configuration.ClientId = clientId;
            this.topicConfiguration = topicConfiguration;
            this.token = token;

            kafkaSupplier = new MockKafkaSupplier();
            pipeBuilder = new KafkaPipeBuilder(kafkaSupplier);

            // ONLY FOR CHECK IF TOLOGY IS CORRECT
            topologyBuilder.BuildTopology();

            threadTopology = StreamThread.Create(
                $"{this.configuration.ApplicationId.ToLower()}-stream-thread-0",
                clientId,
                topologyBuilder,
                this.configuration,
                kafkaSupplier,
                kafkaSupplier.GetAdmin(configuration.ToAdminConfig($"{clientId}-admin")),
                0);
        }

        #region IBehaviorTopologyTestDriver

        public TestInputTopic<K, V> CreateInputTopic<K, V>(string topicName, ISerDes<K> keySerdes, ISerDes<V> valueSerdes)
        {
            var pipeInput = pipeBuilder.Input(topicName, configuration);
            return new TestInputTopic<K, V>(pipeInput, configuration, keySerdes, valueSerdes);
        }

        public TestMultiInputTopic<K, V> CreateMultiInputTopic<K, V>(string[] topics, ISerDes<K> keySerdes = null, ISerDes<V> valueSerdes = null)
        {
            Dictionary<string, IPipeInput> pipes = new Dictionary<string, IPipeInput>();

            foreach (var t in topics)
            {
                var pipeInput = pipeBuilder.Input(t, configuration);
                pipes.Add(t, pipeInput);
            }

            return new TestMultiInputTopic<K, V>(pipes, configuration, keySerdes, valueSerdes);
        }

        public TestOutputTopic<K, V> CreateOutputTopic<K, V>(string topicName, TimeSpan consumeTimeout, ISerDes<K> keySerdes = null, ISerDes<V> valueSerdes = null)
        {
            var pipeOutput = pipeBuilder.Output(topicName, consumeTimeout, topicConfiguration, token);
            return new TestOutputTopic<K, V>(pipeOutput, topicConfiguration, keySerdes, valueSerdes);
        }

        public void Dispose()
        {
            threadTopology.Dispose();
            (kafkaSupplier as MockKafkaSupplier)?.Destroy();
        }

        public IStateStore GetStateStore<K, V>(string name)
        {
            IList<IStateStore> stores = new List<IStateStore>();
            foreach (var task in threadTopology.ActiveTasks)
            {
                var store = task.GetStore(name);
                if (store != null)
                    stores.Add(store);
            }

            return stores.Count > 0 ? new MockReadOnlyKeyValueStore<K, V>(stores) : null;
        }

        public void StartDriver()
        {
            bool isRunningState = false;
            DateTime dt = DateTime.Now;

            threadTopology.StateChanged += (thread, old, @new) =>
            {
                if (@new is Processors.ThreadState && ((Processors.ThreadState)@new) == Processors.ThreadState.RUNNING)
                    isRunningState = true;
            };
            
            threadTopology.ThrowException = false;
            threadTopology.Start(token);
            while (!isRunningState)
            {
                Thread.Sleep(250);
                if (DateTime.Now > dt + startTimeout)
                    throw new StreamsException($"Test topology driver can't initiliaze state after {startTimeout.TotalSeconds} seconds !");
            }
        }

        #endregion
    }
}
