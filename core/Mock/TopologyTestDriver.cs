using Confluent.Kafka;
using kafka_stream_core.Errors;
using kafka_stream_core.Kafka;
using kafka_stream_core.Kafka.Internal;
using kafka_stream_core.Mock.Kafka;
using kafka_stream_core.Mock.Pipes;
using kafka_stream_core.Processors;
using kafka_stream_core.Processors.Internal;
using kafka_stream_core.SerDes;
using kafka_stream_core.Stream;
using kafka_stream_core.Stream.Internal;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace kafka_stream_core.Mock
{
    public class TopologyTestDriver : IDisposable
    {
        private readonly CancellationTokenSource tokenSource = new CancellationTokenSource();
        private readonly InternalTopologyBuilder topologyBuilder;
        private readonly IStreamConfig configuration;
        private readonly ProcessorTopology processorTopology;

        private readonly IDictionary<string, IPipeInput> inputs = new Dictionary<string, IPipeInput>();
        private readonly IDictionary<string, IPipeOutput> outputs = new Dictionary<string, IPipeOutput>();
        private readonly PipeBuilder pipeBuilder = null;

        private readonly IThread threadTopology = null;
        private readonly IKafkaSupplier kafkaSupplier = null;

        public TopologyTestDriver(Topology topology, IStreamConfig config)
            :this(topology.Builder, config)
        { }

        private TopologyTestDriver(InternalTopologyBuilder builder, IStreamConfig config)
        {
            this.topologyBuilder = builder;
            this.configuration = config;
            // ONLY 1 thread for test driver
            this.configuration.NumStreamThreads = 1;

            var processID = Guid.NewGuid();
            var clientId = string.IsNullOrEmpty(configuration.ClientId) ? $"{this.configuration.ApplicationId.ToLower()}-{processID}" : configuration.ClientId;
            this.configuration.ClientId = clientId;

            kafkaSupplier = new MockKafkaSupplier();
            pipeBuilder = new PipeBuilder(kafkaSupplier);

            this.processorTopology = this.topologyBuilder.BuildTopology();

            this.threadTopology = StreamThread.Create(
                $"{this.configuration.ApplicationId.ToLower()}-stream-thread-0",
                clientId,
                builder,
                config,
                kafkaSupplier,
                kafkaSupplier.GetAdmin(configuration.ToAdminConfig($"{clientId}-admin")),
                0);

            RunDriver();
        }

        private void RunDriver()
        {
            bool isRunningState = false;
            DateTime dt = DateTime.Now;
            TimeSpan timeout = TimeSpan.FromSeconds(30);

            threadTopology.StateChanged += (thread, old, @new) => {
                if (@new is Processors.ThreadState && ((Processors.ThreadState)@new) == Processors.ThreadState.PARTITIONS_ASSIGNED)
                    isRunningState = true;
            };

            threadTopology.Start(tokenSource.Token);
            while (!isRunningState)
            {
                Thread.Sleep(250);
                if (DateTime.Now > dt + timeout)
                    throw new StreamsException($"Test topology driver can't initiliaze state after {timeout.TotalSeconds} seconds !");
            }
        }

        public void Dispose()
        {
            tokenSource.Cancel();
            threadTopology.Dispose();

            foreach (var k in inputs)
                k.Value.Dispose();

            foreach (var k in outputs)
                k.Value.Dispose();
        }

        #region Create Input Topic

        public TestInputTopic<K,V> CreateInputTopic<K, V>(string topicName)
            => CreateInputTopic<K, V>(topicName, null, null);

        public TestInputTopic<K,V> CreateInputTopic<K, V>(string topicName, ISerDes<K> keySerdes, ISerDes<V> valueSerdes)
        {
            var pipe = pipeBuilder.Input(topicName, this.configuration);
            inputs.Add(topicName, pipe);
            return new TestInputTopic<K, V>(pipe, this.configuration, keySerdes, valueSerdes);
        }

        public TestInputTopic<K, V> CreateInputTopic<K, V, KS, VS>(string topicName)
            where KS : ISerDes<K>, new()
            where VS : ISerDes<V>, new()
            => CreateInputTopic<K, V>(topicName, new KS(), new VS());

        #endregion

        #region Create Output Topic

        public TestOutputTopic<K, V> CreateOuputTopic<K, V>(string topicName)
            => CreateOuputTopic<K, V>(topicName, TimeSpan.FromSeconds(5), null, null);

        public TestOutputTopic<K, V> CreateOuputTopic<K, V>(string topicName, TimeSpan consumeTimeout, ISerDes<K> keySerdes = null, ISerDes<V> valueSerdes = null)
        {
            var pipe = pipeBuilder.Output(topicName, consumeTimeout, this.configuration, this.tokenSource.Token);
            outputs.Add(topicName, pipe);
            return new TestOutputTopic<K, V>(pipe, this.configuration, keySerdes, valueSerdes);
        }

        public TestOutputTopic<K, V> CreateOuputTopic<K, V, KS, VS>(string topicName)
            where KS : ISerDes<K>, new()
            where VS : ISerDes<V>, new()
            => CreateOuputTopic<K, V, KS, VS>(topicName, TimeSpan.FromSeconds(5));

        public TestOutputTopic<K, V> CreateOuputTopic<K, V, KS, VS>(string topicName, TimeSpan consumeTimeout)
            where KS : ISerDes<K>, new()
            where VS : ISerDes<V>, new()
            => CreateOuputTopic<K, V>(topicName, consumeTimeout, new KS(), new VS());

        #endregion
    }
}
