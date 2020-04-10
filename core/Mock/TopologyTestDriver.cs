using Confluent.Kafka;
using kafka_stream_core.Kafka.Internal;
using kafka_stream_core.Mock.Pipes;
using kafka_stream_core.Processors;
using kafka_stream_core.Processors.Internal;
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
        private readonly PipeBuilder pipeBuilder = new PipeBuilder();

        private readonly IThread threadTopology = null;

        public TopologyTestDriver(Topology topology, IStreamConfig config)
            :this(topology.Builder, config)
        { }

        // https://github.com/confluentinc/confluent-kafka-dotnet/blob/1.4.x/test/Confluent.Kafka.UnitTests/MoqExample.cs

        private TopologyTestDriver(InternalTopologyBuilder builder, IStreamConfig config)
        {
            this.topologyBuilder = builder;
            this.configuration = config;
            this.configuration.NumStreamThreads = 1;
            // MOCK CLUSTER
            this.configuration.AddConfig("test.mock.num.brokers", "3");

            var processID = Guid.NewGuid();
            var clientId = string.IsNullOrEmpty(configuration.ClientId) ? $"{this.configuration.ApplicationId.ToLower()}-{processID}" : configuration.ClientId;
            this.configuration.ClientId = clientId;

            var kafkaSupplier = new DefaultKafkaClientSupplier(new KafkaLoggerAdapter(configuration));

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
            threadTopology.Start(tokenSource.Token);
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

        public TestInputTopic CreateInputTopic()
        {
            var pipe = pipeBuilder.Input("", this.configuration.ToProducerConfig());
            return null;
        }

        public TestOutputTopic CreateOuputTopic()
        {
            var pipe = pipeBuilder.Output("", TimeSpan.FromSeconds(1), this.configuration.ToConsumerConfig());
            return null;
        }
    }
}
