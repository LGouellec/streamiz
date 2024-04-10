using Confluent.Kafka;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.Stream.Internal;

namespace Streamiz.Kafka.Net.Processors
{
    internal class GlobalStreamThreadFactory
    {
        private readonly IAdminClient adminClient;
        private readonly StreamMetricsRegistry streamMetricsRegistry;
        private readonly ProcessorTopology topology;
        private readonly IStreamConfig configuration;
        private readonly string threadClientId;
        private readonly IConsumer<byte[], byte[]> globalConsumer;

        public GlobalStreamThreadFactory(ProcessorTopology topology,
            string threadClientId,
            IConsumer<byte[], byte[]> globalConsumer,
            IStreamConfig configuration,
            IAdminClient adminClient,
            StreamMetricsRegistry streamMetricsRegistry)
        {
            this.adminClient = adminClient;
            this.streamMetricsRegistry = streamMetricsRegistry;
            this.topology = topology;
            this.threadClientId = threadClientId;
            this.configuration = configuration;
            this.globalConsumer = globalConsumer;
        }

        public GlobalStreamThread GetGlobalStreamThread()
        {
            var stateManager = new GlobalStateManager(globalConsumer, topology, adminClient, configuration);
            var context = new GlobalProcessorContext(configuration, stateManager, streamMetricsRegistry);
            stateManager.SetGlobalProcessorContext(context);
            var globalStateUpdateTask = new GlobalStateUpdateTask(stateManager, topology, context);

            return new GlobalStreamThread(threadClientId, globalConsumer, configuration, globalStateUpdateTask, streamMetricsRegistry);
        }
    }
}
