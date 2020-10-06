using Confluent.Kafka;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.Stream.Internal;

namespace Streamiz.Kafka.Net.Processors
{
    internal class GlobalStreamThreadFactory
    {
        private readonly IAdminClient adminClient;
        private readonly ProcessorTopology topology;
        private readonly IStreamConfig configuration;
        private readonly string threadClientId;
        private readonly IConsumer<byte[], byte[]> globalConsumer;

        public GlobalStreamThreadFactory(ProcessorTopology topology,
            string threadClientId,
            IConsumer<byte[], byte[]> globalConsumer,
            IStreamConfig configuration,
            IAdminClient adminClient)
        {
            this.adminClient = adminClient;
            this.topology = topology;
            this.threadClientId = threadClientId;
            this.configuration = configuration;
            this.globalConsumer = globalConsumer;
        }

        public GlobalStreamThread GetGlobalStreamThread()
        {
            var stateManager = new GlobalStateManager(topology, adminClient, configuration);
            var context = new GlobalProcessorContext(configuration, stateManager);
            stateManager.SetGlobalProcessorContext(context);
            var globalStateUpdateTask = new GlobalStateUpdateTask(stateManager, topology, context);

            return new GlobalStreamThread(threadClientId, globalConsumer, configuration, globalStateUpdateTask);
        }
    }
}
