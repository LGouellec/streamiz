using Confluent.Kafka;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.Stream.Internal;
using System;
using System.Collections.Generic;
using System.Text;

namespace Streamiz.Kafka.Net.Processors
{
    internal class GlobalStreamThreadFactory
    {
        private IAdminClient adminClient;
        private ProcessorTopology topology;
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
            var stateManager = new GlobalStateManager(this.topology, this.adminClient);
            var context = new ProcessorContext(this.configuration, stateManager);
            stateManager.SetGlobalProcessorContext(context);
            var globalStateUpdateTask = new GlobalStateUpdateTask(stateManager, this.topology, context);

            return new GlobalStreamThread(threadClientId, globalConsumer, configuration, globalStateUpdateTask);
        }
    }
}
