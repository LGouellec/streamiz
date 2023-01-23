using System.IO;
using Streamiz.Kafka.Net.Metrics;

namespace Streamiz.Kafka.Net.Processors.Internal
{
    internal class GlobalProcessorContext : ProcessorContext
    {
        internal GlobalProcessorContext(
            IStreamConfig configuration,
            IStateManager stateManager,
            StreamMetricsRegistry streamMetricsRegistry)
            : base(null, configuration, stateManager, streamMetricsRegistry) 
        {
        }

        public override TaskId Id => new TaskId { Id = -1, Partition = -1 };

        public override string StateDir => $"{Path.Combine(Configuration.StateDir, Configuration.ApplicationId, "global")}";
    }
}