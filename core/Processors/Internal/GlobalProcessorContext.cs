using System.IO;

namespace Streamiz.Kafka.Net.Processors.Internal
{
    internal class GlobalProcessorContext : ProcessorContext
    {
        internal GlobalProcessorContext(IStreamConfig configuration, IStateManager stateManager)
            : base(null, configuration, stateManager)
        {
        }

        public override TaskId Id => new TaskId { Id = -1, Partition = -1 };

        public override string StateDir => $"{Path.Combine(Configuration.StateDir, Configuration.ApplicationId, "global")}";
        
        public override void Commit() { /* nothing */ }
    }
}