using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Processors.Internal
{
    internal interface IGlobalStateManager : IStateManager
    {
        ISet<string> Initialize();

        void SetGlobalProcessorContext(ProcessorContext processorContext);
    }
}
