using Streamiz.Kafka.Net.Processors.Internal;

namespace Streamiz.Kafka.Net.Processors
{
    internal delegate void ThreadStateListener(IThread thread, ThreadStateTransitionValidator old, ThreadStateTransitionValidator @new);

}
