using Kafka.Streams.Net.Processors.Internal;

namespace Kafka.Streams.Net.Processors
{
    internal delegate void ThreadStateListener(IThread thread, ThreadStateTransitionValidator old, ThreadStateTransitionValidator @new);

}
