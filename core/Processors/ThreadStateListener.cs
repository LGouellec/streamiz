using kafka_stream_core.Processors.Internal;

namespace kafka_stream_core.Processors
{
    internal delegate void ThreadStateListener(IThread thread, ThreadStateTransitionValidator old, ThreadStateTransitionValidator @new);

}
