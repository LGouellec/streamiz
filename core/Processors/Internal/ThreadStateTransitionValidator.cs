namespace Kafka.Streams.Net.Processors.Internal
{
    internal interface ThreadStateTransitionValidator
    {
        bool IsValidTransition(ThreadStateTransitionValidator newState);
    }
}
