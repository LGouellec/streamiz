namespace Streamiz.Kafka.Net.Processors.Internal
{
    internal interface ThreadStateTransitionValidator
    {
        bool IsValidTransition(ThreadStateTransitionValidator newState);
    }
}
