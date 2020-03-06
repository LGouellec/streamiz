namespace kafka_stream_core.Processors.Internal
{
    internal interface ThreadStateTransitionValidator
    {
        bool IsValidTransition(ThreadStateTransitionValidator newState);
    }
}
