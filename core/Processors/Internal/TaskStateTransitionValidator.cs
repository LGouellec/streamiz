namespace Streamiz.Kafka.Net.Processors.Internal
{
    internal interface TaskStateTransitionValidator
    {
        bool IsValidTransition(TaskStateTransitionValidator newState);
    }
}
