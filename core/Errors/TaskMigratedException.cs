namespace Kafka.Streams.Net.Errors
{
    public class TaskMigratedException : StreamsException
    {
        public TaskMigratedException()
            :base("")
        {

        }
        //public Task Task { get; }

        //public TaskMigratedException(Task task, TopicPartition topicPartition, long endOffset,long pos)
        //    :base($"Log end offset of {topicPartition} should not change while restoring: old end offset {endOffset}, current offset {pos}")
        //{
        //    this.Task = task;
        //}
    }
}
