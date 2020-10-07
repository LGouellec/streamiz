namespace Streamiz.Kafka.Net.Processors.Internal
{
    /// <summary>
    /// The task ID representation composed as topic group ID plus the assigned partition ID.
    /// </summary>
    public class TaskId
    {
        /// <summary>
        /// The ID of the topic group.
        /// </summary>
        public int Id { get; set; }

        /// <summary>
        /// The ID of the partition.
        /// </summary>
        public int Partition { get; set; }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override string ToString() => $"{Id}-{Partition}";

        /// <summary>
        /// 
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public override bool Equals(object obj)
        {
            return obj is TaskId && (obj as TaskId).Id.Equals(Id) && (obj as TaskId).Partition.Equals(Partition);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override int GetHashCode()
        {
            return Id.GetHashCode() & Partition.GetHashCode();
        }
    }
}
