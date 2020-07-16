namespace Streamiz.Kafka.Net.Processors.Internal
{
    internal class TaskId
    {
        public int Id { get; set; }
        public int Partition { get; set; }

        public override string ToString() => $"{Id}-{Partition}";

        public override bool Equals(object obj)
        {
            return obj is TaskId && (obj as TaskId).Id.Equals(Id) && (obj as TaskId).Partition.Equals(Partition);
        }

        public override int GetHashCode()
        {
            return Id.GetHashCode() & Partition.GetHashCode();
        }
    }
}
