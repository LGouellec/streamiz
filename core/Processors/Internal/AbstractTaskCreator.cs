using Confluent.Kafka;
using log4net;
using Streamiz.Kafka.Net.Crosscutting;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Processors.Internal
{
    internal abstract class AbstractTaskCreator<T> where T : ITask
    {
        protected readonly ILog log;

        protected AbstractTaskCreator()
        {
            log = Logger.GetLogger(GetType());
        }

        public ICollection<T> CreateTasks(IConsumer<byte[], byte[]> consumer, IDictionary<TaskId, IList<TopicPartition>> tasksToBeCreated)
        {
            List<T> createdTasks = new List<T>();
            foreach (var newTaskAndPartitions in tasksToBeCreated)
            {
                T task = CreateTask(consumer, newTaskAndPartitions.Key, newTaskAndPartitions.Value);
                if (task != null)
                {
                    log.Debug($"Created task {newTaskAndPartitions.Key} with assigned partition {string.Join(",", newTaskAndPartitions.Value)}");
                    createdTasks.Add(task);
                }

            }
            return createdTasks;
        }

        public abstract T CreateTask(IConsumer<byte[], byte[]> consumer, TaskId id, IEnumerable<TopicPartition> partition);
    }
}