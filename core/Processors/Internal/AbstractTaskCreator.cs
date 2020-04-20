using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Text;

namespace Streamiz.Kafka.Net.Processors.Internal
{
    internal abstract class AbstractTaskCreator<T> where T : ITask
    {
        InternalTopologyBuilder builder;
        IStreamConfig config;

        public AbstractTaskCreator(InternalTopologyBuilder builder, IStreamConfig config)
        {
            this.builder = builder;
            this.config = config;
        }

        public ICollection<T> CreateTasks(IConsumer<byte[], byte[]> consumer, IDictionary<TaskId, TopicPartition> tasksToBeCreated)
        {
            List<T> createdTasks = new List<T>();
            foreach (var newTaskAndPartitions in tasksToBeCreated)
            {
                TaskId taskId = newTaskAndPartitions.Key;
                TopicPartition partition = newTaskAndPartitions.Value;
                T task = this.CreateTask(consumer, taskId, partition);
                if (task != null)
                {
                    //log.trace("Created task {} with assigned partitions {}", taskId, partitions);
                    createdTasks.Add(task);
                }

            }
            return createdTasks;
        }

        public abstract T CreateTask(IConsumer<byte[], byte[]> consumer, TaskId id, TopicPartition partitions);
    }
}
