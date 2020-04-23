using Confluent.Kafka;
using log4net;
using Streamiz.Kafka.Net.Crosscutting;
using System;
using System.Collections.Generic;
using System.Text;

namespace Streamiz.Kafka.Net.Processors.Internal
{
    internal abstract class AbstractTaskCreator<T> where T : ITask
    {
        InternalTopologyBuilder builder;
        IStreamConfig config;
        ILog log;

        public AbstractTaskCreator(InternalTopologyBuilder builder, IStreamConfig config)
        {
            this.builder = builder;
            this.config = config;
            this.log = Logger.GetLogger(this.GetType());
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
                    log.Debug($"Created task {taskId} with assigned partition {partition}");
                    createdTasks.Add(task);
                }

            }
            return createdTasks;
        }

        public abstract T CreateTask(IConsumer<byte[], byte[]> consumer, TaskId id, TopicPartition partitions);
    }
}