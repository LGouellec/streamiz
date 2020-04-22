using Confluent.Kafka;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Processors.Internal
{
    internal class TaskManager
    {
        private TaskCreator taskCreator;
        private IAdminClient adminClient;
        private int idTask = 0;

        private readonly IDictionary<TopicPartition, StreamTask> activeTasks = new Dictionary<TopicPartition, StreamTask>();
        private readonly IDictionary<TopicPartition, StreamTask> revokedTasks = new Dictionary<TopicPartition, StreamTask>();

        public IEnumerable<StreamTask> ActiveTasks => activeTasks.Values;
        public IConsumer<byte[], byte[]> Consumer { get; internal set; }

        public TaskManager(TaskCreator taskCreator, IAdminClient adminClient)
        {
            this.taskCreator = taskCreator;
            this.adminClient = adminClient;
        }

        public void CreateTasks(IConsumer<byte[], byte[]> consumer, ICollection<TopicPartition> assignment)
        {
            ++idTask;
            foreach (var partition in assignment)
            {
                if (!activeTasks.ContainsKey(partition))
                {
                    var id = new TaskId { Id = idTask, Partition = partition.Partition.Value, Topic = partition.Topic };
                    var task = taskCreator.CreateTask(Consumer, id, partition);
                    task.GroupMetadata = consumer.ConsumerGroupMetadata;
                    task.InitializeStateStores();
                    task.InitializeTopology();
                    activeTasks.Add(partition, task);
                }
                else if(revokedTasks.ContainsKey(partition))
                {
                    var t = revokedTasks[partition];
                    t.Resume();
                    activeTasks.Add(partition, t);
                    revokedTasks.Remove(partition);
                }
            }
        }

        public void RevokeTasks(ICollection<TopicPartition> assignment)
        {
            foreach (var p in assignment)
            {
                if (activeTasks.ContainsKey(p))
                {
                    var task = activeTasks[p];
                    task.Suspend();
                    if(!revokedTasks.ContainsKey(p))
                        revokedTasks.Add(p, task);
                    activeTasks.Remove(p);
                }
            }
        }

        public StreamTask ActiveTaskFor(TopicPartition partition)
        {
            if (activeTasks.ContainsKey(partition))
                return activeTasks[partition];
            else
                return null;
        }
    
        public void Close()
        {
            foreach (var t in activeTasks)
                t.Value.Close();

            foreach (var t in revokedTasks)
                t.Value.Close();
        }
    }
}