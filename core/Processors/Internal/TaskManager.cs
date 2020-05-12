using Confluent.Kafka;
using System.Collections.Generic;
using System.Linq;

namespace Streamiz.Kafka.Net.Processors.Internal
{
    internal class TaskManager
    {
        private readonly TaskCreator taskCreator;
        private readonly IAdminClient adminClient;
        private int idTask = 0;

        private readonly IDictionary<TopicPartition, StreamTask> activeTasks = new Dictionary<TopicPartition, StreamTask>();
        private readonly IDictionary<TopicPartition, StreamTask> revokedTasks = new Dictionary<TopicPartition, StreamTask>();

        public IEnumerable<StreamTask> ActiveTasks => activeTasks.Values;
        public IEnumerable<StreamTask> RevokedTasks => revokedTasks.Values;

        public IConsumer<byte[], byte[]> Consumer { get; internal set; }
        public IEnumerable<TaskId> ActiveTaskIds => ActiveTasks.Select(a => a.Id);
        public IEnumerable<TaskId> RevokeTaskIds => RevokedTasks.Select(a => a.Id);
        public bool RebalanceInProgress { get; internal set; }

        public TaskManager(TaskCreator taskCreator, IAdminClient adminClient)
        {
            this.taskCreator = taskCreator;
            this.adminClient = adminClient;
        }

        public TaskManager(TaskCreator taskCreator, IAdminClient adminClient, IConsumer<byte[], byte[]> consumer)
        {
            this.taskCreator = taskCreator;
            this.adminClient = adminClient;
            Consumer = consumer;
        }

        public void CreateTasks(ICollection<TopicPartition> assignment)
        {
            ++idTask;
            foreach (var partition in assignment)
            {
                if (revokedTasks.ContainsKey(partition))
                {
                    var t = revokedTasks[partition];
                    t.Resume();
                    activeTasks.Add(partition, t);
                    revokedTasks.Remove(partition);
                }
                else if (!activeTasks.ContainsKey(partition))
                {
                    var id = new TaskId { Id = idTask, Partition = partition.Partition.Value, Topic = partition.Topic };
                    var task = taskCreator.CreateTask(Consumer, id, partition);
                    task.GroupMetadata = Consumer.ConsumerGroupMetadata;
                    task.InitializeStateStores();
                    task.InitializeTopology();
                    activeTasks.Add(partition, task);
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
                    if (!revokedTasks.ContainsKey(p))
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

            activeTasks.Clear();

            foreach (var t in revokedTasks)
                t.Value.Close();

            revokedTasks.Clear();
        }

        internal int CommitAll()
        {
            int committed = 0;
            if (RebalanceInProgress)
                return -1;
            else
            {
                foreach (var t in ActiveTasks)
                {
                    if (t.CommitNeeded)
                    {
                        t.Commit();
                        ++committed;
                    }
                }
                return committed;
            }
        }
    }
}