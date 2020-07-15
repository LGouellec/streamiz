using Confluent.Kafka;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Processors.Internal
{
    internal class TaskManager
    {
        private readonly InternalTopologyBuilder builder;
        private readonly TaskCreator taskCreator;
        private readonly IAdminClient adminClient;

        private readonly IDictionary<TopicPartition, TaskId> partitionsToTaskId = new Dictionary<TopicPartition, TaskId>();
        private readonly IDictionary<TaskId, StreamTask> activeTasks = new Dictionary<TaskId, StreamTask>();
        private readonly IDictionary<TaskId, StreamTask> revokedTasks = new Dictionary<TaskId, StreamTask>();

        public IEnumerable<StreamTask> ActiveTasks => activeTasks.Values;
        public IEnumerable<StreamTask> RevokedTasks => revokedTasks.Values;

        public IConsumer<byte[], byte[]> Consumer { get; internal set; }
        public IEnumerable<TaskId> ActiveTaskIds => activeTasks.Keys;
        public IEnumerable<TaskId> RevokeTaskIds => revokedTasks.Keys;
        public bool RebalanceInProgress { get; internal set; }

        public TaskManager(InternalTopologyBuilder builder, TaskCreator taskCreator, IAdminClient adminClient)
        {
            this.builder = builder;
            this.taskCreator = taskCreator;
            this.adminClient = adminClient;
        }

        public TaskManager(InternalTopologyBuilder builder, TaskCreator taskCreator, IAdminClient adminClient, IConsumer<byte[], byte[]> consumer)
            : this(builder, taskCreator, adminClient)
        {
            Consumer = consumer;
        }

        public void CreateTasks(ICollection<TopicPartition> assignment)
        {
            IDictionary<TaskId, IList<TopicPartition>> tasksToBeCreated = new Dictionary<TaskId, IList<TopicPartition>>();

            foreach (var partition in assignment)
            {
                var taskId = builder.GetTaskIdFromPartition(partition);
                if (revokedTasks.ContainsKey(taskId))
                {
                    var t = revokedTasks[taskId];
                    t.Resume();
                    activeTasks.Add(taskId, t);
                    revokedTasks.Remove(taskId);
                }
                else if (!activeTasks.ContainsKey(taskId))
                {
                    if (tasksToBeCreated.ContainsKey(taskId))
                        tasksToBeCreated[taskId].Add(partition);
                    else
                        tasksToBeCreated.Add(taskId, new List<TopicPartition> { partition });
                }
            }

            if (tasksToBeCreated.Count > 0)
            {
                var tasks = taskCreator.CreateTasks(Consumer, tasksToBeCreated);
                foreach (var task in tasks)
                {
                    task.GroupMetadata = Consumer.ConsumerGroupMetadata;
                    task.InitializeStateStores();
                    task.InitializeTopology();
                    activeTasks.Add(task.Id, task);
                }
            }
        }

        public void RevokeTasks(ICollection<TopicPartition> assignment)
        {
            foreach (var p in assignment)
            {
                var taskId = builder.GetTaskIdFromPartition(p);
                if (activeTasks.ContainsKey(taskId))
                {
                    var task = activeTasks[taskId];
                    task.Suspend();
                    if (!revokedTasks.ContainsKey(taskId))
                    {
                        revokedTasks.Add(taskId, task);
                    }

                    activeTasks.Remove(taskId);
                }
            }
        }

        public StreamTask ActiveTaskFor(TopicPartition partition)
        {
            if (partitionsToTaskId.ContainsKey(partition))
            {
                return activeTasks[partitionsToTaskId[partition]];
            }
            else
            {
                return null;
            }
        }

        public void Close()
        {
            foreach (var t in activeTasks)
            {
                t.Value.Close();
            }

            activeTasks.Clear();

            foreach (var t in revokedTasks)
            {
                t.Value.Close();
            }

            revokedTasks.Clear();
        }

        internal int CommitAll()
        {
            int committed = 0;
            if (RebalanceInProgress)
            {
                return -1;
            }
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