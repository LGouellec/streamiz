﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.Logging;
using Microsoft.Win32.SafeHandles;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Kafka.Internal;

namespace Streamiz.Kafka.Net.Processors.Internal
{
    internal class TaskManager
    {
        [ThreadStatic] 
        private static StreamTask _currentTask;
        internal static StreamTask CurrentTask
        {
            get
            {
                if (_currentTask == null)
                    return UnassignedStreamTask.Create();
                return _currentTask;
            }
            set => _currentTask = value;
        }

        private readonly ILogger log = Logger.GetLogger(typeof(TaskManager));
        private readonly InternalTopologyBuilder builder;
        private readonly TaskCreator taskCreator;
        private readonly IAdminClient adminClient;
        private readonly IChangelogReader changelogReader;
        private readonly ConcurrentDictionary<TopicPartition, TaskId> partitionsToTaskId = new ConcurrentDictionary<TopicPartition, TaskId>();
        private readonly ConcurrentDictionary<TaskId, StreamTask> activeTasks = new ConcurrentDictionary<TaskId, StreamTask>();
        private Task<List<DeleteRecordsResult>> currentDeleteTask = null;
        
        public IEnumerable<StreamTask> ActiveTasks => activeTasks.Values.ToList();
        public IDictionary<TaskId, ITask> Tasks => activeTasks.ToDictionary(i => i.Key, i => (ITask)i.Value);

        public IConsumer<byte[], byte[]> Consumer { get; internal set; }
        public StreamsProducer Producer { get; }
        public IEnumerable<TaskId> ActiveTaskIds => activeTasks.Keys;
        public bool RebalanceInProgress { get; internal set; }
        internal readonly object _lock = new object();

        internal TaskManager(InternalTopologyBuilder builder, TaskCreator taskCreator, IAdminClient adminClient,
            IChangelogReader changelogReader, StreamsProducer producer)
        {
            this.builder = builder;
            this.taskCreator = taskCreator;
            this.adminClient = adminClient;
            this.changelogReader = changelogReader;
            
            Producer = producer;
        }

        internal TaskManager(InternalTopologyBuilder builder, TaskCreator taskCreator, IAdminClient adminClient, IConsumer<byte[], byte[]>  consumer, IChangelogReader changelogReader, StreamsProducer producer)
            : this(builder, taskCreator, adminClient, changelogReader, producer)
        {
            Consumer = consumer;
        }


        public void CreateTasks(ICollection<TopicPartition> assignment)
        {
            CurrentTask = null;
            IDictionary<TaskId, IList<TopicPartition>> tasksToBeCreated = new Dictionary<TaskId, IList<TopicPartition>>();

            foreach (var partition in new List<TopicPartition>(assignment))
            {
                var taskId = builder.GetTaskIdFromPartition(partition);
                if (!activeTasks.ContainsKey(taskId))
                {
                    if (tasksToBeCreated.ContainsKey(taskId))
                        tasksToBeCreated[taskId].Add(partition);
                    else
                        tasksToBeCreated.Add(taskId, new List<TopicPartition> { partition });
                    partitionsToTaskId.TryAdd(partition, taskId);
                }
            }

            if (tasksToBeCreated.Count > 0)
            {
                var tasks = taskCreator.CreateTasks(Consumer, Producer, tasksToBeCreated);
                foreach (var task in tasks)
                {
                    task.InitializeStateStores();
                    task.InitializeTopology();
                    activeTasks.TryAdd(task.Id, task);
                }
            }
        }

        public void RevokeTasks(ICollection<TopicPartition> revokeAssignment)
        {
            CurrentTask = null;
            List<StreamTask> revokedTask = new List<StreamTask>();
            List<StreamTask> commitNeededActiveTask = new List<StreamTask>();
            List<TopicPartitionOffset> consumedOffsetsToCommit = new List<TopicPartitionOffset>();
            
            foreach (var p in revokeAssignment)
            {
                var taskId = builder.GetTaskIdFromPartition(p);
                if (activeTasks.TryGetValue(taskId, out StreamTask task))
                { 
                    revokedTask.Add(task);

                    partitionsToTaskId.TryRemove(p, out _);
                    activeTasks.TryRemove(taskId, out _);
                }
            }

            foreach (var activeTask in activeTasks)
            {
                if(activeTask.Value.CommitNeeded)
                    commitNeededActiveTask.Add(activeTask.Value);
            }

            foreach (var rvTask in revokedTask)
                consumedOffsetsToCommit.AddRange(rvTask.PrepareCommit());

            if (consumedOffsetsToCommit.Any())
            {
                foreach(var acT in commitNeededActiveTask)
                    consumedOffsetsToCommit.AddRange(acT.PrepareCommit());
            }
            
            CommitOffsetsOrTransaction(consumedOffsetsToCommit);

            foreach (var rvTask in revokedTask)
            {
                rvTask.PostCommit(true);
                rvTask.Suspend();
                rvTask.Close(false);
            }
            
            foreach(var acT in commitNeededActiveTask)
                acT.PostCommit(false);
            
            revokedTask.Clear();
            commitNeededActiveTask.Clear();
        }

        public StreamTask ActiveTaskFor(TopicPartition partition)
        {
            if (partitionsToTaskId.TryGetValue(partition, out TaskId taskId))
            {
                if (activeTasks.TryGetValue(taskId, out StreamTask task))
                {
                    return task;
                }
            }

            return null;
        }

        public void Close()
        {
            List<TopicPartitionOffset> consumedOffsets = new List<TopicPartitionOffset>();
            CurrentTask = null;
            foreach (var t in activeTasks)
            {
                CurrentTask = t.Value;
                consumedOffsets.AddRange(t.Value.PrepareCommit());
            }

            CurrentTask = null;
            
            CommitOffsetsOrTransaction(consumedOffsets);
            
            foreach (var task in activeTasks.Values) // flush all active tasks
            {
                CurrentTask = task;
                task.PostCommit(true);
                task.Suspend();
                task.Close(false);
            }
            
            Producer.Dispose();

            activeTasks.Clear();
            CurrentTask = null;
            
            partitionsToTaskId.Clear();
            
            changelogReader.Clear();

            // if one delete request is in progress, we wait the result before closing the manager
            if (currentDeleteTask is {IsCompleted: false})
                currentDeleteTask.GetAwaiter().GetResult();
        }
        
        internal int CommitAll()
        {
            int committed = 0;
            var purgeOffsets = new Dictionary<TopicPartition, long>();
            if (RebalanceInProgress)
            {
                return -1;
            }

            List<StreamTask> tasksToCommit = new List<StreamTask>();
            List<TopicPartitionOffset> consumedOffsets = new List<TopicPartitionOffset>();
            
            foreach (var t in ActiveTasks)
            {
                CurrentTask = t;
                if (t.CommitNeeded || t.CommitRequested)
                {
                    purgeOffsets.AddRange(t.PurgeOffsets);
                    tasksToCommit.Add(t);
                    consumedOffsets.AddRange(t.PrepareCommit());
                }
            }

            CurrentTask = null;
            
            CommitOffsetsOrTransaction(consumedOffsets);

            foreach (var task in tasksToCommit)
            {
                CurrentTask = task;
                task.PostCommit(false);
                ++committed;
            }

            CurrentTask = null;
            
            if (committed > 0) // try to purge the committed records for repartition topics if possible
                PurgeCommittedRecords(purgeOffsets);
            
            tasksToCommit.Clear();
            return committed;
        }

        private void CommitOffsetsOrTransaction(IEnumerable<TopicPartitionOffset> offsets)
        {
            var offsetsToCommit = offsets.ToList();
            if (taskCreator.Configuration.Guarantee == ProcessingGuarantee.AT_LEAST_ONCE)
            {
                if (offsetsToCommit.Any())
                {
                    try
                    {
                        Consumer.Commit(offsetsToCommit);
                    }
                    catch (KafkaException kafkaException)
                    {
                        throw new TaskMigratedException(
                            $"Consumer committing offsets failed, indicating the corresponding thread is no longer part of the group : {kafkaException.Message}");
                    }
                }
            }else if (taskCreator.Configuration.Guarantee == ProcessingGuarantee.EXACTLY_ONCE)
            {
                if (offsetsToCommit.Any() || Producer.TransactionInFlight)
                {
                    Producer.CommitTransaction(offsetsToCommit, Consumer.ConsumerGroupMetadata);
                }
            }
        }

        internal int Process(long now)
        {
            int processed = 0;

            lock (_lock)
            {
                foreach (var task in ActiveTasks)
                {
                    try
                    {
                        CurrentTask = task;
                        if (task.CanProcess(now) && task.Process())
                        {
                            processed++;
                        }
                    }
                    catch (Exception e)
                    {
                        log.LogError(
                            e, "Failed to process stream task {TasksId} due to the following error:", task.Id);
                        throw;
                    }
                }
            }

            CurrentTask = null;
            return processed;
        }

        internal int Punctuate()
        {
            int punctuated = 0;
            foreach (var task in ActiveTasks)
            {
                try
                {
                    if (task.PunctuateStreamTime())
                        ++punctuated;
                    if (task.PunctuateSystemTime())
                        ++punctuated;
                }
                catch (TaskMigratedException)
                {
                    log.LogInformation(
                        $"Failed to punctuate stream task {task.Id} since it got migrated to another thread already. " +
                        "Will trigger a new rebalance and close all tasks as zombies together.");
                    throw;
                }
                catch (StreamsException e)
                {
                    log.LogError($"Failed to punctuate stream task {task.Id} due to the following error: {e.Message}");
                    throw;
                }
                catch (KafkaException e)
                {
                    log.LogError($"Failed to punctuate stream task {task.Id} due to the following error: {e.Message}");
                    throw new StreamsException(e);
                }
            }
            return punctuated;
        }
        
        internal void HandleLostAll()
        {
            log.LogDebug("Closing lost active tasks as zombies");
            CurrentTask = null;

            if (Producer.EosEnabled)
                Producer.AbortTransaction();

            var enumerator = activeTasks.GetEnumerator();
            while (enumerator.MoveNext())
            {
                var task = enumerator.Current.Value;
                task.PrepareCommit();
                task.Suspend();
                task.PostCommit(true);
                task.Close(true);
                
                foreach(var part in task.Partition)
                {
                    partitionsToTaskId.TryRemove(part, out TaskId taskId);
                }
            }
            activeTasks.Clear();
            
            if (Producer.EosEnabled)
                Producer.ResetProducer();
        }

        internal bool NeedRestoration()
            => ActiveTasks.Any(t => t.State == TaskState.CREATED || t.State == TaskState.RESTORING);

        internal bool TryToCompleteRestoration()
        {
            bool allRunning = true;

            foreach(var task in ActiveTasks)
            {
                try
                {
                    task.RestorationIfNeeded();
                }catch(Exception e)
                {
                    log.LogDebug($"Could not initialize task {task.Id} at {DateTime.Now.GetMilliseconds()}, will retry ({e.Message})");
                    allRunning = false;
                }
            }

            var activeTasksWithStateStore = ActiveTasks.Where(t => t.HasStateStores);
            if (allRunning && activeTasksWithStateStore.Any())
            {
                var restored = changelogReader.CompletedChangelogs;
                foreach(var task in activeTasksWithStateStore)
                {
                    if (!task.ChangelogPartitions.Any() || (restored.Any() && task.ChangelogPartitions.ContainsAll(restored)))
                        task.CompleteRestoration();
                    else
                        allRunning = false;
                }
            }
            
            return allRunning;
        }

        private void PurgeCommittedRecords(Dictionary<TopicPartition, long> offsets)
        {
            if (currentDeleteTask == null || currentDeleteTask.IsCompleted)
            {
                if (currentDeleteTask != null && currentDeleteTask.IsFaulted)
                    log.LogDebug($"Previous delete-records request has failed. Try sending the new request now.");

                var recordsToDelete = new List<TopicPartitionOffset>();
                recordsToDelete.AddRange(offsets.Select(k => new TopicPartitionOffset(k.Key,k.Value)));

                if (recordsToDelete.Any())
                {
                    currentDeleteTask = adminClient.DeleteRecordsAsync(recordsToDelete);
                    log.LogDebug($"Sent delete-records request: {string.Join(",", recordsToDelete)}");
                }
            }
        }
    }
}