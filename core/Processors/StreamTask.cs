﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Kafka;
using Streamiz.Kafka.Net.Kafka.Internal;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Metrics.Internal;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.Processors.Public;
using Streamiz.Kafka.Net.Stream.Internal;

namespace Streamiz.Kafka.Net.Processors
{
    internal class StreamTask : AbstractTask
    {
        private readonly IKafkaSupplier kafkaSupplier;
        private readonly StreamMetricsRegistry streamMetricsRegistry;
        private readonly IRecordCollector collector;
        private readonly IDictionary<TopicPartition, long> consumedOffsets;
        private readonly PartitionGrouper partitionGrouper;
        private readonly IList<IProcessor> processors = new List<IProcessor>();
        private readonly bool eosEnabled;
        private readonly long maxTaskIdleMs;
        private readonly long maxBufferedSize = 100;
        private readonly bool followMetadata;
        private readonly List<TaskScheduled> streamTimePunctuationQueue = new();
        private readonly List<TaskScheduled> systemTimePunctuationQueue = new();

        private long idleStartTime;
        private bool transactionInFlight;
        private readonly string threadId;
        
        private Sensor closeTaskSensor;
        private Sensor activeBufferedRecordSensor;
        private Sensor processSensor;
        private Sensor processLatencySensor;
        private Sensor enforcedProcessingSensor;
        private Sensor commitSensor;
        private Sensor activeRestorationSensor;
        private Sensor restorationRecordsSendsor;


        public StreamTask(string threadId, TaskId id, IEnumerable<TopicPartition> partitions,
            ProcessorTopology processorTopology, IConsumer<byte[], byte[]> consumer, IStreamConfig configuration,
            IKafkaSupplier kafkaSupplier, StreamsProducer producer, IChangelogRegister changelogRegister,
            StreamMetricsRegistry streamMetricsRegistry)
            : base(id, partitions, processorTopology, consumer, configuration, changelogRegister)
        {
            this.threadId = threadId;
            this.kafkaSupplier = kafkaSupplier;
            this.streamMetricsRegistry = streamMetricsRegistry;
            consumedOffsets = new Dictionary<TopicPartition, long>();
            maxTaskIdleMs = configuration.MaxTaskIdleMs;
            maxBufferedSize = configuration.BufferedRecordsPerPartition;
            followMetadata = configuration.FollowMetadata;
            idleStartTime = -1;

            eosEnabled = configuration.Guarantee == ProcessingGuarantee.EXACTLY_ONCE;
            var droppedRecordsSensor = TaskMetrics.DroppedRecordsSensor(this.threadId, Id, this.streamMetricsRegistry);
            collector = new RecordCollector(logPrefix, configuration, id, producer, droppedRecordsSensor);

            Context = new ProcessorContext(this, configuration, stateMgr, streamMetricsRegistry)
                .UseRecordCollector(collector);
            Context.FollowMetadata = followMetadata;

            var partitionsQueue = new Dictionary<TopicPartition, RecordQueue>();

            foreach (var p in partitions)
            {
                var sourceProcessor = processorTopology.GetSourceProcessor(p.Topic);
                sourceProcessor.SetTaskId(id);
                var sourceTimestampExtractor = sourceProcessor.Extractor ?? configuration.DefaultTimestampExtractor;
                var queue = new RecordQueue(
                    logPrefix,
                    $"record-queue-{p.Topic}-{id.Id}-{id.Partition}",
                    sourceTimestampExtractor,
                    p,
                    sourceProcessor,
                    droppedRecordsSensor);
                partitionsQueue.Add(p, queue);
                processors.Add(sourceProcessor);
            }

            partitionGrouper = new PartitionGrouper(partitionsQueue);
            
            RegisterSensors();
        }

        #region Private

        private void RegisterSensors()
        {
            closeTaskSensor = ThreadMetrics.ClosedTaskSensor(threadId, streamMetricsRegistry);                        
            activeBufferedRecordSensor = TaskMetrics.ActiveBufferedRecordsSensor(threadId, Id, streamMetricsRegistry);
            processSensor = TaskMetrics.ProcessSensor(threadId, Id, streamMetricsRegistry);                           
            processLatencySensor = TaskMetrics.ProcessLatencySensor(threadId, Id, streamMetricsRegistry);             
            enforcedProcessingSensor = TaskMetrics.EnforcedProcessingSensor(threadId, Id, streamMetricsRegistry);     
            commitSensor = TaskMetrics.CommitSensor(threadId, Id, streamMetricsRegistry);                             
            activeRestorationSensor = TaskMetrics.ActiveRestorationSensor(threadId, Id, streamMetricsRegistry);       
            restorationRecordsSendsor = TaskMetrics.RestorationRecordsSensor(threadId, Id, streamMetricsRegistry);    
        }

        private IDictionary<TopicPartition, long> CheckpointableOffsets
            => collector.CollectorOffsets
                        .Union(consumedOffsets.AsEnumerable())
                        .ToDictionary();

        private IEnumerable<TopicPartitionOffset> GetPartitionsWithOffset()
        {
            foreach (var kp in consumedOffsets)
            {
                yield return new TopicPartitionOffset(kp.Key, kp.Value + 1);
            }
        }

        private TaskScheduled ScheduleTask(long startTime, TimeSpan interval, PunctuationType punctuationType, Action<long> punctuator)
        {
            var taskScheduled = new TaskScheduled(
                startTime,
                interval,
                punctuator, 
                Context.CurrentProcessor);

            switch (punctuationType)
            {
                case PunctuationType.STREAM_TIME:
                    streamTimePunctuationQueue.Add(taskScheduled);
                    break;
                case PunctuationType.PROCESSING_TIME:
                    systemTimePunctuationQueue.Add(taskScheduled);
                    break;
            }

            return taskScheduled;
        }

        #endregion

        #region Abstract

        public override IDictionary<TopicPartition, long> PurgeOffsets
        {
            get
            {
                var purgeableConsumedOffsets = new Dictionary<TopicPartition, long>();
                foreach(var kv in consumedOffsets)
                    if (Topology.RepartitionTopics.Contains(kv.Key.Topic))
                        purgeableConsumedOffsets.AddOrUpdate(kv.Key, kv.Value + 1);
                return purgeableConsumedOffsets;
            }
        }

        public override PartitionGrouper Grouper => partitionGrouper;

        public override bool CanProcess(long now)
        {
            if (state == TaskState.CLOSED || state == TaskState.RESTORING || state == TaskState.CREATED)
                return false;

            if (partitionGrouper.AllPartitionsBuffered)
            {
                idleStartTime = -1;
                return true;
            }

            if (partitionGrouper.NumBuffered() > 0)
            {
                if (idleStartTime == -1)
                {
                    idleStartTime = now;
                }

                if (now - idleStartTime >= maxTaskIdleMs)
                {
                    enforcedProcessingSensor.Record();
                    return true;
                }

                return false;
            }

            idleStartTime = -1;
            return false;
        }

        public override void Close(bool dirty)
        {
            log.LogInformation($"{logPrefix}Closing");

            if (state == TaskState.CREATED || state == TaskState.RESTORING || state == TaskState.RUNNING)
            {
                throw new IllegalStateException($"Illegal state {state} while closing active task {Id}");
            }

            if (state == TaskState.CLOSED)
            {
                log.LogInformation($"{logPrefix}Skip closing since state is {state}");
                return;
            }

            if (state == TaskState.SUSPENDED)
            {
                partitionGrouper.Close();
                
                foreach (var kp in processors)
                {
                    kp.Close();
                }
                
                streamTimePunctuationQueue.ForEach(t => t.Close());
                systemTimePunctuationQueue.ForEach(t => t.Close());
                
                collector.Close(dirty);
                
                CloseStateManager();

                TransitTo(TaskState.CLOSED);
                
                closeTaskSensor.Record();
                log.LogInformation($"{logPrefix}Closed");
                IsClosed = true;
            }
            else
            {
                throw new IllegalStateException($"Unknow state {state} while suspending active task {Id}");
            }

            streamMetricsRegistry.RemoveTaskSensors(threadId, Id.ToString());
            streamTimePunctuationQueue.Clear();
            systemTimePunctuationQueue.Clear();
        }

        private IEnumerable<TopicPartitionOffset> CommittableTopicPartitionOffsets()
        {
            if (state == TaskState.CREATED || state == TaskState.RESTORING)
                return new List<TopicPartitionOffset>();
            if (state == TaskState.RUNNING || state == TaskState.SUSPENDED)
                return GetPartitionsWithOffset();
            
            throw new IllegalStateException(
                $"Illegal state {state} while getting committable offsets for active task {Id}");
        }
        
        public override IEnumerable<TopicPartitionOffset> PrepareCommit()
        {
            log.LogDebug($"{logPrefix}Preparing to commit");
            
            if (state == TaskState.CREATED ||
                state == TaskState.RUNNING ||
                state == TaskState.RESTORING ||
                state == TaskState.SUSPENDED)
            {
                if (commitNeeded || commitRequested)
                {
                    FlushState();
                    return CommittableTopicPartitionOffsets();
                }
                
                log.LogDebug($"Skipped preparing {state} task for commit since there is nothing to commit");
                return new List<TopicPartitionOffset>();
            }
            
            throw new IllegalStateException($"Illegal state {state} while preparing active task {Id} for committing");
        }

        public override void PostCommit(bool enforceCheckpoint)
        {
            if (state == TaskState.CREATED)
                log.LogDebug($"Skipped writing checkpoint for {state} task");
            else if (state == TaskState.RESTORING || state == TaskState.SUSPENDED)
            {
                MayWriteCheckpoint(enforceCheckpoint);
                log.LogDebug($"Finalized commit for {state} task with enforce checkpoint {enforceCheckpoint}");
            }
            else if (state == TaskState.RUNNING)
            {
                if(enforceCheckpoint || !eosEnabled)
                    MayWriteCheckpoint(enforceCheckpoint);
                
                consumedOffsets.Clear();
                commitNeeded = false;
                commitRequested = false;
                commitSensor.Record();
                
                log.LogDebug($"Finalized commit for {state} task with enforce checkpoint {enforceCheckpoint}");
            }
            else
                throw new IllegalStateException($"Illegal state {state} while post committing active task {Id}");
        }
        
        public override IStateStore GetStore(string name)
        {
            return Context.GetStateStore(name);
        }

        public override void RestorationIfNeeded()
        {
            if(state == TaskState.CREATED)
            {
                if (stateMgr.ChangelogPartitions.Any())
                {
                    stateMgr.InitializeOffsetsFromCheckpoint();

                    TransitTo(TaskState.RESTORING);
                    activeRestorationSensor.Record();
                    log.LogInformation($"{logPrefix}Restoration will start soon.");
                }
                else
                {
                    TransitTo(TaskState.RUNNING);
                }
            }
        }

        public override void InitializeTopology()
        {
            log.LogDebug($"{logPrefix}Initializing topology with theses source processors : {string.Join(", ", processors.Select(p => p.Name))}.");
            
            Context.CurrentProcessor = null;
            foreach (var p in processors)
            {
                p.Init(Context);
            }
            Context.CurrentProcessor = null;

            collector.Initialize();

            taskInitialized = true;
        }

        public override bool InitializeStateStores()
        {
            log.LogDebug($"{logPrefix}Initializing state stores.");
            RegisterStateStores();
            return false;
        }
        
        public override void Suspend()
        {
            log.LogDebug($"{logPrefix}Suspending");

            if (state == TaskState.CREATED || state == TaskState.RESTORING)
            {
                log.LogInformation($"{logPrefix}Suspended {state}");
                TransitTo(TaskState.SUSPENDED);
            }
            else if (state == TaskState.RUNNING)
            {
                foreach (var kp in processors)
                    kp.Close();
                partitionGrouper.Clear();
                log.LogInformation($"{logPrefix}Suspended running");
                TransitTo(TaskState.SUSPENDED);
            }
            else if (state == TaskState.SUSPENDED)
            {
                log.LogInformation($"{logPrefix}Skip suspended since state is {state}");
            }
            else if (state == TaskState.CLOSED)
            {
                throw new IllegalStateException($"Illegal state {state} while suspending active task {Id}");
            }
            else
            {
                throw new IllegalStateException($"Unknow state {state} while suspending active task {Id}");
            }
        }

        protected override void FlushState()
        {
            base.FlushState();
            collector?.Flush();
        }

        public override void MayWriteCheckpoint(bool force = false)
        {
            if (commitNeeded || force)
                stateMgr.UpdateChangelogOffsets(CheckpointableOffsets);

            WriteCheckpoint(force);
        }

        public override TaskScheduled RegisterScheduleTask(TimeSpan interval, PunctuationType punctuationType,
            Action<long> punctuator)
        {
            switch (punctuationType)
            {
                case PunctuationType.STREAM_TIME:
                    // align punctuation to 0L, punctuate as soon as we have data
                    return ScheduleTask(0L, interval, punctuationType, punctuator);
                case PunctuationType.PROCESSING_TIME:
                    // align punctuation to now, punctuate after interval has elapsed
                    return ScheduleTask(DateTime.Now.GetMilliseconds() + (long)interval.TotalMilliseconds, interval, punctuationType, punctuator);
                default:
                    return null;
            }
        }

        #endregion

        public bool Process()
        {
            var record = partitionGrouper.NextRecord;
            if (record == null)
            {
                return false;
            }

            Context.SetRecordMetaData(record.Record);

            var recordInfo = $"Topic:{record.Record.Topic}|Partition:{record.Record.Partition.Value}|Offset:{record.Record.Offset}|Timestamp:{record.Record.Message.Timestamp.UnixTimestampMs}";

            log.LogDebug($"{logPrefix}Start processing one record [{recordInfo}]");
            long latency = ActionHelper.MeasureLatency(() => record.Processor.Process(record.Record));
            log.LogDebug($"{logPrefix}Completed processing one record [{recordInfo}]");

            consumedOffsets.AddOrUpdate(record.Record.TopicPartition, record.Record.Offset);
            commitNeeded = true;

            if (record.Queue.Size == maxBufferedSize)
            {
                consumer.Resume(record.Record.TopicPartition.ToSingle());
            }
                
            processSensor.Record();
            processLatencySensor.Record(latency);
            activeBufferedRecordSensor.Record(Grouper.NumBuffered());
            return true;
        }

        public void AddRecord(ConsumeResult<byte[], byte[]> record)
        {
            int newQueueSize = partitionGrouper.AddRecord(record.TopicPartition, record);

            if (newQueueSize > maxBufferedSize)
            {
                consumer.Pause(record.TopicPartition.ToSingle());
            }

            log.LogDebug($"{logPrefix}Added record into the buffered queue of partition {record.TopicPartition}, new queue size is {newQueueSize}");
        }

        public void AddRecords(IEnumerable<ConsumeResult<byte[], byte[]>> records)
        {
            foreach (var r in records)
            {
                AddRecord(r);
            }
        }

        public void CompleteRestoration()
        {
            if(state == TaskState.RUNNING)
            {
                activeRestorationSensor.Record(0);
                restorationRecordsSendsor.Record(0);
            }
            else if(state == TaskState.RESTORING)
            {
                TransitTo(TaskState.RUNNING);
                activeRestorationSensor.Record(0);
                log.LogInformation($"{logPrefix}Restored and ready to run");
            }
            else if(state == TaskState.CREATED || state == TaskState.SUSPENDED || state == TaskState.CLOSED)
            {
                throw new IllegalStateException($"Illegal state {state} while completing restoration for active task {Id}");
            }
            else
            {
                throw new IllegalStateException($"Illegal state {state} while completing restoration for active task {Id}");
            }
        }

        public bool PunctuateSystemTime()
        {
            long systemTime = DateTime.Now.GetMilliseconds();

            bool punctuated = false;

            foreach (var taskScheduled in systemTimePunctuationQueue
                .Where(t => t.CanExecute(systemTime)))
            {
                Context.CurrentProcessor = taskScheduled.Processor;
                Context.SetUnknownRecordMetaData(systemTime);
                taskScheduled.Execute(systemTime);
                punctuated = true;
            }

            Context.CurrentProcessor = null;

            systemTimePunctuationQueue.RemoveAll(t => t.IsCancelled || t.IsCompleted);
            return punctuated;
        }

        public bool PunctuateStreamTime()
        {
            if (partitionGrouper.StreamTime < 0)
                return false;

            bool punctuated = false;

            foreach (var taskScheduled in streamTimePunctuationQueue
                .Where(t => t.CanExecute(partitionGrouper.StreamTime)))
            {
                Context.CurrentProcessor = taskScheduled.Processor;
                Context.SetUnknownRecordMetaData(partitionGrouper.StreamTime);
                taskScheduled.Execute(partitionGrouper.StreamTime);
                punctuated = true;
            }
            Context.CurrentProcessor = null;
            
            streamTimePunctuationQueue.RemoveAll(t => t.IsCancelled || t.IsCompleted);
            return punctuated;
        }
    }   
}
