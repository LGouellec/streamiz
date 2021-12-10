using Confluent.Kafka;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Kafka;
using Streamiz.Kafka.Net.Kafka.Internal;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.Stream.Internal;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Streamiz.Kafka.Net.Processors
{
    internal class StreamTask : AbstractTask
    {
        private readonly IKafkaSupplier kafkaSupplier;
        private readonly IRecordCollector collector;
        private readonly IDictionary<TopicPartition, long> consumedOffsets;
        private readonly PartitionGrouper partitionGrouper;
        private readonly IList<IProcessor> processors = new List<IProcessor>();
        private readonly bool eosEnabled = false;
        private readonly long maxTaskIdleMs = 0;
        private readonly long maxBufferedSize = 100;
        private readonly bool followMetadata = false;

        private long idleStartTime;
        private IProducer<byte[], byte[]> producer;
        private bool transactionInFlight = false;
        private readonly string threadId;


        public StreamTask(string threadId, TaskId id, IEnumerable<TopicPartition> partitions, ProcessorTopology processorTopology, IConsumer<byte[], byte[]> consumer, IStreamConfig configuration, IKafkaSupplier kafkaSupplier, IProducer<byte[], byte[]> producer, IChangelogRegister changelogRegister)
            : base(id, partitions, processorTopology, consumer, configuration, changelogRegister)
        {
            this.threadId = threadId;
            this.kafkaSupplier = kafkaSupplier;
            consumedOffsets = new Dictionary<TopicPartition, long>();
            maxTaskIdleMs = configuration.MaxTaskIdleMs;
            maxBufferedSize = configuration.BufferedRecordsPerPartition;
            followMetadata = configuration.FollowMetadata;
            idleStartTime = -1;

            // eos enabled
            if (producer == null)
            {
                this.producer = CreateEOSProducer();
                InitializeTransaction();
                eosEnabled = true;
            }
            else
            {
                this.producer = producer;
            }

            collector = new RecordCollector(logPrefix, configuration, id);
            collector.Init(ref this.producer);

            Context = new ProcessorContext(this, configuration, stateMgr).UseRecordCollector(collector);
            Context.FollowMetadata = followMetadata;

            var partitionsQueue = new Dictionary<TopicPartition, RecordQueue>();

            foreach (var p in partitions)
            {
                var sourceProcessor = processorTopology.GetSourceProcessor(p.Topic);
                var sourceTimestampExtractor = sourceProcessor.Extractor ?? configuration.DefaultTimestampExtractor;
                var queue = new RecordQueue(
                    logPrefix,
                    $"record-queue-{p.Topic}-{id.Id}-{id.Partition}",
                    sourceTimestampExtractor,
                    p,
                    sourceProcessor);
                partitionsQueue.Add(p, queue);
                processors.Add(sourceProcessor);
            }

            partitionGrouper = new PartitionGrouper(partitionsQueue);
        }

        internal IConsumerGroupMetadata GroupMetadata { get; set; }

        #region Private

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

        private void Commit(bool startNewTransaction)
        {
            log.Debug($"{logPrefix}Comitting");

            if (state == TaskState.CLOSED)
                throw new IllegalStateException($"Illegal state {state} while committing active task {Id}");
            else if (state == TaskState.SUSPENDED || state == TaskState.CREATED
                || state == TaskState.RUNNING || state == TaskState.RESTORING)
            {
                FlushState();
                if (eosEnabled)
                {
                    producer.SendOffsetsToTransaction(GetPartitionsWithOffset(), GroupMetadata, configuration.TransactionTimeout);
                    producer.CommitTransaction(configuration.TransactionTimeout);
                    transactionInFlight = false;
                    if (startNewTransaction)
                    {
                        producer.BeginTransaction();
                        transactionInFlight = true;
                    }
                    consumedOffsets.Clear();
                }
                else
                {
                    try
                    {
                        consumer.Commit(GetPartitionsWithOffset());
                        consumedOffsets.Clear();
                    }
                    catch (TopicPartitionOffsetException e)
                    {
                        log.Info($"{logPrefix}Committing failed with a non-fatal error: {e.Message}, we can ignore this since commit may succeed still");
                    }
                    catch (KafkaException e)
                    {
                        // TODO : get info about offset committing
                        log.Error($"{logPrefix}Error during committing offset ......", e);
                    }
                }
                commitNeeded = false;
                commitRequested = false;
            }
            else
                throw new IllegalStateException($"Unknown state {state} while committing active task {Id}");
        }

        private IProducer<byte[], byte[]> CreateEOSProducer()
        {
            IProducer<byte[], byte[]> tmpProducer = null;
            var newConfig = configuration.Clone();
            log.Info($"${logPrefix}Creating producer client for task {Id}");
            newConfig.TransactionalId = $"{newConfig.ApplicationId}-{Id}";
            tmpProducer = kafkaSupplier.GetProducer(newConfig.ToProducerConfig(StreamThread.GetTaskProducerClientId(threadId, Id)));
            return tmpProducer;
        }

        private void InitializeTransaction()
        {
            bool initTransaction = false;
            while (!initTransaction)
            {
                try
                {
                    producer.InitTransactions(configuration.TransactionTimeout);
                    initTransaction = true;
                }
                catch (KafkaRetriableException)
                {
                    initTransaction = false;
                }
                catch (KafkaException e)
                {
                    throw new StreamsException($"{logPrefix}Failed to initialize task {Id} due to timeout ({configuration.TransactionTimeout}).", e);
                }
            }

        }

        #endregion

        #region Abstract

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
            else if (partitionGrouper.NumBuffered() > 0)
            {
                if (idleStartTime == -1)
                {
                    idleStartTime = now;
                }

                if (now - idleStartTime >= maxTaskIdleMs)
                {
                    return true;
                }
                else
                {
                    return false;
                }
            }
            else
            {
                idleStartTime = -1;
                return false;
            }
        }

        public override void Close()
        {
            log.Info($"{logPrefix}Closing");

            Suspend();

            if (state == TaskState.CREATED || state == TaskState.RESTORING || state == TaskState.RUNNING)
            {
                throw new IllegalStateException($"Illegal state {state} while closing active task {Id}");
            }
            else if (state == TaskState.CLOSED)
            {
                log.Info($"{logPrefix}Skip closing since state is {state}");
                return;
            }
            else if (state == TaskState.SUSPENDED)
            {
                foreach (var kp in processors)
                {
                    kp.Close();
                }

                partitionGrouper.Close();

                collector.Close();
                CloseStateManager();

                TransitTo(TaskState.CLOSED);
                log.Info($"{logPrefix}Closed");
                IsClosed = true;
            }
            else
            {
                throw new IllegalStateException($"Unknow state {state} while suspending active task {Id}");
            }
        }

        public override void Commit() => Commit(true);

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
                    log.Info($"{logPrefix}Restoration will start soon.");
                }
                else
                {
                    TransitTo(TaskState.RUNNING);
                }
            }
        }

        public override void InitializeTopology()
        {
            log.Debug($"{logPrefix}Initializing topology with theses source processors : {string.Join(", ", processors.Select(p => p.Name))}.");
            foreach (var p in processors)
            {
                p.Init(Context);
            }

            if (eosEnabled)
            {
                producer.BeginTransaction();
                transactionInFlight = true;
            }

            taskInitialized = true;
        }

        public override bool InitializeStateStores()
        {
            log.Debug($"{logPrefix}Initializing state stores.");
            RegisterStateStores();
            return false;
        }

        public override void Resume()
        {
            if (state == TaskState.CREATED ||
                state == TaskState.RESTORING ||
                state == TaskState.RUNNING)
            {
                log.Debug($"{logPrefix}Skip resuming since state is {state}");
            }
            else if (state == TaskState.SUSPENDED)
            {
                log.Debug($"{logPrefix}Resuming");
                InitializeStateStores();
                if (eosEnabled)
                {
                    if (producer != null)
                    {
                        throw new IllegalStateException("Task producer should be null.");
                    }

                    producer = CreateEOSProducer();
                    InitializeTransaction();
                    collector.Init(ref producer);
                }
            }
            else if (state == TaskState.CLOSED)
            {
                throw new IllegalStateException($"Illegal state {state} while resuming active task {Id}");
            }
            else
            {
                throw new IllegalStateException($"Unknow state {state} while resuming active task {Id}");
            }
        }

        public override void Suspend()
        {
            log.Debug($"{logPrefix}Suspending");

            if (state == TaskState.CREATED || state == TaskState.RESTORING)
            {
                log.Info($"{logPrefix}Suspended {(state == TaskState.CREATED ? "created" : "restoring")}");

                // TODO : remove when stream task refactoring is finished
                if (eosEnabled)
                {
                    if (transactionInFlight)
                    {
                        producer.AbortTransaction(configuration.TransactionTimeout);
                    }

                    collector.Close();
                    producer = null;
                }
                
                FlushState();
                CloseStateManager();

                TransitTo(TaskState.SUSPENDED);
            }
            else if (state == TaskState.RUNNING)
            {
                try
                {
                    Commit(false);
                }
                finally
                {
                    partitionGrouper.Clear();

                    if (eosEnabled)
                    {
                        if (transactionInFlight)
                        {
                            producer.AbortTransaction(configuration.TransactionTimeout);
                        }

                        collector.Close();
                        producer = null;
                    }
                    
                    FlushState();
                    CloseStateManager();
                }

                log.Info($"{logPrefix}Suspended running");
                TransitTo(TaskState.SUSPENDED);
            }
            else if (state == TaskState.SUSPENDED)
            {
                log.Info($"{logPrefix}Skip suspended since state is {state}");
                return;
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

        #endregion

        public bool Process()
        {
            var record = partitionGrouper.NextRecord;
            if (record == null)
            {
                return false;
            }
            else
            {
                Context.SetRecordMetaData(record.Record);

                var recordInfo = $"Topic:{record.Record.Topic}|Partition:{record.Record.Partition.Value}|Offset:{record.Record.Offset}|Timestamp:{record.Record.Message.Timestamp.UnixTimestampMs}";

                log.Debug($"{logPrefix}Start processing one record [{recordInfo}]");
                record.Processor.Process(record.Record);
                log.Debug($"{logPrefix}Completed processing one record [{recordInfo}]");

                consumedOffsets.AddOrUpdate(record.Record.TopicPartition, record.Record.Offset);
                commitNeeded = true;

                if (record.Queue.Size == maxBufferedSize)
                {
                    consumer.Resume(record.Record.TopicPartition.ToSingle());
                }

                return true;
            }
        }

        public void AddRecord(ConsumeResult<byte[], byte[]> record)
        {
            int newQueueSize = partitionGrouper.AddRecord(record.TopicPartition, record);

            if (newQueueSize > maxBufferedSize)
            {
                consumer.Pause(record.TopicPartition.ToSingle());
            }

            log.Debug($"{logPrefix}Added record into the buffered queue of partition {Partition}, new queue size is {newQueueSize}");
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
                return;
            }
            else if(state == TaskState.RESTORING)
            {
                TransitTo(TaskState.RUNNING);
                log.Info($"{logPrefix}Restored and ready to run");
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
    }   
}
