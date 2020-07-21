using Confluent.Kafka;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Kafka;
using Streamiz.Kafka.Net.Kafka.Internal;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.Stream.Internal;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Processors
{
    internal class StreamTask : AbstractTask
    {
        private readonly IKafkaSupplier kafkaSupplier;
        private readonly IRecordCollector collector;
        private readonly IDictionary<string, IProcessor> processors = new Dictionary<string, IProcessor>();
        private readonly RecordQueue<ConsumeResult<byte[], byte[]>> queue;
        private readonly IDictionary<TopicPartition, long> consumedOffsets;
        private readonly bool eosEnabled = false;

        private IProducer<byte[], byte[]> producer;
        private bool transactionInFlight = false;
        private readonly string threadId;

        public StreamTask(string threadId, TaskId id, IEnumerable<TopicPartition> partitions, ProcessorTopology processorTopology, IConsumer<byte[], byte[]> consumer, IStreamConfig configuration, IKafkaSupplier kafkaSupplier, IProducer<byte[], byte[]> producer)
            : base(id, partitions, processorTopology, consumer, configuration)
        {
            this.threadId = threadId;
            this.kafkaSupplier = kafkaSupplier;
            consumedOffsets = new Dictionary<TopicPartition, long>();

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

            collector = new RecordCollector(logPrefix);
            collector.Init(ref this.producer);

            // TODO FIX
            //var sourceTimestampExtractor = (processorTopology.GetSourceProcessor(id.Topic) as ISourceProcessor).Extractor;
            ITimestampExtractor sourceTimestampExtractor = null;
            Context = new ProcessorContext(configuration, stateMgr).UseRecordCollector(collector);

            foreach (var p in partitions)
            {
                processors.Add(p.Topic, processorTopology.GetSourceProcessor(p.Topic));
            }


            // REFACTOR RECORD QUEUE WITH JOIN
            queue = new RecordQueue<ConsumeResult<byte[], byte[]>>(
                100,
                logPrefix,
                $"record-queue-{id.Id}-{id.Partition}",
                sourceTimestampExtractor == null ? configuration.DefaultTimestampExtractor : sourceTimestampExtractor);
        }

        internal IConsumerGroupMetadata GroupMetadata { get; set; }

        #region Private

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

        public override bool CanProcess => queue.Size > 0;

        public override void Close()
        {
            IsClosed = true;
            log.Info($"{logPrefix}Closing");

            Suspend();

            foreach (var kp in processors)
            {
                kp.Value.Close();
            }

            collector.Close();
            CloseStateManager();
            log.Info($"{logPrefix}Closed");
        }

        public override void Commit() => Commit(true);

        public override IStateStore GetStore(string name)
        {
            return Context.GetStateStore(name);
        }

        public override void InitializeTopology()
        {
            log.Debug($"{logPrefix}Initializing topology with theses source processors : {string.Join(", ", processors.Keys)}.");
            foreach (var p in processors)
            {
                p.Value.Init(Context);
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
            log.Debug($"{logPrefix}Resuming");
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

        public override void Suspend()
        {
            log.Debug($"{logPrefix}Suspending");

            try
            {
                Commit(false);
            }
            finally
            {
                if (eosEnabled)
                {
                    if (transactionInFlight)
                    {
                        producer.AbortTransaction(configuration.TransactionTimeout);
                    }

                    collector.Close();
                    producer = null;
                }
            }
        }

        protected override void FlushState()
        {
            base.FlushState();
            collector?.Flush();
        }

        #endregion

        public bool Process()
        {
            if (queue.Size > 0)
            {
                var record = queue.GetNextRecord();
                if (record != null)
                {
                    // TODO : gérer timestampextractor
                    Context.SetRecordMetaData(record);

                    var recordInfo = $"Topic:{record.Topic}|Partition:{record.Partition.Value}|Offset:{record.Offset}|Timestamp:{record.Message.Timestamp.UnixTimestampMs}";
                    log.Debug($"{logPrefix}Start processing one record [{recordInfo}]");
                    if (processors.ContainsKey(record.Topic))
                    {
                        processors[record.Topic].Process(record.Message.Key, record.Message.Value);
                    }
                    else
                    {
                        log.Error($"{logPrefix}Impossible to process record {recordInfo}. Processor for topic {record.Topic} doesn't exist !");
                        throw new StreamsException($"{logPrefix}Impossible to process record {recordInfo}. Processor for topic {record.Topic} doesn't exist !");
                    }
                    log.Debug($"{logPrefix}Completed processing one record [{recordInfo}]");

                    queue.Commit();

                    consumedOffsets.AddOrUpdate(record.TopicPartition, record.Offset);
                    commitNeeded = true;

                    return true;
                }
                return false;
            }
            return false;
        }

        public void AddRecords(IEnumerable<ConsumeResult<byte[], byte[]>> records)
        {
            foreach (var r in records)
            {
                queue.AddRecord(r);
            }

            // TODO : NO PAUSE FOR MOMENT
            //if (queue.MaxSize <= queue.Size)
            //    consumer.Pause(new List<TopicPartition> { partition });

            int newQueueSize = queue.Size;
            log.Debug($"{logPrefix}Added records into the buffered queue of partition {Partition}, new queue size is {newQueueSize}");

            //// if after adding these records, its partition queue's buffered size has been
            //// increased beyond the threshold, we can then pause the consumption for this partition
            //if (newQueueSize > maxBufferedSize)
            //{
            //    consumer.pause(singleton(partition));
            //}
        }
    }
}
