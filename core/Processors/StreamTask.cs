using Confluent.Kafka;
using Streamiz.Kafka.Net.Kafka;
using Streamiz.Kafka.Net.Kafka.Internal;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.Stream.Internal;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Processors
{
    internal class StreamTask : AbstractTask
    {
        private readonly IProducer<byte[], byte[]> producer;
        private readonly IRecordCollector collector;
        private readonly IProcessor processor;
        private readonly RecordQueue<ConsumeResult<byte[], byte[]>> queue;

        public StreamTask(string threadId, TaskId id, TopicPartition partition, ProcessorTopology processorTopology, IConsumer<byte[], byte[]> consumer, IStreamConfig configuration, IProducer<byte[], byte[]> producer)
            : base(id, partition, processorTopology, consumer, configuration)
        {
            this.producer = producer;
            this.collector = new RecordCollector(logPrefix);
            collector.Init(producer);

            var sourceTimestampExtractor = (processorTopology.GetSourceProcessor(id.Topic) as ISourceProcessor).Extractor;
            Context = new ProcessorContext(configuration, stateMgr).UseRecordCollector(collector);
            processor = processorTopology.GetSourceProcessor(partition.Topic);
            queue = new RecordQueue<ConsumeResult<byte[], byte[]>>(
                100,
                logPrefix,
                $"record-queue-{id.Topic}-{id.Partition}",
                sourceTimestampExtractor == null ? configuration.DefaultTimestampExtractor : sourceTimestampExtractor);
        }

        #region Private

        #endregion

        #region Abstract

        public override bool CanProcess => queue.Size > 0;

        public override void Close()
        {
            log.Info($"{logPrefix}Closing");
            FlushState();
            processor.Close();
            collector.Close();
            CloseStateManager();
            log.Info($"{logPrefix}Closed");
        }

        public override void Commit()
        {
            log.Debug($"{logPrefix}Comitting");

            FlushState();
            // TODO: producer eos
            
            try
            {
                consumer.Commit();
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
            commitNeeded = false;
        }

        public override IStateStore GetStore(string name)
        {
            return Context.GetStateStore(name);
        }

        public override void InitializeTopology()
        {
            log.Debug($"{logPrefix}Initializing topology with processor source : {processor}.");
            processor.Init(Context);
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
            // NOTHING FOR MOMENT
        }

        public override void Suspend()
        {
            // NOTHING FOR MOMENT
        }

        protected override void FlushState()
        {
            base.FlushState();
            this.collector.Flush();
        }
        
        #endregion

        public bool Process()
        {
            if (queue.Size > 0)
            {
                var record = queue.GetNextRecord();
                if (record != null)
                {
                    this.Context.SetRecordMetaData(record);

                    var recordInfo = $"Topic:{record.Topic}|Partition:{record.Partition.Value}|Offset:{record.Offset}|Timestamp:{record.Message.Timestamp.UnixTimestampMs}";
                    log.Debug($"{logPrefix}Start processing one record [{recordInfo}]");
                    processor.Process(record.Message.Key, record.Message.Value);
                    log.Debug($"{logPrefix}Completed processing one record [{recordInfo}]");

                    queue.Commit();
                    commitNeeded = true;
                    return true;
                }
                return false;
            }
            return false;
        }

        public void AddRecords(TopicPartition partition, IEnumerable<ConsumeResult<byte[], byte[]>> records)
        {
            foreach (var r in records)
                queue.AddRecord(r);

            // TODO : NO PAUSE FOR MOMENT
            //if (queue.MaxSize <= queue.Size)
            //    consumer.Pause(new List<TopicPartition> { partition });

            int newQueueSize = queue.Size;

            if (log.IsDebugEnabled)
            {
                log.Debug($"{logPrefix}Added records into the buffered queue of partition {partition}, new queue size is {newQueueSize}");
            }
            
            //// if after adding these records, its partition queue's buffered size has been
            //// increased beyond the threshold, we can then pause the consumption for this partition
            //if (newQueueSize > maxBufferedSize)
            //{
            //    consumer.pause(singleton(partition));
            //}
        }
    }
}
