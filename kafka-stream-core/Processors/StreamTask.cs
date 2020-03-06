using System;
using System.Collections.Generic;
using System.Text;
using Confluent.Kafka;
using kafka_stream_core.Kafka;
using kafka_stream_core.Kafka.Internal;
using kafka_stream_core.Processors.Internal;
using kafka_stream_core.Stream.Internal;

namespace kafka_stream_core.Processors
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
            this.collector = new RecordCollectorImpl(threadId);
            collector.Init(producer);

            Context = new ProcessorContext(configuration).UseRecordCollector(collector);

            processor = processorTopology.GetSourceProcessor(partition.Topic);
            queue = new RecordQueue<ConsumeResult<byte[], byte[]>>(100);
        }

        #region Abstract

        public override bool CanProcess => queue.Size > 0;

        public override void Close()
        {
            processor.Close();
        }

        public override void Commit()
        {
            // TODO :
            consumer.Commit();
        }

        public override StateStore GetStore(string name)
        {
            return null;
        }

        public override void InitializeTopology()
        {
            processor.Init(Context);
            taskInitialized = true;
        }

        public override void Resume()
        {
            // NOTHING FOR MOMENT
        }

        public override void Suspend()
        {
            // NOTHING FOR MOMENT
        }

        #endregion

        public bool Process()
        {
            if (queue.Size > 0)
            {
                var record = queue.GetNextRecord();
                if (record != null)
                {
                    processor.Process(record.Key, record.Value);
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

            if (queue.MaxSize <= queue.Size)
                consumer.Pause(new List<TopicPartition> { partition });

            //final int newQueueSize = partitionGroup.addRawRecords(partition, records);

                //if (log.isTraceEnabled())
                //{
                //    log.trace("Added records into the buffered queue of partition {}, new queue size is {}", partition, newQueueSize);
                //}

                //// if after adding these records, its partition queue's buffered size has been
                //// increased beyond the threshold, we can then pause the consumption for this partition
                //if (newQueueSize > maxBufferedSize)
                //{
                //    consumer.pause(singleton(partition));
                //}
        }
    }
}
