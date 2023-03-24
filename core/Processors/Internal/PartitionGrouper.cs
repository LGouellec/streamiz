using System.Collections.Generic;
using Confluent.Kafka;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;

namespace Streamiz.Kafka.Net.Processors.Internal
{
    internal class RecordInfo
    {
        public ISourceProcessor Processor { get; }
        public ConsumeResult<byte[], byte[]> Record { get; }
        public RecordQueue Queue { get; }

        public RecordInfo(
            ISourceProcessor processor,
            ConsumeResult<byte[], byte[]> record,
            RecordQueue queue)
        {
            Processor = processor;
            Record = record;
            Queue = queue;
        }
    }

    internal class PartitionGrouper
    {
        private readonly Dictionary<TopicPartition, RecordQueue> partitionsQueue;
        private readonly PriorityQueue<RecordQueue> priorityQueue;

        private long streamTime;
        private int totalBuffered;
        private bool allBuffered;

        public PartitionGrouper(Dictionary<TopicPartition, RecordQueue> partitionsQueue)
        {
            this.partitionsQueue = partitionsQueue;
            priorityQueue = new PriorityQueue<RecordQueue>(partitionsQueue.Count);
            totalBuffered = 0;
            streamTime = -1;
            allBuffered = false;
        }

        internal int AddRecord(TopicPartition topicPartition, ConsumeResult<byte[], byte[]> record)
        {
            var queue = partitionsQueue.Get(topicPartition);
            if (queue == null)
            {
                throw new IllegalStateException($"Partition {topicPartition} was not found");
            }

            int oldSize = queue.Size;
            int newSize = queue.Queue(record);

            if (oldSize == 0 && newSize > 0)
            {
                priorityQueue.Enqueue(queue);
                if (priorityQueue.Count == partitionsQueue.Count)
                {
                    allBuffered = true;
                }
            }

            totalBuffered += (newSize - oldSize);
            return newSize;
        }

        internal RecordInfo NextRecord
        {
            get
            {
                RecordInfo record = null;
                var queue = priorityQueue.Dequeue();
                if (queue != null)
                {
                    var r = queue.Poll();
                    if (r != null)
                    {
                        --totalBuffered;
                        if (queue.IsEmpty)
                        {
                            allBuffered = false;
                        }
                        else
                        {
                            priorityQueue.Enqueue(queue);
                        }

                        if (r.Message.Timestamp.UnixTimestampMs > streamTime)
                        {
                            streamTime = r.Message.Timestamp.UnixTimestampMs;
                        }

                        record = new RecordInfo(queue.Processor, r, queue);
                    }
                }
                return record;
            }
        }
        internal long StreamTime => streamTime;
        
        internal void Clear()
        {
            foreach (var kp in partitionsQueue)
            {
                kp.Value.Clear();
            }
        }

        internal void Close()
        {
            Clear();
            partitionsQueue.Clear();
        }

        internal bool AllPartitionsBuffered => allBuffered;

        internal int NumBuffered() => totalBuffered;

        internal int NumBuffered(TopicPartition topicPartition)
        {
            var queue = partitionsQueue.Get(topicPartition);

            if (queue == null)
            {
                throw new IllegalStateException($"Partition {topicPartition} was not found");
            }

            return queue.Size;
        }
    }
}
