using Confluent.Kafka;
using log4net;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using System;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Processors.Internal
{
    internal class RecordQueue :
        IRecordQueue<ConsumeResult<byte[], byte[]>>,
        IComparable<RecordQueue>
    {
        // LOGGER + NAME
        private readonly string logPrefix;
        private readonly ILog log = Logger.GetLogger(typeof(RecordQueue));

        private readonly List<ConsumeResult<byte[], byte[]>> queue;
        private ConsumeResult<byte[], byte[]> currentRecord = null;
        private long partitionTime = -1;

        private readonly ITimestampExtractor timestampExtractor;
        private readonly TopicPartition topicPartition;
        private readonly ISourceProcessor sourceProcessor;

        public RecordQueue(
            string logPrefix,
            string nameQueue,
            ITimestampExtractor timestampExtractor,
            TopicPartition topicPartition,
            ISourceProcessor sourceProcessor)
        {
            this.logPrefix = $"{logPrefix}- recordQueue [{nameQueue}] ";
            queue = new List<ConsumeResult<byte[], byte[]>>();

            this.timestampExtractor = timestampExtractor;
            this.topicPartition = topicPartition;
            this.sourceProcessor = sourceProcessor;
        }

        public long HeadRecordTimestamp
            => currentRecord == null ? -1 : currentRecord.Message.Timestamp.UnixTimestampMs;

        public ISourceProcessor Processor => sourceProcessor;

        #region IRecordQueue Impl

        public int Size => queue.Count + (currentRecord == null ? 0 : 1);

        public bool IsEmpty => Size == 0;

        public int Queue(ConsumeResult<byte[], byte[]> item)
        {
            log.Debug($"{logPrefix}Adding new record in queue.");
            queue.Add(item);
            UpdateHeadRecord();
            log.Debug($"{logPrefix}Record added in queue. New size : {Size}");
            return Size;
        }

        public ConsumeResult<byte[], byte[]> Poll()
        {
            log.Debug($"{logPrefix}Polling record in queue.");
            var record = currentRecord;
            currentRecord = null;
            UpdateHeadRecord();
            log.Debug($"{logPrefix}{(record == null ? "No r" : "R")}ecord polled. ({(record != null ? $"Record info [Topic:{record.Topic}|Partition:{record.Partition}|Offset:{record.Offset}]" : "")})");
            return record;
        }

        public void Clear()
        {
            queue.Clear();
            currentRecord = null;
            partitionTime = -1;
            log.Debug($"{logPrefix} cleared !");
        }

        #endregion

        private void UpdateHeadRecord()
        {
            while (currentRecord == null && queue.Count > 0)
            {
                var record = queue[0];
                var recordObject = ToConsumeObject(record);
                long timestamp = -1;
                try
                {
                    timestamp = timestampExtractor.Extract(recordObject, partitionTime);
                }
                catch (Exception e)
                {
                    throw new StreamsException("Fatal error in Timestamp extractor callback", e);
                }

                log.Debug("");
                if (timestamp < 0)
                {
                    log.Warn($"Skipping record due to negative extracted timestamp. topic=[{record.Topic}] partition=[{record.Partition}] offset=[{record.Offset}] extractedTimestamp=[{timestamp}] extractor=[{timestampExtractor.GetType().Name}]");
                    continue;
                }

                currentRecord = record;
                currentRecord.Message.Timestamp = new Timestamp(timestamp, currentRecord.Message.Timestamp.Type);
                partitionTime = Math.Max(partitionTime, timestamp);
                queue.RemoveAt(0);
            }
        }

        private ConsumeResult<object, object> ToConsumeObject(ConsumeResult<byte[], byte[]> record)
        {
            return new ConsumeResult<object, object>
            {
                Topic = record.Topic,
                IsPartitionEOF = record.IsPartitionEOF,
                Offset = record.Offset,
                TopicPartitionOffset = record.TopicPartitionOffset,
                Message = new Message<object, object>
                {
                    Headers = record.Message.Headers,
                    Timestamp = record.Message.Timestamp,
                    Key = sourceProcessor.DeserializeKey(record.Topic, record.Message.Headers, record.Message.Key),
                    Value = sourceProcessor.DeserializeValue(record.Topic, record.Message.Headers, record.Message.Value)
                },
                Partition = record.Partition
            };
        }

        public int CompareTo(RecordQueue other)
            => HeadRecordTimestamp.CompareTo(other.HeadRecordTimestamp);
    }
}
