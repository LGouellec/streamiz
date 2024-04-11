using Confluent.Kafka;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.SerDes;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using Microsoft.Extensions.Logging;
using Streamiz.Kafka.Net.Metrics;

namespace Streamiz.Kafka.Net.Kafka.Internal
{
    // TODO : Need to refactor, not necessary now to have one producer by thread if EOS is enable, see EOS_V2
    internal class RecordCollector : IRecordCollector
    {
        private sealed class RetryRecord
        {
            public byte[] Key { get; set; }
            public byte[] Value { get; set; }
            public Headers Headers { get; set; }
            public Timestamp Timestamp { get; set; }
            public Partition Partition { get; set; }
            public string Topic { get; set; }
        }

        private sealed class RetryRecordContext
        {
            private readonly Dictionary<string, RetryRecord> records;
            private readonly Queue<RetryRecord> queueRecords;
            private const string RETRY_HEADER_KEY = "streamiz-retry-guid";

            public RetryRecordContext()
            {
                records = new Dictionary<string, RetryRecord>();
                queueRecords = new Queue<RetryRecord>();
            }

            public void AddRecord(RetryRecord record)
            {
                if (record.Headers.TryGetLastBytes(RETRY_HEADER_KEY, out byte[] previousGuid))
                {
                    string oldGuidKey = Encoding.UTF8.GetString(previousGuid);
                    record.Headers.Remove(oldGuidKey);
                    records.Remove(oldGuidKey);
                }
                var newKey = Guid.NewGuid().ToString();
                record.Headers.AddOrUpdate(RETRY_HEADER_KEY, Encoding.UTF8.GetBytes(newKey));
                records.Add(newKey, record);
                queueRecords.Enqueue(record);
            }

            public void AckRecord(DeliveryReport<byte[], byte[]> report)
            {
                if (report.Headers != null &&
                    report.Headers.TryGetLastBytes(RETRY_HEADER_KEY, out var retryHeader))
                    records.Remove(Encoding.UTF8.GetString(retryHeader));
            }

            public RetryRecord NextRecord()
                => queueRecords.Count > 0 ? queueRecords.Dequeue() : null;

            public bool HasNext => queueRecords.Count > 0;

            public void Clear()
            {
                records.Clear();
                queueRecords.Clear();
            }
        }

        // IF EOS DISABLED, ONE PRODUCER BY TASK BUT ONE INSTANCE RECORD COLLECTOR BY TASK
        // WHEN CLOSING TASK, WE MUST DISPOSE PRODUCER WHEN NO MORE INSTANCE OF RECORD COLLECTOR IS PRESENT
        // IT'S A GARBAGE COLLECTOR LIKE
        private static IDictionary<string, int> instanceProducer = new Dictionary<string, int>();
        private readonly object _lock = new();

        private IProducer<byte[], byte[]> producer;
        private readonly IStreamConfig configuration;
        private readonly TaskId id;
        private readonly Sensor droppedRecordsSensor;
        private Exception exception = null;

        private readonly string logPrefix;
        private readonly ILogger log = Logger.GetLogger(typeof(RecordCollector));

        private readonly ConcurrentDictionary<TopicPartition, long> collectorsOffsets =
            new ConcurrentDictionary<TopicPartition, long>();

        private readonly RetryRecordContext retryRecordContext = new RetryRecordContext();

        public IDictionary<TopicPartition, long> CollectorOffsets => collectorsOffsets.ToDictionary();

        public RecordCollector(string logPrefix, IStreamConfig configuration, TaskId id, Sensor droppedRecordsSensor)
        {
            this.logPrefix = $"{logPrefix}";
            this.configuration = configuration;
            this.id = id;
            this.droppedRecordsSensor = droppedRecordsSensor;
        }

        public void Init(ref IProducer<byte[], byte[]> producer)
        {
            this.producer = producer;

            string producerName = producer.Name.Split('#')[0];
            lock (_lock)
            {
                if (instanceProducer.ContainsKey(producerName))
                    ++instanceProducer[producerName];
                else
                    instanceProducer.Add(producerName, 1);
            }
        }

        public void Close()
        {
            log.LogDebug("{LogPrefix}Closing producer", logPrefix);
            if (producer != null)
            {
                lock (_lock)
                {
                    string producerName = producer.Name.Split('#')[0];
                    if (instanceProducer.ContainsKey(producerName) && --instanceProducer[producerName] <= 0)
                    {
                        if (retryRecordContext.HasNext)
                            log.LogWarning(
                                "There are messages still pending to retry in the backpressure queue. These messages won't longer flush into the corresponding topic !");

                        retryRecordContext.Clear();
                        producer.Dispose();
                        producer = null;
                        CheckForException();
                    }
                }
            }
        }

        public void Flush()
        {
            log.LogDebug("{LogPrefix}Flushing producer", logPrefix);
            if (producer != null)
            {
                try
                {
                    while (retryRecordContext.HasNext)
                        ProduceRetryRecord();
                    
                    producer.Flush();
                    CheckForException();
                }
                catch (ObjectDisposedException)
                {
                    // has been disposed
                }
            }
        }

        public void Send<K, V>(string topic, K key, V value, Headers headers, long timestamp, ISerDes<K> keySerializer,
            ISerDes<V> valueSerializer)
            => SendInternal(topic, key, value, headers, null, timestamp, keySerializer, valueSerializer);

        public void Send<K, V>(string topic, K key, V value, Headers headers, int partition, long timestamp,
            ISerDes<K> keySerializer, ISerDes<V> valueSerializer)
            => SendInternal(topic, key, value, headers, partition, timestamp, keySerializer, valueSerializer);

        private bool IsFatalError(DeliveryReport<byte[], byte[]> report)
        {
            return report.Error.IsFatal ||
                   report.Error.Code == ErrorCode.TopicAuthorizationFailed ||
                   report.Error.Code == ErrorCode.GroupAuthorizationFailed ||
                   report.Error.Code == ErrorCode.ClusterAuthorizationFailed ||
                   report.Error.Code == ErrorCode.UnsupportedSaslMechanism ||
                   report.Error.Code == ErrorCode.SecurityDisabled ||
                   report.Error.Code == ErrorCode.SaslAuthenticationFailed ||
                   report.Error.Code == ErrorCode.TopicException ||
                   report.Error.Code == ErrorCode.Local_KeySerialization ||
                   report.Error.Code == ErrorCode.Local_ValueSerialization ||
                   report.Error.Code == ErrorCode.OffsetMetadataTooLarge;
        }

        private bool IsRecoverableError(DeliveryReport<byte[], byte[]> report)
        {
            return IsRecoverableError(report.Error);
        }

        private bool IsRecoverableError(Error error)
        {
            return error.Code == ErrorCode.TransactionCoordinatorFenced ||
                   error.Code == ErrorCode.UnknownProducerId ||
                   error.Code == ErrorCode.OutOfOrderSequenceNumber;
        }

        private void SendInternal<K, V>(string topic, K key, V value, Headers headers, int? partition, long timestamp,
            ISerDes<K> keySerializer, ISerDes<V> valueSerializer)
        {
            Debug.Assert(producer != null, nameof(producer) + " != null");
            var k = key != null
                ? keySerializer.Serialize(key, new SerializationContext(MessageComponentType.Key, topic, headers))
                : null;
            var v = value != null
                ? valueSerializer.Serialize(value, new SerializationContext(MessageComponentType.Value, topic, headers))
                : null;

            CheckForException();

            while (retryRecordContext.HasNext)
                ProduceRetryRecord();
            
            try
            {
                if (partition.HasValue)
                {
                    producer.Produce(
                        new TopicPartition(topic, partition.Value),
                        new Message<byte[], byte[]>
                        {
                            Key = k,
                            Value = v,
                            Headers = headers,
                            Timestamp = new Timestamp(timestamp, TimestampType.CreateTime)
                        },
                        HandleError);
                }
                else
                {
                    producer.Produce(
                        topic,
                        new Message<byte[], byte[]>
                        {
                            Key = k,
                            Value = v,
                            Headers = headers,
                            Timestamp = new Timestamp(timestamp, TimestampType.CreateTime)
                        },
                        HandleError);
                }
            }
            catch (ProduceException<byte[], byte[]> produceException)
            {
                ManageProduceException(produceException);
            }
        }

        private void HandleError(DeliveryReport<byte[], byte[]> report)
        {
            if (report.Error.IsError)
            {
                StringBuilder sb = new StringBuilder();
                sb.AppendLine(
                    $"{logPrefix}Error encountered sending record to topic {report.Topic} for task {id} due to:");
                sb.AppendLine($"{logPrefix}Error Code : {report.Error.Code.ToString()}");
                sb.AppendLine($"{logPrefix}Message : {report.Error.Reason}");

                if (IsFatalError(report))
                {
                    sb.AppendLine(
                        $"{logPrefix}Written offsets would not be recorded and no more records would be sent since this is a fatal error.");
                    log.LogError(sb.ToString());
                    lock (_lock)
                        exception = new StreamsException(sb.ToString());
                }
                else if (IsRecoverableError(report))
                {
                    sb.AppendLine(
                        $"{logPrefix}Written offsets would not be recorded and no more records would be sent since the producer is fenced, indicating the task may be migrated out");
                    log.LogError(sb.ToString());
                    lock (_lock)
                        exception = new TaskMigratedException(sb.ToString());
                }
                else
                {
                    var exceptionResponse = configuration.ProductionExceptionHandler(report);
                    if (exceptionResponse == ProductionExceptionHandlerResponse.FAIL)
                    {
                        sb.AppendLine(
                            $"{logPrefix}Exception handler choose to FAIL the processing, no more records would be sent.");
                        log.LogError(sb.ToString());
                        lock (_lock)
                            exception = new ProductionException(sb.ToString());
                    }
                    else if (exceptionResponse == ProductionExceptionHandlerResponse.RETRY)
                    {
                        sb.AppendLine(
                            $"{logPrefix}Exception handler choose to RETRY sending the message to the next iteration");
                        log.LogWarning(sb.ToString());
                        var retryRecord = new RetryRecord()
                        {
                            Key = report.Key,
                            Value = report.Value,
                            Headers = report.Headers,
                            Timestamp = report.Timestamp,
                            Partition = report.Partition,
                            Topic = report.Topic
                        };
                        retryRecordContext.AddRecord(retryRecord);
                    }
                    else
                    {
                        sb.AppendLine(
                            $"{logPrefix}Exception handler choose to CONTINUE processing in spite of this error but written offsets would not be recorded.");
                        log.LogError(sb.ToString());
                        droppedRecordsSensor.Record();
                    }
                }
            }
            else if (report.Status == PersistenceStatus.NotPersisted ||
                     report.Status == PersistenceStatus.PossiblyPersisted)
            {
                log.LogWarning(
                    "{logPrefix}Record not persisted or possibly persisted: (timestamp {Timestamp}) topic=[{Topic}] partition=[{Partition}] offset=[{Offset}]. May config Retry configuration, depends your use case",
                    logPrefix, report.Message.Timestamp.UnixTimestampMs, report.Topic, report.Partition, report.Offset);
            }
            else if (report.Status == PersistenceStatus.Persisted)
            {
                log.LogDebug(
                    "{LogPrefix}Record persisted: (timestamp {Timestamp}) topic=[{Topic}] partition=[{Partition}] offset=[{Offset}]",
                    logPrefix, report.Message.Timestamp.UnixTimestampMs, report.Topic, report.Partition, report.Offset);
                collectorsOffsets.AddOrUpdate(report.TopicPartition, report.Offset.Value);
                retryRecordContext.AckRecord(report);
            }
        }

        private void ProduceRetryRecord()
        {
            var retryRecord = retryRecordContext.NextRecord();
            if (retryRecord != null)
            {
                try
                {
                    producer.Produce(
                        new TopicPartition(retryRecord.Topic, retryRecord.Partition),
                        new Message<byte[], byte[]>
                        {
                            Key = retryRecord.Key,
                            Value = retryRecord.Value,
                            Headers = retryRecord.Headers,
                            Timestamp = retryRecord.Timestamp
                        }, HandleError);
                }
                catch (ProduceException<byte[], byte[]> produceException)
                {
                    ManageProduceException(produceException);
                }
            }
        }

        private void ManageProduceException(ProduceException<byte[], byte[]> produceException)
        {
            if (IsRecoverableError(produceException.Error))
            {
                throw new TaskMigratedException(
                    $"Producer got fenced trying to send a record [{logPrefix}] : {produceException.Message}");
            }

            StringBuilder sb = new StringBuilder();
            sb.AppendLine(
                $"{logPrefix}Error encountered sending record to topic {produceException.DeliveryResult.Topic} for task {id} due to:");
            sb.AppendLine($"{logPrefix}Error Code : {produceException.Error.Code.ToString()}");
            sb.AppendLine($"{logPrefix}Message : {produceException.Error.Reason}");

            var buildDeliveryReport =
                new DeliveryReport<byte[], byte[]>
                {
                    Message = produceException.DeliveryResult.Message,
                    Status = produceException.DeliveryResult.Status,
                    TopicPartitionOffsetError = new TopicPartitionOffsetError(
                        produceException.DeliveryResult.TopicPartitionOffset,
                        produceException.Error)
                };

            var exceptionHandlerResponse = configuration.ProductionExceptionHandler(buildDeliveryReport);

            if (exceptionHandlerResponse == ProductionExceptionHandlerResponse.FAIL)
            {
                sb.AppendLine(
                    $"{logPrefix}Exception handler choose to FAIL the processing, no more records would be sent.");
                log.LogError(sb.ToString());
                throw new StreamsException(
                    $"Error encountered trying to send record to topic {produceException.DeliveryResult.Topic} [{logPrefix}] : {produceException.Message}");
            }

            if (exceptionHandlerResponse == ProductionExceptionHandlerResponse.RETRY)
            {
                sb.AppendLine(
                    $"{logPrefix}Exception handler choose to RETRY sending the message to the next iteration");
                log.LogWarning(sb.ToString());
                var retryRecord = new RetryRecord
                {
                    Key = buildDeliveryReport.Key,
                    Value = buildDeliveryReport.Value,
                    Headers = buildDeliveryReport.Headers,
                    Timestamp = buildDeliveryReport.Timestamp,
                    Partition = buildDeliveryReport.Partition,
                    Topic = buildDeliveryReport.Topic
                };
                retryRecordContext.AddRecord(retryRecord);
            }
            else if (exceptionHandlerResponse == ProductionExceptionHandlerResponse.CONTINUE)
            {
                sb.AppendLine(
                    $"{logPrefix}Exception handler choose to CONTINUE processing in spite of this error but written offsets would not be recorded.");
                log.LogError(sb.ToString());
            }
        }

        private void CheckForException()
        {
            lock (_lock)
            {
                if (exception == null) return;
                var e = exception;
                exception = null;
                throw e;
            }
        }
    }
}