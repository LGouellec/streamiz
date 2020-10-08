using Confluent.Kafka;
using log4net;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.SerDes;
using System;
using System.Collections.Generic;
using System.Text;

namespace Streamiz.Kafka.Net.Kafka.Internal
{
    internal class RecordCollector : IRecordCollector
    {
        // IF EOS DISABLED, ONE PRODUCER BY TASK BUT ONE INSTANCE RECORD COLLECTOR BY TASK
        // WHEN CLOSING TASK, WE MUST DISPOSE PRODUCER WHEN NO MORE INSTANCE OF RECORD COLLECTOR IS PRESENT
        // IT'S A GARBAGE COLLECTOR LIKE
        private static IDictionary<string, int> instanceProducer = new Dictionary<string, int>();
        private static object _lock = new object();

        private IProducer<byte[], byte[]> producer;
        private readonly IStreamConfig configuration;
        private readonly TaskId id;

        private readonly string logPrefix;
        private readonly ILog log = Logger.GetLogger(typeof(RecordCollector));


        public RecordCollector(string logPrefix, IStreamConfig configuration, TaskId id) 
        {
            this.logPrefix = $"{logPrefix}";
            this.configuration = configuration;
            this.id = id;
        }

        public void Init(ref IProducer<byte[], byte[]> producer)
        {
            this.producer = producer;

            string producerName = producer.Name.Split("#")[0];
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
            log.Debug($"{logPrefix}Closing producer");
            if(producer != null)
            {
                lock (_lock)
                {
                    string producerName = producer.Name.Split("#")[0];
                    if (--instanceProducer[producerName] <= 0)
                    {
                        producer.Dispose();
                        producer = null;
                    }
                }
            }
        }

        public void Flush()
        {
            log.Debug($"{logPrefix}Flusing producer");
            if (producer != null)
            {
                try
                {
                    producer.Flush();
                }catch(ObjectDisposedException)
                {
                    // has been disposed
                }
            }
        }

        public void Send<K, V>(string topic, K key, V value, Headers headers, long timestamp, ISerDes<K> keySerializer, ISerDes<V> valueSerializer)
        {
            var k = key != null ? keySerializer.Serialize(key, new SerializationContext(MessageComponentType.Key, topic,headers)) : null;
            var v = value != null ? valueSerializer.Serialize(value, new SerializationContext(MessageComponentType.Value, topic, headers)) : null;

            producer?.Produce(
                topic,
                new Message<byte[], byte[]> { Key = k, Value = v },
                (report) =>
                {
                    if (report.Error.IsError)
                    {
                        StringBuilder sb = new StringBuilder();
                        sb.AppendLine($"{logPrefix}Error encountered sending record to topic {topic} for task {id} due to:");
                        sb.AppendLine($"{logPrefix}Error Code : {report.Error.Code.ToString()}"); 
                        sb.AppendLine($"{logPrefix}Message : {report.Error.Reason}");

                        if (IsFatalError(report))
                        {
                            sb.AppendLine($"{logPrefix}Written offsets would not be recorded and no more records would be sent since this is a fatal error.");
                            log.Error(sb.ToString());
                            throw new StreamsException(sb.ToString());
                        }
                        else if (IsRecoverableError(report))
                        {
                            sb.AppendLine($"{logPrefix}Written offsets would not be recorded and no more records would be sent since the producer is fenced, indicating the task may be migrated out");
                            // TODO : recoverable task
                            log.Error(sb.ToString());
                        }
                        else
                        {
                            if(configuration.ProductionExceptionHandler(report) == ExceptionHandlerResponse.FAIL)
                            {
                                sb.AppendLine($"{logPrefix}Exception handler choose to FAIL the processing, no more records would be sent.");
                                log.Error(sb.ToString());
                                throw new ProductionException(sb.ToString());
                            }
                            else
                            {
                                sb.AppendLine($"{logPrefix}Exception handler choose to CONTINUE processing in spite of this error but written offsets would not be recorded.");
                                log.Error(sb.ToString());
                            }
                        }
                    }
                    else if(report.Status == PersistenceStatus.NotPersisted || report.Status == PersistenceStatus.PossiblyPersisted)
                    {
                        log.Warn($"{logPrefix}Record not persisted or possibly persisted: (timestamp {report.Message.Timestamp.UnixTimestampMs}) topic=[{topic}] partition=[{report.Partition}] offset=[{report.Offset}]. May config Retry configuration, depends your use case.");
                    }
                    else if (report.Status == PersistenceStatus.Persisted)
                    {
                        log.Debug($"{logPrefix}Record persisted: (timestamp {report.Message.Timestamp.UnixTimestampMs}) topic=[{topic}] partition=[{report.Partition}] offset=[{report.Offset}]");
                    }
                });
        }

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
            return report.Error.Code == ErrorCode.TransactionCoordinatorFenced ||
                     report.Error.Code == ErrorCode.UnknownProducerId ||
                     report.Error.Code == ErrorCode.OutOfOrderSequenceNumber;
        }
    }
}