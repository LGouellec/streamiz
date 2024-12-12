﻿using Confluent.Kafka;
using Streamiz.Kafka.Net.Mock.Sync;
using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace Streamiz.Kafka.Net.Tests.Helpers
{
    internal class ProducerSyncExceptionOptions
    {
        public IDictionary<string, Func<Exception>> ExceptionsToThrow;
        public bool IsFatal { get; set; } = false;
        public bool IsRecoverable { get; set; } = false;
        public bool IsProductionException { get; set; } = false;

        public int NumberOfError { get; set; } = 1;

        public List<string> WhiteTopics { get; set; } = new();
    }

    internal class ProducerSyncExceptionSupplier : SyncKafkaSupplier
    {
        private KafkaProducerException producerException = null;
        private readonly ProducerSyncExceptionOptions options = null;

        public ProducerSyncExceptionSupplier(ProducerSyncExceptionOptions options)
        {
            this.options = options;
        }

        public override IProducer<byte[], byte[]> GetProducer(ProducerConfig config)
        {
            if (producerException == null)
            {
                var p = base.GetProducer(config) as SyncProducer;
                producerException = new KafkaProducerException(p, options);
            }

            return producerException;
        }
    }

    internal class KafkaProducerException : IProducer<byte[], byte[]>
    {
        private SyncProducer innerProducer;
        private ProducerSyncExceptionOptions options;
        private bool handleError = true;

        private KafkaProducerException(SyncProducer syncProducer)
        {
            innerProducer = syncProducer;
        }
        
        private void CheckThrowException(string currentMethod)
        {
            if (options.ExceptionsToThrow != null &&
                options.ExceptionsToThrow.ContainsKey(currentMethod))
            {
                throw options.ExceptionsToThrow[currentMethod].Invoke();
            }
        }

        public KafkaProducerException(SyncProducer syncProducer, ProducerSyncExceptionOptions options)
            : this(syncProducer)
        {
            this.options = options;
        }

        public Handle Handle => throw new NotImplementedException();

        public string Name => "";

        public void AbortTransaction(TimeSpan timeout)
        {
            CheckThrowException(MethodBase.GetCurrentMethod().Name);
        }

        public void SetSaslCredentials(string username, string password)
        {
            CheckThrowException(MethodBase.GetCurrentMethod().Name);
        }

        public int AddBrokers(string brokers)
        {
            CheckThrowException(MethodBase.GetCurrentMethod().Name);
            return 0;
        }

        public void BeginTransaction()
        {
            CheckThrowException(MethodBase.GetCurrentMethod().Name);
        }

        public void CommitTransaction(TimeSpan timeout)
        {
            CheckThrowException(MethodBase.GetCurrentMethod().Name);
        }

        public void Dispose()
        {
            CheckThrowException(MethodBase.GetCurrentMethod().Name);
        }

        public int Flush(TimeSpan timeout)
        {
            CheckThrowException(MethodBase.GetCurrentMethod().Name);
            return 0;
        }

        public void Flush(CancellationToken cancellationToken = default)
        {
            CheckThrowException(MethodBase.GetCurrentMethod().Name);
        }

        public void InitTransactions(TimeSpan timeout)
        {
            CheckThrowException(MethodBase.GetCurrentMethod().Name);
        }

        public int Poll(TimeSpan timeout)
        {
            CheckThrowException(MethodBase.GetCurrentMethod().Name);
            return 0;
        }

        private void HandleError(DeliveryReport<byte[], byte[]> initReport,
            Action<DeliveryReport<byte[], byte[]>> deliveryHandler)
        {
            CheckThrowException(MethodBase.GetCurrentMethod().Name);
            --options.NumberOfError;
            handleError = options.NumberOfError > 0;
            
            if (options.IsProductionException)
            {
                var result = new DeliveryResult<byte[], byte[]>
                {
                    Message = initReport.Message,
                    Partition = initReport.Partition,
                    Topic = initReport.Topic
                };

                if (options.IsRecoverable)
                {
                    throw new ProduceException<byte[], byte[]>(new Error(ErrorCode.ProducerFenced,
                        "TransactionCoordinatorFenced", false), result);
                }
                else
                {
                    throw new ProduceException<byte[], byte[]>(
                        new Error(ErrorCode.Local_InvalidArg, "Invalid arg", false),
                        result);
                }
            }
            else
            {
                if (options.IsFatal)
                {
                    initReport.Error = new Error(ErrorCode.TopicAuthorizationFailed, "TopicAuthorizationFailed",
                        true);
                    deliveryHandler(initReport);
                }
                else if (options.IsRecoverable)
                {
                    initReport.Error = new Error(ErrorCode.ProducerFenced,
                        "TransactionCoordinatorFenced",
                        false);
                    deliveryHandler(initReport);
                }
                else
                {
                    initReport.Error = new Error(ErrorCode.Local_InvalidArg, "Invalid arg", false);
                    deliveryHandler(initReport);
                }
            }
        }

        public void Produce(string topic, Message<byte[], byte[]> message,
            Action<DeliveryReport<byte[], byte[]>> deliveryHandler = null)
        {
            CheckThrowException(MethodBase.GetCurrentMethod().Name);
            if (options.WhiteTopics.Contains(topic) || !handleError)
                innerProducer.Produce(topic, message, deliveryHandler);
            else
            {
                var report = new DeliveryReport<byte[], byte[]>
                {
                    Message = message,
                    Topic = topic
                };
                HandleError(report, deliveryHandler);
            }
        }

        public void Produce(TopicPartition topicPartition, Message<byte[], byte[]> message,
            Action<DeliveryReport<byte[], byte[]>> deliveryHandler = null)
        {
            CheckThrowException(MethodBase.GetCurrentMethod().Name);
            if (options.WhiteTopics.Contains(topicPartition.Topic) || !handleError)
                innerProducer.Produce(topicPartition, message, deliveryHandler);
            else
            {
                var report = new DeliveryReport<byte[], byte[]>
                {
                    Message = message,
                    Topic = topicPartition.Topic,
                    Partition = topicPartition.Partition
                };
                HandleError(report, deliveryHandler);
            }
        }

        public async Task<DeliveryResult<byte[], byte[]>> ProduceAsync(string topic,
            Message<byte[], byte[]> message, CancellationToken cancellationToken = default)
        {
            CheckThrowException(MethodBase.GetCurrentMethod().Name);
            if (options.WhiteTopics.Contains(topic) || !handleError)
                return await innerProducer.ProduceAsync(topic, message, cancellationToken);
            else
                throw new NotImplementedException();
        }

        public async Task<DeliveryResult<byte[], byte[]>> ProduceAsync(TopicPartition topicPartition,
            Message<byte[], byte[]> message, CancellationToken cancellationToken = default)
        {
            CheckThrowException(MethodBase.GetCurrentMethod().Name);
            if (options.WhiteTopics.Contains(topicPartition.Topic) || !handleError)
                return await innerProducer.ProduceAsync(topicPartition, message, cancellationToken);
            else
                throw new NotImplementedException();
        }

        public void SendOffsetsToTransaction(IEnumerable<TopicPartitionOffset> offsets,
            IConsumerGroupMetadata groupMetadata, TimeSpan timeout)
        {
            CheckThrowException(MethodBase.GetCurrentMethod().Name);
        }

        public void CommitTransaction()
        {
            CheckThrowException(MethodBase.GetCurrentMethod().Name);
        }

        public void AbortTransaction()
        {
            CheckThrowException(MethodBase.GetCurrentMethod().Name);
        }
    }
}