using System;
using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Crosscutting;

namespace Streamiz.Kafka.Net.Kafka.Internal
{
    /// <summary>
    /// Manages the producers within a Kafka Streams application.
    /// If EOS is enabled, it is responsible to init and begin transactions if necessary.
    /// It also tracks the transaction status, ie, if a transaction is in-fight.
    /// For non-EOS, the user should not call transaction related methods.
    /// </summary>
    internal class StreamsProducer : IDisposable
    {
        private readonly ILogger log = Logger.GetLogger(typeof(StreamsProducer));
        private readonly string _logPrefix;
        private IProducer<byte[], byte[]> _producer;
        private bool _transactionInitialized = false;
        private readonly IStreamConfig _configuration;
        private readonly IKafkaSupplier _kafkaSupplier;
        private readonly ProducerConfig _producerConfig;
        private readonly IAdminClient _adminClient;

        public bool EosEnabled { get; }
        public bool TransactionInFlight { get; private set; }

        // for testing
        internal IProducer<byte[], byte[]> Producer => _producer;

        public string Name => _producer.Name;
        
        private IDictionary<string, (int, DateTime)> _cachePartitionsForTopics =
            new Dictionary<string, (int, DateTime)>();
        
        public StreamsProducer(
            IStreamConfig config,
            String threadId,
            Guid processId,
            IKafkaSupplier kafkaSupplier,
            String logPrefix)
        {
            _logPrefix = logPrefix;
            _configuration = config;
            _kafkaSupplier = kafkaSupplier;

            _producerConfig = config.ToProducerConfig(StreamThread.GetThreadProducerClientId(threadId))
                .Wrap(threadId);
            
            _adminClient = kafkaSupplier.GetAdmin(config.ToAdminConfig(threadId));
            
            switch (config.Guarantee)
            {
                case ProcessingGuarantee.AT_LEAST_ONCE:
                    break;
                case ProcessingGuarantee.EXACTLY_ONCE:
                    _producerConfig.TransactionalId = $"{config.ApplicationId}-{processId}";
                    break;
                default:
                    throw new StreamsException($"Guarantee {config.Guarantee} is not supported yet");
            }

            _producer = _kafkaSupplier.GetProducer(_producerConfig);
            EosEnabled = config.Guarantee == ProcessingGuarantee.EXACTLY_ONCE;
        }

        internal static bool IsRecoverable(Error error)
        {
            return error.Code == ErrorCode.InvalidProducerIdMapping ||
                   error.Code == ErrorCode.ProducerFenced ||
                   error.Code == ErrorCode.InvalidProducerEpoch ||
                   error.Code == ErrorCode.UnknownProducerId;
        }
        
        public void InitTransaction()
        {
            if (!EosEnabled)
                throw new IllegalStateException("Exactly-once is not enabled");

            if (!_transactionInitialized)
            {
                try
                {
                    _producer.InitTransactions(_configuration.TransactionTimeout);
                    _transactionInitialized = true;
                }
                catch (KafkaRetriableException)
                {
                    log.LogWarning(
                        "Timeout exception caught trying to initialize transactions. " +
                        "The broker is either slow or in bad state (like not having enough replicas) in " +
                        "responding to the request, or the connection to broker was interrupted sending " +
                        "the request or receiving the response. " +
                        "Will retry initializing the task in the next loop. " +
                        "Consider overwriting 'transaction.timeout' to a larger value to avoid timeout errors"
                    );
                    throw;
                }
                catch (KafkaException kafkaException)
                {
                    throw new StreamsException("Error encountered trying to initialize transactions", kafkaException);
                }
            }
        }

        public void ResetProducer()
        {
            Close();
            _producer = _kafkaSupplier.GetProducer(_producerConfig);
        }

        private void StartTransaction()
        {
            if (EosEnabled && !TransactionInFlight)
            {
                try
                {
                    _producer.BeginTransaction();
                    TransactionInFlight = true;
                }
                catch (KafkaException kafkaException)
                {
                    if (IsRecoverable(kafkaException.Error))
                        throw new TaskMigratedException(
                            $"Producer got fenced trying to begin a new transaction : {kafkaException.Message}");

                    throw new StreamsException("Error encountered trying to begin a new transaction", kafkaException);
                }
            }
        }

        public void Produce(TopicPartition topicPartition, Message<byte[], byte[]> message,
            Action<DeliveryReport<byte[], byte[]>> deliveryReport)
        {
            StartTransaction();
            _producer.Produce(topicPartition, message, deliveryReport);
        }
        
        public void Produce(string topic, Message<byte[], byte[]> message,
            Action<DeliveryReport<byte[], byte[]>> deliveryReport)
        {
            StartTransaction();
            _producer.Produce(topic, message, deliveryReport);
        }
        
        public void CommitTransaction(IEnumerable<TopicPartitionOffset> offsets, IConsumerGroupMetadata groupMetadata)
        {
            if (!EosEnabled)
                throw new IllegalStateException("Exactly-once is not enabled");
            
            StartTransaction();
            try
            {
                _producer.SendOffsetsToTransaction(offsets, groupMetadata, _configuration.TransactionTimeout);
                _producer.CommitTransaction();
                TransactionInFlight = false;
            }
            catch (KafkaException kafkaException)
            {
                if (IsRecoverable(kafkaException.Error))
                    throw new TaskMigratedException(
                        $"Producer got fenced trying to commit a transaction : {kafkaException.Message}");
                
                throw new StreamsException("Error encountered trying to commit a transaction", kafkaException);
            }
        }

        public void AbortTransaction()
        {
            if (!EosEnabled)
                throw new IllegalStateException("Exactly-once is not enabled");

            if (TransactionInFlight)
            {
                try
                {
                    _producer.AbortTransaction();
                }
                catch (KafkaException kafkaException)
                {
                    if (IsRecoverable(kafkaException.Error))
                        log.LogDebug(
                            $"Encountered {kafkaException.Message} while aborting the transaction; this is expected and hence swallowed");
                    else
                        throw new StreamsException("Error encounter trying to abort a transaction", kafkaException);

                }
                finally
                {
                    TransactionInFlight = false;
                }
            }
        }

        public void Flush()
        {
            _producer.Flush();
        }

        private void Close()
        {
            TransactionInFlight = false;
            _transactionInitialized = false;
        }

        public void Dispose()
        {
            Close();
            _producer.Dispose();
            _adminClient?.Dispose();
        }
        
        public int PartitionsFor(string topic)
        {
            var adminConfig = _configuration.ToAdminConfig("");
            var refreshInterval = adminConfig.TopicMetadataRefreshIntervalMs ?? 5 * 60 * 1000;

            if (_cachePartitionsForTopics.ContainsKey(topic) &&
                _cachePartitionsForTopics[topic].Item2 + TimeSpan.FromMilliseconds(refreshInterval) > DateTime.Now)
                return _cachePartitionsForTopics[topic].Item1;
            
            var metadata = _adminClient.GetMetadata(topic, TimeSpan.FromSeconds(5));
            var partitionCount = metadata.Topics.FirstOrDefault(t => t.Topic.Equals(topic))!.Partitions.Count;
            _cachePartitionsForTopics.AddOrUpdate(topic, (partitionCount, DateTime.Now));
            return partitionCount;
        }
    }
}