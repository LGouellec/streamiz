using System;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;

namespace Streamiz.Kafka.Net.Processors
{
    internal class StatestoreRestoreManager
    {
        #region Args
        
        internal interface IStatestoreRestoreArgs
        {
            internal void TriggerStreamEvent(KafkaStream stream);
        }

        internal class StatestoreRestoreStartArgs : IStatestoreRestoreArgs
        {
            private readonly TopicPartition _topicPartition;
            private readonly string _storeName;
            private readonly long _startOffset;
            private readonly long _endOffset;

            internal StatestoreRestoreStartArgs(
                TopicPartition topicPartition,
                String storeName,
                long startOffset, long endOffset)
            {
                _topicPartition = topicPartition;
                _storeName = storeName;
                _startOffset = startOffset;
                _endOffset = endOffset;
            }

            public override string ToString()
                =>
                    $"StatestoreRestoreStartArgs(TopicPartition:{_topicPartition}|StoreName:{_storeName}|StartOffset:{_startOffset}|EndOffset:{_endOffset})";

            void IStatestoreRestoreArgs.TriggerStreamEvent(KafkaStream stream)
            {
                stream.TriggerOnRestoreStartEvent(_topicPartition, _storeName, _startOffset, _endOffset);
            }
        }
        
        internal class StatestoreRestoreBatchArgs : IStatestoreRestoreArgs
        {
            private readonly TopicPartition _topicPartition;
            private readonly string _storeName;
            private readonly long _batchEndOffset;
            private readonly long _numRestored;

            internal StatestoreRestoreBatchArgs(
                TopicPartition topicPartition,
                String storeName,
                long batchEndOffset,
                long numRestored)
            {
                _topicPartition = topicPartition;
                _storeName = storeName;
                _batchEndOffset = batchEndOffset;
                _numRestored = numRestored;
            }

            public override string ToString()
                =>
                    $"StatestoreRestoreBatchArgs(TopicPartition:{_topicPartition}|StoreName:{_storeName}|BatchEndOffset:{_batchEndOffset}|NumRestored:{_numRestored})";
            
            
            void IStatestoreRestoreArgs.TriggerStreamEvent(KafkaStream stream)
            {
                stream.TriggerOnRestoreBatchEvent(_topicPartition, _storeName, _batchEndOffset, _numRestored);
            }
        }
        
        internal class StatestoreRestoreEndArgs : IStatestoreRestoreArgs
        {

            private readonly TopicPartition _topicPartition;
            private readonly string _storeName;
            private readonly long _totalRestored;

            internal StatestoreRestoreEndArgs(
                TopicPartition topicPartition,
                String storeName,
                long totalRestored)
            {
                _topicPartition = topicPartition;
                _storeName = storeName;
                _totalRestored = totalRestored;
            }

            public override string ToString()
                =>
                    $"StatestoreRestoreEndArgs(TopicPartition:{_topicPartition}|StoreName:{_storeName}|TotalRestored:{_totalRestored})";

            
            void IStatestoreRestoreArgs.TriggerStreamEvent(KafkaStream stream)
            {
                stream.TriggerOnRestoreEndEvent(_topicPartition, _storeName, _totalRestored);
            }
        }
        
        #endregion
        
        private readonly ILogger log = Logger.GetLogger(typeof(StreamStateManager));
        private readonly KafkaStream stream;
        private static readonly object threadStatesLock = new();

        public StatestoreRestoreManager(KafkaStream stream)
        {
            this.stream = stream;
        }

        internal void StatestoreRestoreChange(IStatestoreRestoreArgs args)
        {
            lock (threadStatesLock)
            {
                if (stream != null)
                {
                    try
                    {
                        log.LogDebug($"Calling state store restore listener with args {args}");
                        args.TriggerStreamEvent(stream);
                        log.LogInformation($"State store restore listener called with args {args}");
                    }
                    catch (Exception e)
                    {
                        throw new StreamsException($"State restore listener failed on restoration with args {args}", e);
                    }
                }
            }
        }
    }
}