using Confluent.Kafka;
using kafka_stream_core.Crosscutting;
using log4net;
using System.Threading;

namespace kafka_stream_core.Kafka.Internal
{
    internal class KafkaLoggerAdapter
    {
        private readonly ILog log = null;

        public KafkaLoggerAdapter(IStreamConfig configuration)
        {
            log = Logger.GetLogger(typeof(KafkaLoggerAdapter));
        }
        
        #region Log Consumer

        internal void LogConsume(IConsumer<byte[], byte[]> consumer, LogMessage message)
        {
            string logPrefix = Thread.CurrentThread.Name != null ? $"stream-thread[{Thread.CurrentThread.Name}] " : "";
            log.Debug($"{logPrefix}Log consumer {consumer.Name} - {message.Message}");
        }

        internal void ErrorConsume(IConsumer<byte[], byte[]> consumer, Error error)
        {
            string logPrefix = Thread.CurrentThread.Name != null ? $"stream-thread[{Thread.CurrentThread.Name}] " : "";
            log.Error($"{logPrefix}Error consumer {consumer.Name} - {error.Reason}");
        }

        #endregion

        #region Log Producer

        internal void LogProduce(IProducer<byte[], byte[]> producer, LogMessage message)
        {
            string logPrefix = Thread.CurrentThread.Name != null ? $"stream-thread[{Thread.CurrentThread.Name}] " : "";
            log.Debug($"{logPrefix}Log producer {producer.Name} - {message.Message}");
        }

        internal void ErrorProduce(IProducer<byte[], byte[]> producer, Error error)
        {
            string logPrefix = Thread.CurrentThread.Name != null ? $"stream-thread[{Thread.CurrentThread.Name}] " : "";
            log.Error($"{logPrefix}Error producer {producer.Name} - {error.Reason}");
        }

        #endregion

        #region Log Admin

        internal void ErrorAdmin(IAdminClient admin, Error error)
        {
            string logPrefix = Thread.CurrentThread.Name != null ? $"stream-thread[{Thread.CurrentThread.Name}] " : "";
            log.Error($"{logPrefix}Error admin {admin.Name} - {error.Reason}");
        }

        internal void LogAdmin(IAdminClient admin, LogMessage message)
        {
            string logPrefix = Thread.CurrentThread.Name != null ? $"stream-thread[{Thread.CurrentThread.Name}] " : "";
            log.Debug($"{logPrefix}Log admin {admin.Name} - {message.Message}");
        }

        #endregion
    }
}