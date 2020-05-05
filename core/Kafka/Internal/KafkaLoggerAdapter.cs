using Confluent.Kafka;
using Streamiz.Kafka.Net.Crosscutting;
using log4net;
using System;
using System.Threading;

namespace Streamiz.Kafka.Net.Kafka.Internal
{
    internal class KafkaLoggerAdapter
    {
        private readonly ILog log = null;

        public KafkaLoggerAdapter(IStreamConfig configuration)
            : this(configuration, Logger.GetLogger(typeof(KafkaLoggerAdapter)))
        {}

        public KafkaLoggerAdapter(IStreamConfig configuration, ILog log)
        {
            this.log = log;
        }

        #region Log Consumer

        internal void LogConsume(IConsumer<byte[], byte[]> consumer, LogMessage message)
        {
            string logPrefix = Thread.CurrentThread.Name != null ? $"stream-thread[{Thread.CurrentThread.Name}] " : "";
            log.Debug($"{logPrefix}Log consumer {GetName(consumer)} - {message.Message}");
        }

        internal void ErrorConsume(IConsumer<byte[], byte[]> consumer, Error error)
        {
            string logPrefix = Thread.CurrentThread.Name != null ? $"stream-thread[{Thread.CurrentThread.Name}] " : "";
            log.Error($"{logPrefix}Error consumer {GetName(consumer)} - {error.Reason}");
        }

        #endregion

        #region Log Producer

        internal void LogProduce(IProducer<byte[], byte[]> producer, LogMessage message)
        {
            string logPrefix = Thread.CurrentThread.Name != null ? $"stream-thread[{Thread.CurrentThread.Name}] " : "";
            log.Debug($"{logPrefix}Log producer {GetName(producer)} - {message.Message}");
        }

        internal void ErrorProduce(IProducer<byte[], byte[]> producer, Error error)
        {
            string logPrefix = Thread.CurrentThread.Name != null ? $"stream-thread[{Thread.CurrentThread.Name}] " : "";
            log.Error($"{logPrefix}Error producer {GetName(producer)} - {error.Reason}");
        }

        #endregion

        #region Log Admin

        internal void ErrorAdmin(IAdminClient admin, Error error)
        {
            string logPrefix = Thread.CurrentThread.Name != null ? $"stream-thread[{Thread.CurrentThread.Name}] " : "";
            log.Error($"{logPrefix}Error admin {GetName(admin)} - {error.Reason}");
        }

        internal void LogAdmin(IAdminClient admin, LogMessage message)
        {
            string logPrefix = Thread.CurrentThread.Name != null ? $"stream-thread[{Thread.CurrentThread.Name}] " : "";
            log.Debug($"{logPrefix}Log admin {GetName(admin)} - {message.Message}");
        }

        #endregion

        private string GetName(IClient client)
        {
            // FOR FIX
            string name = "";
            try
            {
                name = client.Name;
            }
            catch (NullReferenceException)
            {
                name = "Unknown";
            }
            return name;
        }
    }
}