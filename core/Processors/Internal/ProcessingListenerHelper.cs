using System;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Streamiz.Kafka.Net.Processors.Public;

namespace Streamiz.Kafka.Net.Processors.Internal
{
    /// <summary>
    /// Invokes an <see cref="IProcessingListener"/> around an action. Exceptions thrown by the listener are logged and ignored,
    /// exceptions thrown by the action are passed to the listener and rethrown unchanged.
    /// </summary>
    internal static class ProcessingListenerHelper
    {
        internal static void Process(
            IProcessingListener listener,
            ProcessorContext context,
            ConsumeResult<byte[], byte[]> record,
            Action<ConsumeResult<byte[], byte[]>> process,
            ILogger log,
            string logPrefix)
        {
            object state;
            try
            {
                state = listener.OnProcessStart(context, record);
            }
            catch (Exception e)
            {
                LogListenerException(log, logPrefix, nameof(IProcessingListener.OnProcessStart), e);
                process(record);
                return;
            }

            try
            {
                process(record);
            }
            catch (Exception e)
            {
                OnProcessEnd(listener, state, e, log, logPrefix);
                throw;
            }

            OnProcessEnd(listener, state, null, log, logPrefix);
        }

        internal static void Punctuate(
            IProcessingListener listener,
            ProcessorContext context,
            string processorName,
            PunctuationType punctuationType,
            long timestamp,
            Action punctuate,
            ILogger log,
            string logPrefix)
        {
            object state;
            try
            {
                state = listener.OnPunctuateStart(context, processorName, punctuationType, timestamp);
            }
            catch (Exception e)
            {
                LogListenerException(log, logPrefix, nameof(IProcessingListener.OnPunctuateStart), e);
                punctuate();
                return;
            }

            try
            {
                punctuate();
            }
            catch (Exception e)
            {
                OnPunctuateEnd(listener, state, e, log, logPrefix);
                throw;
            }

            OnPunctuateEnd(listener, state, null, log, logPrefix);
        }

        private static void OnProcessEnd(IProcessingListener listener, object state, Exception exception, ILogger log, string logPrefix)
        {
            try
            {
                listener.OnProcessEnd(state, exception);
            }
            catch (Exception e)
            {
                LogListenerException(log, logPrefix, nameof(IProcessingListener.OnProcessEnd), e);
            }
        }

        private static void OnPunctuateEnd(IProcessingListener listener, object state, Exception exception, ILogger log, string logPrefix)
        {
            try
            {
                listener.OnPunctuateEnd(state, exception);
            }
            catch (Exception e)
            {
                LogListenerException(log, logPrefix, nameof(IProcessingListener.OnPunctuateEnd), e);
            }
        }

        private static void LogListenerException(ILogger log, string logPrefix, string callback, Exception e)
            => log.LogWarning(e, "{LogPrefix}Processing listener {Callback} threw an exception, it is ignored", logPrefix, callback);
    }
}
