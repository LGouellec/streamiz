using System;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Processors.Public;

namespace Streamiz.Kafka.Net.Processors.Internal
{
    internal static class ProcessingListenerHelper
    {
        private static readonly ILogger log = Logger.GetLogger(typeof(ProcessingListenerHelper));

        internal static void Process(
            IProcessingListener listener,
            ProcessorContext context,
            IProcessor processor,
            ConsumeResult<byte[], byte[]> record)
        {
            if (listener == null)
            {
                processor.Process(record);
                return;
            }

            Run(
                () => listener.OnProcessStart(context, record),
                () => processor.Process(record),
                listener.OnProcessEnd);
        }

        internal static void Punctuate(
            IProcessingListener listener,
            ProcessorContext context,
            TaskScheduled taskScheduled,
            PunctuationType punctuationType,
            long timestamp)
        {
            if (listener == null)
            {
                taskScheduled.Execute(timestamp);
                return;
            }

            Run(
                () => listener.OnPunctuateStart(context, taskScheduled.Processor?.Name, punctuationType, timestamp),
                () => taskScheduled.Execute(timestamp),
                listener.OnPunctuateEnd);
        }

        private static void Run(Func<object> start, Action body, Action<object, Exception> end)
        {
            object state = null;
            try
            {
                state = start();
            }
            catch (Exception e)
            {
                log.LogWarning(e, "Processing listener start callback failed, the exception is ignored");
            }

            try
            {
                body();
            }
            catch (Exception e)
            {
                End(end, state, e);
                throw;
            }

            End(end, state, null);
        }

        private static void End(Action<object, Exception> end, object state, Exception exception)
        {
            try
            {
                end(state, exception);
            }
            catch (Exception e)
            {
                log.LogWarning(e, "Processing listener end callback failed, the exception is ignored");
            }
        }
    }
}
