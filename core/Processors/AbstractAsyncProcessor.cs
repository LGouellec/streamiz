using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Metrics.Internal;
using Streamiz.Kafka.Net.Stream;

namespace Streamiz.Kafka.Net.Processors
{
    internal abstract class AbstractAsyncProcessor<K, V, K1, V1> : 
        AbstractProcessor<K, V>, IAsyncProcessor<K, V, K1, V1>
    {
        private Sensor retrySensor;

        protected AbstractAsyncProcessor(RetryPolicy policy)
        {
            Policy = policy;
        }
        
        public RetryPolicy Policy { get; }

        public override void Process(K key, V value)
        {
            var context = new ExternalContext() {
                RetryNumber = 0
            };
            DateTime startProcessing = DateTime.Now;
            bool result = false, retry = true;
            Task<IEnumerable<KeyValuePair<K1, V1>>> task = null;
            Exception noneRetriableException = null;
            context.FirstCallEpoch = DateTime.Now.GetMilliseconds();
            
            while (retry)
            {
                if(context.RetryNumber > 0)
                    Thread.Sleep(TimeSpan.FromMilliseconds(Policy.RetryBackOffMs));

                context.CurrentCallEpoch = DateTime.Now.GetMilliseconds();
                
                if (context.RetryNumber == Policy.NumberOfRetry)
                    throw new NoneRetryableException($"Number of retry exceeded", context.RetryNumber,  context.CurrentCallEpoch - startProcessing.GetMilliseconds(), null); 
                
                ++context.RetryNumber;

                if (startProcessing.Add(TimeSpan.FromMilliseconds(Policy.TimeoutMs)).GetMilliseconds() <
                    context.CurrentCallEpoch)
                    throw new NotEnoughtTimeException("", context.CurrentCallEpoch - startProcessing.GetMilliseconds());
                
                task = ProcessAsync(key, value, Context.RecordContext.Headers, Context.Timestamp, context);
                retrySensor.Record(context.RetryNumber);
                
                try
                {
                    task.Wait();
                    result = true;
                    retry = false;
                }
                catch (AggregateException ae)
                {
                    LogProcessingKeyValueWithRetryNumber(key, value, context.RetryNumber, false);
                    if (ContainsRetryableExceptions(ae))
                    {
                        context.LastExceptions = ae.InnerExceptions;
                        log.LogDebug($"{logPrefix}An retryable exception is thrown during the processing : {ae.InnerExceptions.First().Message}");
                        log.LogInformation($"{logPrefix}An retryable exception is thrown during the processing : {ae.InnerExceptions.First().Message}");
                    }
                    else
                    {
                        retry = false;
                        noneRetriableException = ae.InnerExceptions.First();
                    }
                }
            }

            if (result && task.IsCompleted)
            {
                LogProcessingKeyValueWithRetryNumber(key, value, context.RetryNumber, true);
                if (task.Result != null && task.Result.Any())
                {
                    foreach (var kv in task.Result)
                    {
                        var cloneHeader = Context.RecordContext.Headers.Clone();
                        Forward(kv.Key, kv.Value);
                        Context.SetHeaders(cloneHeader);
                    }
                }
            }

            if (!retry && !result)
                throw new StreamsException(noneRetriableException);
        }
        
        public override void Init(ProcessorContext context)
        {
            retrySensor = ProcessorNodeMetrics.RetrySensor( 
                Thread.CurrentThread.Name,
                context.Id,
                Name,
                context.Metrics);
            
            base.Init(context);
        }

        private bool ContainsRetryableExceptions(AggregateException ae)
            => (from innerException in ae.InnerExceptions 
                from policyException in Policy.RetriableExceptions 
                where policyException.IsInstanceOfType(innerException) 
                select innerException).Any();

        private void LogProcessingKeyValueWithRetryNumber(K key, V value, int retryNumber, bool result) => log.LogDebug(
            $"{logPrefix}Process<{typeof(K).Name},{typeof(V).Name}> message with key {key} and {value}" +
            $" with record metadata [topic:{Context.RecordContext.Topic}|" +
            $"partition:{Context.RecordContext.Partition}|offset:{Context.RecordContext.Offset}] [retry.number={retryNumber}, result={(result ? "Success" : "Failure")}]");
        
        public abstract Task<IEnumerable<KeyValuePair<K1, V1>>> ProcessAsync(K key, V value, Headers headers, long timestamp,
            ExternalContext context);
        
    }
}