using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Stream;

namespace Streamiz.Kafka.Net.Processors
{
    internal class KStreamAsyncCallProcessor<K, V, K1, V1> : 
        AbstractProcessor<K, V>, IAsyncProcessor<K, V, K1, V1>
    {
        private readonly Func<ExternalRecord<K, V>, ExternalContext, Task<KeyValuePair<K1, V1>?>> asyncExternalCall;
        
        public KStreamAsyncCallProcessor(
            Func<ExternalRecord<K, V>, ExternalContext, Task<KeyValuePair<K1, V1>?>> asyncExternalCall,
            RetryPolicy retryPolicy)
        {
            this.asyncExternalCall = asyncExternalCall;
            Policy = retryPolicy;
        }

        public override void Process(K key, V value)
        {
            var context = new ExternalContext() {
                RetryNumber = 0
            };
            DateTime startProcessing = DateTime.Now;
            bool result = false, retry = true;
            Task<KeyValuePair<K1, V1>?> task = null;
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
                
                if (startProcessing.Add(TimeSpan.FromMilliseconds(Policy.TimeoutMs)) <
                    context.CurrentCallEpoch.FromMilliseconds())
                    throw new NotEnoughtTimeException("", context.CurrentCallEpoch - startProcessing.GetMilliseconds());
                
                task = ProcessAsync(key, value, Context.RecordContext.Headers, Context.Timestamp, context);

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
                    }
                    else
                    {
                        retry = false;
                        noneRetriableException = ae.InnerExceptions.First();
                    }
                }
            }

            if (result && task.IsCompletedSuccessfully)
            {
                LogProcessingKeyValueWithRetryNumber(key, value, context.RetryNumber, true);
                if(task.Result.HasValue)
                    Forward(task.Result.Value.Key, task.Result.Value.Value);
            }

            if (!retry && !result)
                throw new StreamsException(noneRetriableException);
        }

        private bool ContainsRetryableExceptions(AggregateException ae)
            => (from innerException in ae.InnerExceptions 
                from policyException in Policy.RetriableExceptions 
                where policyException.IsInstanceOfType(innerException) 
                select innerException).Any();

        protected void LogProcessingKeyValueWithRetryNumber(K key, V value, int retryNumber, bool result) => log.LogDebug(
            $"{logPrefix}Process<{typeof(K).Name},{typeof(V).Name}> message with key {key} and {value}" +
            $" with record metadata [topic:{Context.RecordContext.Topic}|" +
            $"partition:{Context.RecordContext.Partition}|offset:{Context.RecordContext.Offset}] [retry.number={retryNumber}, result={(result ? "Sucess" : "Failure")}]");
        
        public async Task<KeyValuePair<K1, V1>?> ProcessAsync(K key, V value, Headers headers, long timestamp, ExternalContext context)
        {
            var record = new ExternalRecord<K, V>(key, value, headers, timestamp);
            
            return await asyncExternalCall(
                record,
                context);
        }

        public RetryPolicy Policy { get; }
    }
}