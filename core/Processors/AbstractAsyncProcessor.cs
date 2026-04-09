using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.ObjectModel;
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
        AbstractProcessor<K, V>, IAsyncProcessor<K, V, K1, V1>, IDisposable
    {
        private Sensor retrySensor;
        private readonly ParallelProcessingConfig parallelProcessingConfig;

        // Parallel processing state
        private readonly bool useParallelProcessing;
        private readonly SemaphoreSlim concurrencySemaphore;
        private readonly ConcurrentQueue<Task> activeTasks;
        private readonly CancellationTokenSource closeCts;

        protected AbstractAsyncProcessor(
            RetryPolicy policy,
            ParallelProcessingConfig parallelProcessingConfig = null)
        {
            Policy = policy;
            this.parallelProcessingConfig = parallelProcessingConfig;
            this.useParallelProcessing = parallelProcessingConfig != null &&
                                         parallelProcessingConfig.Mode != ParallelProcessingMode.SEQUENTIAL;

            if (useParallelProcessing)
            {
                concurrencySemaphore = new SemaphoreSlim(parallelProcessingConfig.MaxConcurrency);
                activeTasks = new ConcurrentQueue<Task>();
                closeCts = new CancellationTokenSource();
            }
        }

        public RetryPolicy Policy { get; }

        public override void Process(K key, V value)
        {
            if (useParallelProcessing)
            {
                ProcessWithParallelism(key, value);
            }
            else
            {
                ProcessSequential(key, value);
            }
        }

        private void ProcessWithParallelism(K key, V value)
        {
            // Clean up completed tasks first
            CleanupCompletedTasks();

            // Wait for available concurrency slot
            concurrencySemaphore.Wait(closeCts.Token);

            // Start async processing in background
            var task = Task.Run(async () =>
            {
                try
                {
                    await ProcessAsyncWithRetry(key, value);
                }
                finally
                {
                    // Release concurrency slot
                    concurrencySemaphore.Release();
                }
            }, closeCts.Token);

            // Track the task
            activeTasks.Enqueue(task);
        }

        private async Task ProcessAsyncWithRetry(K key, V value)
        {
            var context = new ExternalContext() {
                RetryNumber = 0
            };
            DateTime startProcessing = DateTime.Now;
            bool result = false, retry = true;
            Task<IEnumerable<KeyValuePair<K1, V1>>> task = null;
            Exception noneRetriableException = null;
            context.FirstCallEpoch = DateTime.Now.GetMilliseconds();

            while (retry && !closeCts.Token.IsCancellationRequested)
            {
                if (context.RetryNumber > 0)
                    await Task.Delay(TimeSpan.FromMilliseconds(Policy.RetryBackOffMs), closeCts.Token);

                context.CurrentCallEpoch = DateTime.Now.GetMilliseconds();

                if (context.RetryNumber == Policy.NumberOfRetry)
                {
                    log.LogError($"{logPrefix}Number of retry exceeded for key {key}");
                    return; // Skip this record
                }

                ++context.RetryNumber;

                if (startProcessing.Add(TimeSpan.FromMilliseconds(Policy.TimeoutMs)).GetMilliseconds() <
                    context.CurrentCallEpoch)
                {
                    log.LogError($"{logPrefix}Timeout exceeded for key {key}");
                    return; // Skip this record
                }

                task = ProcessAsync(key, value, Context.RecordContext.Headers, Context.Timestamp, context);
                retrySensor?.Record(context.RetryNumber);

                try
                {
                    await task;
                    result = true;
                    retry = false;
                }
                catch (Exception ex)
                {
                    LogProcessingKeyValueWithRetryNumber(key, value, context.RetryNumber, false);
                    if (ex is AggregateException ae && ContainsRetryableExceptions(ae))
                    {
                        context.LastExceptions = ae.InnerExceptions;
                        log.LogDebug($"{logPrefix}Retryable exception during processing: {ae.InnerExceptions.First().Message}");
                    }
                    else if (Policy.RetriableExceptions.Any(t => t.IsInstanceOfType(ex)))
                    {
                        context.LastExceptions = new ReadOnlyCollection<Exception>(new List<Exception> { ex });
                        log.LogDebug($"{logPrefix}Retryable exception during processing: {ex.Message}");
                    }
                    else
                    {
                        retry = false;
                        noneRetriableException = ex;
                    }
                }
            }

            if (result && task.IsCompleted)
            {
                LogProcessingKeyValueWithRetryNumber(key, value, context.RetryNumber, true);
                if (task.Result != null && task.Result.Any())
                {
                    // Forward results - note: this may have ordering implications
                    foreach (var kv in task.Result)
                    {
                        lock (Context) // Protect Context access from multiple threads
                        {
                            var cloneHeader = Context.RecordContext.Headers.Clone();
                            Forward(kv.Key, kv.Value);
                            Context.SetHeaders(cloneHeader);
                        }
                    }
                }
            }

            if (!retry && !result && noneRetriableException != null)
            {
                log.LogError(noneRetriableException, $"{logPrefix}Non-retriable exception for key {key}");
                // Don't throw - would crash the background task
            }
        }

        private void CleanupCompletedTasks()
        {
            // Remove completed tasks from the queue
            while (activeTasks.TryPeek(out var task) && task.IsCompleted)
            {
                activeTasks.TryDequeue(out _);
            }
        }

        private void ProcessSequential(K key, V value)
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

            if (useParallelProcessing)
            {
                log.LogInformation($"{logPrefix}Initialized with parallel processing: " +
                                  $"Mode={parallelProcessingConfig.Mode}, " +
                                  $"MaxConcurrency={parallelProcessingConfig.MaxConcurrency}");
            }

            base.Init(context);
        }

        public override void Close()
        {
            if (useParallelProcessing)
            {
                log.LogInformation($"{logPrefix}Closing parallel async processor, waiting for {activeTasks.Count} active tasks");

                // Signal shutdown
                closeCts?.Cancel();

                // Wait for active tasks with timeout
                var timeout = parallelProcessingConfig?.MaxWaitForCompletion ?? TimeSpan.FromSeconds(30);
                var waitTask = Task.WhenAll(activeTasks.ToArray());

                if (!waitTask.Wait(timeout))
                {
                    log.LogWarning($"{logPrefix}Some async tasks did not complete within timeout");
                }

                concurrencySemaphore?.Dispose();
                closeCts?.Dispose();
            }

            base.Close();
        }

        public void Dispose()
        {
            Close();
        }

        private bool ContainsRetryableExceptions(AggregateException ae)
            => (from innerException in ae.InnerExceptions
                from policyException in Policy.RetriableExceptions
                where policyException.IsInstanceOfType(innerException)
                select innerException).Any();

        private void LogProcessingKeyValueWithRetryNumber(K key, V value, int retryNumber, bool result)
        {
            if (log.IsEnabled(LogLevel.Debug))
            {
                log.LogDebug(
                    $"{logPrefix}Process<{typeof(K).Name},{typeof(V).Name}> message with key {key} and {value}" +
                    $" with record metadata [topic:{Context.RecordContext.Topic}|" +
                    $"partition:{Context.RecordContext.Partition}|offset:{Context.RecordContext.Offset}] [retry.number={retryNumber}, result={(result ? "Success" : "Failure")}]");
            }
        }

        public abstract Task<IEnumerable<KeyValuePair<K1, V1>>> ProcessAsync(K key, V value, Headers headers, long timestamp,
            ExternalContext context);
    }
}
