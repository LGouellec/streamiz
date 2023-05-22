using System;
using System.Collections.Generic;
using System.Linq;

namespace Streamiz.Kafka.Net.Stream
{
    /// <summary>
    /// Builder class to build a new retry policy.
    /// </summary>
    public class RetryPolicyBuilder
    {
        private static readonly long DEFAULT_RETRY_BACKOFF_MS = 100;
        private static readonly int DEFAULT_NUMBER_OF_RETRY = 5;
        private static readonly EndRetryBehavior DEFAULT_RETRY_BEHAVIOR = EndRetryBehavior.SKIP;
        private static readonly List<Type> DEFAULT_RETRIABLE_EXCEPTIONS = new();
        private static long DEFAULT_MEMORY_BUFFER_SIZE = 100;
        private static long DEFAULT_TIMEOUT_MS = -1; // MaxPollIntervalMs / 3

        private long retryBackOffMs = DEFAULT_RETRY_BACKOFF_MS;
        private int numberOfRetry = DEFAULT_NUMBER_OF_RETRY;
        private EndRetryBehavior endRetryBehavior = DEFAULT_RETRY_BEHAVIOR;
        private readonly List<Type> retriableExceptions = DEFAULT_RETRIABLE_EXCEPTIONS.ToList();
        private long memoryBufferSize = DEFAULT_MEMORY_BUFFER_SIZE;
        private long timeoutMs = DEFAULT_TIMEOUT_MS;

        internal RetryPolicyBuilder()
        {
            
        }
        
        /// <summary>
        /// Specify the time to wait before attempting a new retry a failed request.
        /// Default: 100 ms
        /// </summary>
        /// <param name="retryBackOffMs">Retry backoff in ms</param>
        /// <returns></returns>
        public RetryPolicyBuilder RetryBackOffMs(long retryBackOffMs)
        {
            this.retryBackOffMs = retryBackOffMs;
            return this;
        }

        /// <summary>
        /// Specify the maximum number of retry for one record.
        /// Default: 5
        /// </summary>
        /// <param name="numberOfRetry">Maximum retry of one record</param>
        /// <returns></returns>
        public RetryPolicyBuilder NumberOfRetry(int numberOfRetry)
        {
            this.numberOfRetry = numberOfRetry;
            return this;
        }

        /// <summary>
        /// Specify the behavior when the retry is attempted.
        /// Default: Skipping the message
        /// </summary>
        /// <param name="endRetryBehavior"></param>
        /// <returns></returns>
        public RetryPolicyBuilder RetryBehavior(EndRetryBehavior endRetryBehavior)
        {
            this.endRetryBehavior = endRetryBehavior;
            return this;
        }

        /// <summary>
        /// Specify the retriable exception.
        /// Default: None
        /// If an exception is thrown during the asynchronous processing, the processor will checked if this exception is retriable or not.
        /// If not, the processor will execute the <see cref="IStreamConfig.InnerExceptionHandler"/>.
        /// </summary>
        /// <typeparam name="T">Type of retriable exception. You can chain wil multiple exceptions</typeparam>
        /// <returns></returns>
        public RetryPolicyBuilder RetriableException<T>()
            where T : Exception
        {
            retriableExceptions.Add(typeof(T));
            return this;
        }

        /// <summary>
        /// Specify the buffering size. Only use with <see cref="EndRetryBehavior.BUFFERED"/>.
        /// Default: 100
        /// </summary>
        /// <param name="memoryBufferSize">Maximum size of the buffer</param>
        /// <returns></returns>
        public RetryPolicyBuilder MemoryBufferSize(long memoryBufferSize)
        {
            this.memoryBufferSize = memoryBufferSize;
            return this;
        }

        /// <summary>
        /// Specify the the total time that a record will be delayed prior to executed the asynchronous processing.
        /// Be carefull to not set this timeout greater than <see cref="IStreamConfig.MaxPollIntervalMs"/>.
        /// Default: -1, that means <see cref="IStreamConfig.MaxPollIntervalMs"/> / 3.
        /// </summary>
        /// <param name="timeoutMs">Timeout for one asynchronous operation</param>
        /// <returns></returns>
        public RetryPolicyBuilder TimeoutMs(long timeoutMs)
        {
            this.timeoutMs = timeoutMs;
            return this;
        }
        
        /// <summary>
        /// Build the retry policy
        /// </summary>
        /// <returns></returns>
        public RetryPolicy Build()
        {
            return new RetryPolicy(
                retryBackOffMs,
                numberOfRetry, 
                endRetryBehavior,
                retriableExceptions,
                memoryBufferSize,
                timeoutMs);
        }
    }

    /// <summary>
    /// Retry behavior when it's exceed
    /// </summary>
    public enum EndRetryBehavior
    {
        /// <summary>
        /// Skip the message
        /// </summary>
        SKIP,
        /// <summary>
        /// Fail the dedicated external thread
        /// </summary>
        FAIL,
        /// <summary>
        /// Buffered the record and try out later
        /// </summary>
        BUFFERED
    }
    
    /// <summary>
    /// Retry policy configuration
    /// </summary>
    public class RetryPolicy
    {
        /// <summary>
        /// Timeout in milliseconds for the asynchronous operation
        /// </summary>
        public long TimeoutMs { get; internal set; }
        
        /// <summary>
        /// Time between two retry
        /// </summary>
        public long RetryBackOffMs { get; }
        
        /// <summary>
        /// Maximum number of retry
        /// </summary>
        public int NumberOfRetry { get; }
        
        /// <summary>
        /// Retry behavior when it's exceed
        /// </summary>
        public EndRetryBehavior EndRetryBehavior { get; }
        
        /// <summary>
        /// List of retriable exceptions
        /// </summary>
        public List<Type> RetriableExceptions { get; }
        
        /// <summary>
        /// Maximum size of buffering
        /// </summary>
        public long MemoryBufferSize { get; }

        internal RetryPolicy(long retryBackOffMs,
            int numberOfRetry,
            EndRetryBehavior endRetryBehavior,
            List<Type> retriableExceptions,
            long memoryBufferSize,
            long timeoutMs)
        {
            RetryBackOffMs = retryBackOffMs;
            NumberOfRetry = numberOfRetry;
            EndRetryBehavior = endRetryBehavior;
            RetriableExceptions = retriableExceptions;
            MemoryBufferSize = memoryBufferSize;
            TimeoutMs = timeoutMs;
        }

        /// <summary>
        /// Create a new builder
        /// </summary>
        /// <returns></returns>
        public static RetryPolicyBuilder NewBuilder() => new();
    }
}