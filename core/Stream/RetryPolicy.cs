using System;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Stream
{
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
        private List<Type> retriableExceptions = DEFAULT_RETRIABLE_EXCEPTIONS;
        private long memoryBufferSize = DEFAULT_MEMORY_BUFFER_SIZE;
        private long timeoutMs;

        private RetryPolicyBuilder()
        {
            
        }

        public static RetryPolicyBuilder NewBuilder() => new RetryPolicyBuilder();
        
        public RetryPolicyBuilder RetryBackOffMs(long retryBackOffMs)
        {
            this.retryBackOffMs = retryBackOffMs;
            return this;
        }

        public RetryPolicyBuilder NumberOfRetry(int numberOfRetry)
        {
            this.numberOfRetry = numberOfRetry;
            return this;
        }

        public RetryPolicyBuilder RetryBehavior(EndRetryBehavior endRetryBehavior)
        {
            this.endRetryBehavior = endRetryBehavior;
            return this;
        }

        public RetryPolicyBuilder RetriableException<T>()
            where T : Exception
        {
            retriableExceptions.Add(typeof(T));
            return this;
        }

        public RetryPolicyBuilder MemoryBufferSize(long memoryBufferSize)
        {
            this.memoryBufferSize = memoryBufferSize;
            return this;
        }

        public RetryPolicyBuilder TimeoutMs(long timeoutMs)
        {
            this.timeoutMs = timeoutMs;
            return this;
        }
        
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

    public enum EndRetryBehavior
    {
        SKIP,
        FAIL,
        BUFFERED
    }
    
    public class RetryPolicy
    {
        public long TimeoutMs { get; internal set; }
        public long RetryBackOffMs { get; }
        public int NumberOfRetry { get; }
        public EndRetryBehavior EndRetryBehavior { get; }
        public List<Type> RetriableExceptions { get; }
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
    }
}