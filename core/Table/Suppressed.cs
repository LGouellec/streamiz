using System;
using System.Collections.Generic;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;

namespace Streamiz.Kafka.Net.Table
{
    /// <summary>
    /// <see cref="Suppressed{K,V}"/> builder
    /// </summary>
    public static class SuppressedBuilder
    {
        /// <summary>
        /// Configure the suppression to emit only the "final results" from the window.
        /// <para>
        /// By default all Streams operators emit results whenever new results are available. This includes windowed operations.
        /// </para>
        /// <para>
        /// Using a <see cref="StrictBufferConfig"/> configuration will instead emit just one result per key for each window, guaranteeing
        /// to deliver only the final result. This option is suitable for use cases in which the business logic
        /// requires a hard guarantee that only the final result is propagated. For example, sending alerts.
        /// </para>
        /// To accomplish this, the operator will buffer events from the window until the window close (that is,
        /// until the end-time passes, and additionally until the grace period expires). Since windowed operators
        /// are required to reject out-of-order events for a window whose grace period is expired, there is an additional
        /// guarantee that the final results emitted from this suppression will match any queryable state upstream.
        /// <para>
        /// Using a <see cref="EagerConfig"/> configuration will define how much space to use for buffering intermediate results.
        /// It may results multiple values for the same window depending your throughput (# of keys or # of bytes).
        /// In this case, it can be appropriate to limit the number of downstream records and limiting the memory usage of your application.
        /// </para>
        /// </summary>
        /// <param name="gracePeriod">Define the grace period where your window is considered expired</param>
        /// <param name="bufferConfig">A configuration specifying how much space to use for buffering intermediate results.</param>
        /// <typeparam name="K">Key type</typeparam>
        /// <typeparam name="V">Value type</typeparam>
        /// <returns>a suppression configuration</returns>
        public static Suppressed<K, V> UntilWindowClose<K, V>(TimeSpan gracePeriod, IBufferConfig bufferConfig)
            where K : Windowed => new(
            null, 
            gracePeriod, 
            bufferConfig, 
            (_, key) => key.Window.EndMs,
            true);
            
        /// <summary>
        /// Configure the suppression to wait a specific amount of time after receiving a record before emitting it further downstream.
        /// If another record for the same key arrives in the mean time, it replaces the first record in the buffer but does not re-start the timer.
        /// </summary>
        /// <param name="timeToWaitMoreEvents">The amount of time to wait, per record, for new events.</param>
        /// <param name="bufferConfig">A configuration specifying how much space to use for buffering intermediate results.</param>
        /// <typeparam name="K">Key type</typeparam>
        /// <typeparam name="V">Value type</typeparam>
        /// <returns>a suppression configuration</returns>
        public static Suppressed<K, V> UntilTimeLimit<K, V>(TimeSpan timeToWaitMoreEvents, IBufferConfig bufferConfig) 
            => new(
                null, 
                timeToWaitMoreEvents, 
                bufferConfig, 
                (context, _) => context.Timestamp,
                false);
    }

    /// <summary>
    /// Buffer Full Strategy
    /// </summary>
    public enum BUFFER_FULL_STRATEGY
    {
        /// <summary>
        /// Emit early the results
        /// </summary>
        EMIT,
        /// <summary>
        /// Stop the stream
        /// </summary>
        SHUTDOWN
    }
    
    /// <summary>
    /// Determine the buffer rules for the suppress processor in term of records, cache size and the strategy in case the byffer is full
    /// </summary>
    public interface IBufferConfig
    {
        /// <summary>
        /// Define how many records the buffer can accept
        /// </summary>
        long MaxRecords{ get; }
        /// <summary>
        /// Define how many bytes the buffer can store
        /// </summary>
        long MaxBytes { get; }
        /// <summary>
        /// Define the buffer full strategy policy
        /// </summary>
        BUFFER_FULL_STRATEGY BufferFullStrategy { get; }
        /// <summary>
        /// Enable the change logging
        /// </summary>
        bool LoggingEnabled { get; }
        /// <summary>
        /// Explicit config for the backup changelog topic
        /// </summary>
        IDictionary<string, string> Config { get; }
    }
    
    /// <summary>
    /// Marker class for a buffer configuration that is "strict" in the sense that it will strictly enforce the time bound and never emit early.
    /// </summary>
    public class StrictBufferConfig : IBufferConfig
    {
        /// <summary>
        /// Define how many records the buffer can accept
        /// </summary>
        public long MaxRecords { get; }
        /// <summary>
        /// Define how many bytes the buffer can store
        /// </summary>
        public long MaxBytes { get; }
        /// <summary>
        /// Define the buffer full strategy policy as "SHUTDOWN"
        /// </summary>
        public BUFFER_FULL_STRATEGY BufferFullStrategy => BUFFER_FULL_STRATEGY.SHUTDOWN;
        /// <summary>
        /// Enable the change logging
        /// </summary>
        public bool LoggingEnabled { get; private set; }
        /// <summary>
        /// Explicit config for the backup changelog topic
        /// </summary>
        public IDictionary<string, string> Config { get; private set;}
        
        private StrictBufferConfig(long maxRecords, long maxBytes)
        {
            MaxRecords = maxRecords;
            MaxBytes = maxBytes;
            LoggingEnabled = true;
            Config = new Dictionary<string, string>();
        }
        
        private StrictBufferConfig()
        {
            MaxRecords = Int64.MaxValue;
            MaxBytes = Int64.MaxValue;
            LoggingEnabled = true;
            Config = new Dictionary<string, string>();
        }
        
        /// <summary>
        /// Create a buffer unconstrained by size (either keys or bytes).
        /// As a result, the buffer will consume as much memory as it needs, dictated by the time bound.
        /// If there isn't enough resource available to meet the demand, the application will encounter an
        /// <see cref="OutOfMemoryException"/> and shut down (not guaranteed to be a graceful exit). 
        /// This is a convenient option if you doubt that your buffer will be that large, but also don't
        /// wish to pick particular constraints, such as in testing.
        /// This buffer is "strict" in the sense that it will enforce the time bound or crash.
        /// It will never emit early.
        /// </summary>
        /// <returns></returns>
        public static StrictBufferConfig Unbounded() => new();
        
        /// <summary>
        /// Create a buffer to gracefully shut down the application when the number of different keys exceed the maximum.
        /// </summary>
        /// <param name="maxRecords">Maximum number of different keys</param>
        /// <returns></returns>
        public static StrictBufferConfig Bounded(long maxRecords) => new(maxRecords, Int64.MaxValue);
        
        /// <summary>
        /// Create a buffer to gracefully shut down the application when the number of bytes used by the buffer is exceed.
        /// </summary>
        /// <param name="maxBytes">Maximum number of bytes</param>
        /// <returns></returns>
        public static StrictBufferConfig Bounded(CacheSize maxBytes) => new(Int64.MaxValue, maxBytes.CacheSizeBytes);
        
        /// <summary>
        /// Enable the logging feature
        /// </summary>
        /// <param name="config">Optional configuration for the changelog topic</param>
        /// <returns></returns>
        public StrictBufferConfig WithLoggingEnabled(IDictionary<string, string> config)
        {
            Config = config ?? new Dictionary<string, string>();
            LoggingEnabled = true;
            return this;
        }

        /// <summary>
        /// Disable the logging feature
        /// </summary>
        /// <returns></returns>
        public StrictBufferConfig WithLoggingDisabled()
        {
            Config = null;
            LoggingEnabled = false;
            return this;
        }
    }

    /// <summary>
    /// Marker class for a buffer configuration that will strictly enforce size constraints (bytes and/or number of records) on the buffer, so it is suitable for reducing duplicate
    /// results downstream, but does not promise to eliminate them entirely.
    /// </summary>
    public class EagerConfig : IBufferConfig
    {
        /// <summary>
        /// Define how many records the buffer can accept
        /// </summary>
        public long MaxRecords { get; }
        /// <summary>
        /// Define how many bytes the buffer can store
        /// </summary>
        public long MaxBytes { get; }
        /// <summary>
        /// Define the buffer full strategy policy as "EMIT"
        /// </summary>
        public BUFFER_FULL_STRATEGY BufferFullStrategy => BUFFER_FULL_STRATEGY.EMIT;
        /// <summary>
        /// Enable the change logging
        /// </summary>
        public bool LoggingEnabled { get; private set; }
        /// <summary>
        /// Explicit config for the backup changelog topic
        /// </summary>
        public IDictionary<string, string> Config { get; private set; }

        private EagerConfig(long maxRecords, long maxBytes)
        {
            MaxRecords = maxRecords;
            MaxBytes = maxBytes;
            LoggingEnabled = true;
            Config = new Dictionary<string, string>();
        }

        /// <summary>
        /// Create a size-constrained buffer in terms of the maximum number of keys it will store.
        /// </summary>
        /// <param name="maxRecords">Maximum number of keys</param>
        /// <returns></returns>
        public static EagerConfig EmitEarlyWhenFull(long maxRecords) => new(maxRecords, Int64.MaxValue);
        
        /// <summary>
        /// Create a size-constrained buffer in terms of the maximum number of bytes it will use.
        /// </summary>
        /// <param name="maxBytes">Maximum number of bytes</param>
        /// <returns></returns>
        public static EagerConfig EmitEarlyWhenFull(CacheSize maxBytes) => new(Int64.MaxValue, maxBytes.CacheSizeBytes);
        
        /// <summary>
        /// Create a size-constrained buffer in terms of the maximum number of keys it will store OR maximum number of bytes it will use.
        /// </summary>
        /// <param name="maxRecords">Maximum number of keys</param>
        /// <param name="maxBytes">Maximum number of bytes</param>
        /// <returns></returns>
        public static EagerConfig EmitEarlyWhenFull(long maxRecords, CacheSize maxBytes) => new(maxRecords, maxBytes.CacheSizeBytes);

        /// <summary>
        /// Enable the logging feature
        /// </summary>
        /// <param name="config">Optional configuration for the changelog topic</param>
        /// <returns></returns>
        public EagerConfig WithLoggingEnabled(IDictionary<string, string> config)
        {
            Config = config ?? new Dictionary<string, string>();
            LoggingEnabled = true;
            return this;
        }

        /// <summary>
        /// Disable the logging feature
        /// </summary>
        /// <returns></returns>
        public EagerConfig WithLoggingDisabled()
        {
            Config = null;
            LoggingEnabled = false;
            return this;
        }
    }
    
    /// <summary>
    /// Define the behavior of the suppress processor used in <see cref="IKTable{K,V}.Suppress"/>.
    /// </summary>
    /// <typeparam name="K">Type of key</typeparam>
    /// <typeparam name="V">Type of value</typeparam>
    public class Suppressed<K,V>
    {
        /// <summary>
        /// Used as a grace period
        /// </summary>
        public TimeSpan SuppressionTime { get; }
        
        /// <summary>
        /// Define the buffer config policy
        /// </summary>
        public IBufferConfig BufferConfig { get; }
        
        /// <summary>
        /// Define the buffer time for each record
        /// </summary>
        public Func<ProcessorContext, K, long> TimeDefinition { get; }
        
        /// <summary>
        /// Be careful. It's only safe to drop tombstones for windowed KTables in "final results" mode.
        /// </summary>
        public bool SafeToDropTombstones { get; }
        
        /// <summary>
        /// Name of the suppress processor
        /// </summary>
        public string Name { get; internal set; }
        
        /// <summary>
        /// (Optional) Key serdes
        /// </summary>
        public ISerDes<K> KeySerdes { get; private set; }
        
        /// <summary>
        /// (Optional) Value serdes
        /// </summary>
        public ISerDes<V> ValueSerdes { get; private set; }
        
        internal Suppressed(
            string name,
            TimeSpan suppressionTime,
            IBufferConfig bufferConfig,
            Func<ProcessorContext, K, long> timeDefinition,
            bool safeToDropTombstones)
        {
            Name = name;
            SuppressionTime = suppressionTime;
            BufferConfig = bufferConfig;
            TimeDefinition = timeDefinition;
            SafeToDropTombstones = safeToDropTombstones;
        }

        /// <summary>
        /// Define the key serdes
        /// </summary>
        /// <param name="keySerdes">key serdes</param>
        /// <returns></returns>
        public Suppressed<K, V> WithKeySerdes(ISerDes<K> keySerdes)
        {
            KeySerdes = keySerdes;
            return this;
        }
        
        /// <summary>
        /// Define the value serdes 
        /// </summary>
        /// <param name="valueSerdes">value serdes</param>
        /// <returns></returns>
        public Suppressed<K, V> WithValueSerdes(ISerDes<V> valueSerdes)
        {
            ValueSerdes = valueSerdes;
            return this;
        }
    }
}