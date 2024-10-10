using System;
using System.Collections.Generic;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;

namespace Streamiz.Kafka.Net.Table
{
    public static class SuppressedBuilder
    {
        public static Suppressed<K, V> UntilWindowClose<K, V>(TimeSpan gracePeriod, IBufferConfig bufferConfig)
            where K : Windowed => new(
            null, 
            gracePeriod, 
            bufferConfig, 
            (_, key) => key.Window.EndMs,
            true);
            
        public static Suppressed<K, V> UntilTimeLimit<K, V>(TimeSpan timeToWaitMoreEvents, IBufferConfig bufferConfig) 
            => new(
                null, 
                timeToWaitMoreEvents, 
                bufferConfig, 
                (context, _) => context.Timestamp,
                false);
    }

    public enum BUFFER_FULL_STRATEGY
    {
        EMIT,
        SHUTDOWN
    }
    
    public interface IBufferConfig
    {
        long MaxRecords{ get; }
        long MaxBytes { get; }
        BUFFER_FULL_STRATEGY BufferFullStrategy { get; }
        bool LoggingEnabled { get; }
        IDictionary<string, string> Config { get; }
    }
    
    public class StrictBufferConfig : IBufferConfig
    {
        public long MaxRecords { get; }
        public long MaxBytes { get; }
        public BUFFER_FULL_STRATEGY BufferFullStrategy => BUFFER_FULL_STRATEGY.SHUTDOWN;
        public bool LoggingEnabled { get; private set; }
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
        
        public static StrictBufferConfig Unbounded() => new();
        public static StrictBufferConfig Bounded(long maxRecords) => new(maxRecords, Int64.MaxValue);
        public static StrictBufferConfig Bounded(CacheSize maxBytes) => new(Int64.MaxValue, maxBytes.CacheSizeBytes);
        
        public StrictBufferConfig WithLoggingEnabled(IDictionary<string, string> config)
        {
            Config = config ?? new Dictionary<string, string>();
            LoggingEnabled = true;
            return this;
        }

        public StrictBufferConfig WithLoggingDisabled()
        {
            Config = null;
            LoggingEnabled = false;
            return this;
        }
    }

    public class EagerConfig : IBufferConfig
    {
        public long MaxRecords { get; }
        public long MaxBytes { get; }
        public BUFFER_FULL_STRATEGY BufferFullStrategy => BUFFER_FULL_STRATEGY.EMIT;
        public bool LoggingEnabled { get; private set; }
        public IDictionary<string, string> Config { get; private set; }

        private EagerConfig(long maxRecords, long maxBytes)
        {
            MaxRecords = maxRecords;
            MaxBytes = maxBytes;
            LoggingEnabled = true;
            Config = new Dictionary<string, string>();
        }

        public static EagerConfig EmitEarlyWhenFull(long maxRecords) => new(maxRecords, Int64.MaxValue);
        public static EagerConfig EmitEarlyWhenFull(CacheSize maxBytes) => new(Int64.MaxValue, maxBytes.CacheSizeBytes);
        public static EagerConfig EmitEarlyWhenFull(long maxRecords, CacheSize maxBytes) => new(maxRecords, maxBytes.CacheSizeBytes);

        public EagerConfig WithLoggingEnabled(IDictionary<string, string> config)
        {
            Config = config ?? new Dictionary<string, string>();
            LoggingEnabled = true;
            return this;
        }

        public EagerConfig WithLoggingDisabled()
        {
            Config = null;
            LoggingEnabled = false;
            return this;
        }
    }
    
    public class Suppressed<K,V>
    {
        public TimeSpan SuppressionTime { get; }
        public IBufferConfig BufferConfig { get; }
        public Func<ProcessorContext, K, long> TimeDefinition { get; }
        public bool SafeToDropTombstones { get; }
        public string Name { get; internal set; }
        
        public ISerDes<K> KeySerdes { get; private set; }
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

        public Suppressed<K, V> WithKeySerdes(ISerDes<K> keySerdes)
        {
            KeySerdes = keySerdes;
            return this;
        }
        
        public Suppressed<K, V> WithValueSerdes(ISerDes<V> valueSerdes)
        {
            ValueSerdes = valueSerdes;
            return this;
        }
    }
}