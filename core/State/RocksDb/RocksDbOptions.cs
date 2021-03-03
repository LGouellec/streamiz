using RocksDbSharp;
using System;

namespace Streamiz.Kafka.Net.State.RocksDb
{
    public class RocksDbOptions
    {
        private readonly DbOptions dbOptions;
        private readonly ColumnFamilyOptions columnFamilyOptions;

        internal RocksDbOptions(DbOptions dbOptions, ColumnFamilyOptions columnFamilyOptions)
        {
            this.dbOptions = dbOptions;
            this.columnFamilyOptions = columnFamilyOptions;
        }

        public RocksDbOptions EnableStatistics()
        {
            dbOptions.EnableStatistics();
            return this;
        }
        public string StatisticsString => dbOptions.GetStatisticsString();
        public RocksDbOptions IncreaseParallelism(int totalThreads)
        {
            dbOptions.IncreaseParallelism(totalThreads);
            return this;
        }
        public RocksDbOptions PrepareForBulkLoad()
        {
            dbOptions.PrepareForBulkLoad();
            return this;
        }
        public RocksDbOptions SetAccessHintOnCompactionStart(int value)
        {
            dbOptions.SetAccessHintOnCompactionStart(value);
            return this;
        }
        public RocksDbOptions SetAdviseRandomOnOpen(bool value)
        {
            dbOptions.SetAdviseRandomOnOpen(value);
            return this;
        }
        public RocksDbOptions SetAllowConcurrentMemtableWrite(bool value)
        {
            dbOptions.SetAllowConcurrentMemtableWrite(value);
            return this;
        }
        public RocksDbOptions SetAllowMmapReads(bool value)
        {
            dbOptions.SetAllowMmapReads(value);
            return this;
        }
        public RocksDbOptions SetAllowMmapWrites(bool value)
        {
            dbOptions.SetAllowMmapWrites(value);
            return this;
        }
        public RocksDbOptions SetBaseBackgroundCompactions(int value)
        {
            dbOptions.SetBaseBackgroundCompactions(value);
            return this;
        }
        public RocksDbOptions SetBytesPerSync(ulong value)
        {
            dbOptions.SetBytesPerSync(value);
            return this;
        }
        public RocksDbOptions SetCreateIfMissing(bool value = true)
        {
            dbOptions.SetCreateIfMissing(value);
            return this;
        }
        public RocksDbOptions SetCreateMissingColumnFamilies(bool value = true)
        {
            dbOptions.SetCreateMissingColumnFamilies(value);
            return this;
        }
        public RocksDbOptions SetDbLogDir(string value)
        {
            dbOptions.SetDbLogDir(value);
            return this;
        }
        public RocksDbOptions SetDbWriteBufferSize(ulong size)
        {
            dbOptions.SetDbWriteBufferSize(size);
            return this;
        }
        public RocksDbOptions SetDeleteObsoleteFilesPeriodMicros(ulong value)
        {
            dbOptions.SetDeleteObsoleteFilesPeriodMicros(value);
            return this;
        }
        public RocksDbOptions SetEnableWriteThreadAdaptiveYield(bool value)
        {
            dbOptions.SetEnableWriteThreadAdaptiveYield(value);
            return this;
        }
        public RocksDbOptions SetEnv(IntPtr env)
        {
            dbOptions.SetEnv(env);
            return this;
        }
        public RocksDbOptions SetErrorIfExists(bool value = true)
        {
            dbOptions.SetErrorIfExists(value);
            return this;
        }
        public RocksDbOptions SetInfoLog(IntPtr logger)
        {
            dbOptions.SetInfoLog(logger);
            return this;
        }
        public RocksDbOptions SetIsFdCloseOnExec(bool value)
        {
            dbOptions.SetIsFdCloseOnExec(value);
            return this;
        }
        public RocksDbOptions SetKeepLogFileNum(ulong value)
        {
            dbOptions.SetKeepLogFileNum(value);
            return this;
        }
        public RocksDbOptions SetLogFileTimeToRoll(ulong value)
        {
            dbOptions.SetLogFileTimeToRoll(value);
            return this;
        }
        public RocksDbOptions SetManifestPreallocationSize(ulong value)
        {
            dbOptions.SetManifestPreallocationSize(value);
            return this;
        }
        public RocksDbOptions SetMaxBackgroundCompactions(int value)
        {
            dbOptions.SetMaxBackgroundCompactions(value);
            return this;
        }
        public RocksDbOptions SetMaxBackgroundFlushes(int value)
        {
            dbOptions.SetMaxBackgroundFlushes(value);
            return this;
        }
        public RocksDbOptions SetMaxFileOpeningThreads(int value)
        {
            dbOptions.SetMaxFileOpeningThreads(value);
            return this;
        }
        public RocksDbOptions SetMaxLogFileSize(ulong value)
        {
            dbOptions.SetMaxLogFileSize(value);
            return this;
        }
        public RocksDbOptions SetMaxManifestFileSize(ulong value)
        {
            dbOptions.SetMaxManifestFileSize(value);
            return this;
        }
        public RocksDbOptions SetMaxOpenFiles(int value)
        {
            dbOptions.SetMaxOpenFiles(value);
            return this;
        }
        public RocksDbOptions SetMaxTotalWalSize(ulong n)
        {
            dbOptions.SetMaxTotalWalSize(n);
            return this;
        }
        public RocksDbOptions SetParanoidChecks(bool value = true)
        {
            dbOptions.SetParanoidChecks(value);
            return this;
        }
        public RocksDbOptions SetRecycleLogFileNum(ulong value)
        {
            dbOptions.SetRecycleLogFileNum(value);
            return this;
        }
        public RocksDbOptions SetStatsDumpPeriodSec(uint value)
        {
            dbOptions.SetStatsDumpPeriodSec(value);
            return this;
        }
        public RocksDbOptions SetTableCacheNumShardbits(int value)
        {
            dbOptions.SetTableCacheNumShardbits(value);
            return this;
        }
        [Obsolete]
        public RocksDbOptions SetTableCacheRemoveScanCountLimit(int value)
        {
            dbOptions.SetTableCacheRemoveScanCountLimit(value);
            return this;
        }
        public RocksDbOptions SetUseAdaptiveMutex(bool value)
        {
            dbOptions.SetUseAdaptiveMutex(value);
            return this;
        }
        public RocksDbOptions SetUseDirectIoForFlushAndCompaction(bool value)
        {
            dbOptions.SetUseDirectIoForFlushAndCompaction(value);
            return this;
        }
        public RocksDbOptions SetUseDirectReads(bool value)
        {
            dbOptions.SetUseDirectReads(value);
            return this;
        }
        public RocksDbOptions SetUseFsync(int value)
        {
            dbOptions.SetUseFsync(value);
            return this;
        }
        public RocksDbOptions SetWalDir(string value)
        {
            dbOptions.SetWalDir(value);
            return this;
        }
        public RocksDbOptions SetWalRecoveryMode(Recovery mode)
        {
            dbOptions.SetWalRecoveryMode(mode);
            return this;
        }
        public RocksDbOptions SetWALSizeLimitMB(ulong value)
        {
            dbOptions.SetWALSizeLimitMB(value);
            return this;
        }
        public RocksDbOptions SetWALTtlSeconds(ulong value)
        {
            dbOptions.SetWALTtlSeconds(value);
            return this;
        }
        public RocksDbOptions SkipStatsUpdateOnOpen(bool val = false)
        {
            dbOptions.SkipStatsUpdateOnOpen(val);
            return this;
        }
    }
}