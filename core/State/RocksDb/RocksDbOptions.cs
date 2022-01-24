using RocksDbSharp;
using System;

namespace Streamiz.Kafka.Net.State.RocksDb
{
    /// <summary>
    /// RocksDB log levels.
    /// </summary>
    public enum RocksLogLevel
    {
        /// <summary>
        /// Debug level
        /// </summary>
        DEBUG = 0,
        /// <summary>
        /// Info level
        /// </summary>
        INFO = 1,
        /// <summary>
        /// Warning level
        /// </summary>
        WARN = 2,
        /// <summary>
        /// Error level
        /// </summary>
        ERROR = 3,
        /// <summary>
        /// Fatal level
        /// </summary>
        FATAL = 4,
        /// <summary>
        /// Header level
        /// </summary>
        HEADER = 5,
        /// <summary>
        /// Num info level
        /// </summary>
        NUM_INFO_LOG = 6
    }

    /// <summary>
    /// Rocksdb class config adapter.
    /// This class adapt <see cref="DbOptions"/> and <see cref="ColumnFamilyOptions"/>,
    /// allow users to custom the configuration of RocksDb state store using <see cref="IStreamConfig.RocksDbConfigHandler" />
    /// </summary>
    public class RocksDbOptions
    {
        private readonly DbOptions dbOptions;
        private readonly ColumnFamilyOptions columnFamilyOptions;

        internal RocksDbOptions(DbOptions dbOptions, ColumnFamilyOptions columnFamilyOptions)
        {
            this.dbOptions = dbOptions;
            this.columnFamilyOptions = columnFamilyOptions;
        }

        #region DbOptions
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
        #endregion

        #region ColumnFamilyOptions
        public RocksDbOptions OptimizeForPointLookup(ulong blockCacheSizeMb)
        {
            columnFamilyOptions.OptimizeForPointLookup(blockCacheSizeMb);
            return this;
        }
        public RocksDbOptions OptimizeLevelStyleCompaction(ulong memtableMemoryBudget)
        {
            columnFamilyOptions.OptimizeLevelStyleCompaction(memtableMemoryBudget);
            return this;
        }
        public RocksDbOptions OptimizeUniversalStyleCompaction(ulong memtableMemoryBudget)
        {
            columnFamilyOptions.OptimizeUniversalStyleCompaction(memtableMemoryBudget);
            return this;
        }
        public RocksDbOptions SetArenaBlockSize(ulong value)
        {
            columnFamilyOptions.SetArenaBlockSize(value);
            return this;
        }
        public RocksDbOptions SetBlockBasedTableFactory(BlockBasedTableOptions table_options)
        {
            columnFamilyOptions.SetBlockBasedTableFactory(table_options);
            return this;
        }
        public RocksDbOptions SetBloomLocality(uint value)
        {
            columnFamilyOptions.SetBloomLocality(value);
            return this;
        }
        public RocksDbOptions SetCompactionFilter(IntPtr compactionFilter)
        {
            columnFamilyOptions.SetCompactionFilter(compactionFilter);
            return this;
        }
        public RocksDbOptions SetCompactionFilterFactory(IntPtr compactionFilterFactory)
        {
            columnFamilyOptions.SetCompactionFilterFactory(compactionFilterFactory);
            return this;
        }
        public RocksDbOptions SetCompactionReadaheadSize(ulong size)
        {
            columnFamilyOptions.SetCompactionReadaheadSize(size);
            return this;
        }
        public RocksDbOptions SetCompactionStyle(Compaction value)
        {
            columnFamilyOptions.SetCompactionStyle(value);
            return this;
        }
        public RocksDbOptions SetComparator(IntPtr comparator)
        {
            columnFamilyOptions.SetComparator(comparator);
            return this;
        }
        public RocksDbOptions SetComparator(Comparator comparator)
        {
            columnFamilyOptions.SetComparator(comparator);
            return this;
        }
        public RocksDbOptions SetCompression(Compression value)
        {
            columnFamilyOptions.SetCompression(value);
            return this;
        }
        public RocksDbOptions SetCompressionOptions(int p1, int p2, int p3, int p4)
        {
            columnFamilyOptions.SetCompressionOptions(p1, p2, p3, p4);
            return this;
        }
        public RocksDbOptions SetCompressionPerLevel(Compression[] levelValues, ulong numLevels)
        {
            columnFamilyOptions.SetCompressionPerLevel(levelValues, numLevels);
            return this;
        }
        [Obsolete]
        public RocksDbOptions SetCompressionPerLevel(Compression[] levelValues, UIntPtr numLevels)
        {
            columnFamilyOptions.SetCompressionPerLevel(levelValues, numLevels);
            return this;
        }
        public RocksDbOptions SetDisableAutoCompactions(int value)
        {
            columnFamilyOptions.SetDisableAutoCompactions(value);
            return this;
        }
        public RocksDbOptions SetFifoCompactionOptions(IntPtr fifoCompactionOptions)
        {
            columnFamilyOptions.SetFifoCompactionOptions(fifoCompactionOptions);
            return this;
        }
        public RocksDbOptions SetHardPendingCompactionBytesLimit(ulong value)
        {
            columnFamilyOptions.SetHardPendingCompactionBytesLimit(value);
            return this;
        }
        [Obsolete]
        public RocksDbOptions SetHardRateLimit(double value)
        {
            columnFamilyOptions.SetHardRateLimit(value);
            return this;
        }
        public RocksDbOptions SetHashLinkListRep(ulong value)
        {
            columnFamilyOptions.SetHashLinkListRep(value);
            return this;
        }
        public RocksDbOptions SetHashSkipListRep(ulong p1, int p2, int p3)
        {
            columnFamilyOptions.SetHashSkipListRep(p1, p2, p3);
            return this;
        }
        public RocksDbOptions SetInfoLogLevel(RocksLogLevel value)
        {
            columnFamilyOptions.SetInfoLogLevel((int)value);
            return this;
        }
        public RocksDbOptions SetInplaceUpdateNumLocks(ulong value)
        {
            columnFamilyOptions.SetInplaceUpdateNumLocks(value);
            return this;
        }
        public RocksDbOptions SetInplaceUpdateSupport(bool value)
        {
            columnFamilyOptions.SetInplaceUpdateSupport(value);
            return this;
        }
        public RocksDbOptions SetLevel0FileNumCompactionTrigger(int value)
        {
            columnFamilyOptions.SetLevel0FileNumCompactionTrigger(value);
            return this;
        }
        public RocksDbOptions SetLevel0SlowdownWritesTrigger(int value)
        {
            columnFamilyOptions.SetLevel0SlowdownWritesTrigger(value);
            return this;
        }
        public RocksDbOptions SetLevel0StopWritesTrigger(int value)
        {
            columnFamilyOptions.SetLevel0StopWritesTrigger(value);
            return this;
        }
        public RocksDbOptions SetLevelCompactionDynamicLevelBytes(bool value)
        {
            columnFamilyOptions.SetLevelCompactionDynamicLevelBytes(value);
            return this;
        }
        public RocksDbOptions SetMaxBytesForLevelBase(ulong value)
        {
            columnFamilyOptions.SetMaxBytesForLevelBase(value);
            return this;
        }
        public RocksDbOptions SetMaxBytesForLevelMultiplier(double value)
        {
            columnFamilyOptions.SetMaxBytesForLevelMultiplier(value);
            return this;
        }
        public RocksDbOptions SetMaxBytesForLevelMultiplierAdditional(int[] levelValues, ulong numLevels)
        {
            columnFamilyOptions.SetMaxBytesForLevelMultiplierAdditional(levelValues, numLevels);
            return this;
        }
        public RocksDbOptions SetMaxCompactionBytes(ulong bytes)
        {
            columnFamilyOptions.SetMaxCompactionBytes(bytes);
            return this;
        }
        [Obsolete]
        public RocksDbOptions SetMaxMemCompactionLevel(int value)
        {
            columnFamilyOptions.SetMaxMemCompactionLevel(value);
            return this;
        }
        public RocksDbOptions SetMaxSequentialSkipInIterations(ulong value)
        {
            columnFamilyOptions.SetMaxSequentialSkipInIterations(value);
            return this;
        }
        public RocksDbOptions SetMaxSuccessiveMerges(ulong value)
        {
            columnFamilyOptions.SetMaxSuccessiveMerges(value);
            return this;
        }
        public RocksDbOptions SetMaxWriteBufferNumber(int value)
        {
            columnFamilyOptions.SetMaxWriteBufferNumber(value);
            return this;
        }
        public RocksDbOptions SetMaxWriteBufferNumberToMaintain(int value)
        {
            columnFamilyOptions.SetMaxWriteBufferNumberToMaintain(value);
            return this;
        }
        public RocksDbOptions SetMemtableHugePageSize(ulong size)
        {
            columnFamilyOptions.SetMemtableHugePageSize(size);
            return this;
        }
        public RocksDbOptions SetMemtablePrefixBloomSizeRatio(double ratio)
        {
            columnFamilyOptions.SetMemtablePrefixBloomSizeRatio(ratio);
            return this;
        }
        public RocksDbOptions SetMemtableVectorRep()
        {
            columnFamilyOptions.SetMemtableVectorRep();
            return this;
        }
        public RocksDbOptions SetMergeOperator(IntPtr mergeOperator)
        {
            columnFamilyOptions.SetMergeOperator(mergeOperator);
            return this;
        }
        public RocksDbOptions SetMergeOperator(MergeOperator mergeOperator)
        {
            columnFamilyOptions.SetMergeOperator(mergeOperator);
            return this;
        }
        public RocksDbOptions SetMinLevelToCompress(int level)
        {
            columnFamilyOptions.SetMinLevelToCompress(level);
            return this;
        }
        public RocksDbOptions SetMinWriteBufferNumberToMerge(int value)
        {
            columnFamilyOptions.SetMinWriteBufferNumberToMerge(value);
            return this;
        }
        public RocksDbOptions SetNumLevels(int value)
        {
            columnFamilyOptions.SetNumLevels(value);

            return this;
        }
        public RocksDbOptions SetOptimizeFiltersForHits(int value)
        {
            columnFamilyOptions.SetOptimizeFiltersForHits(value);
            return this;
        }
        public RocksDbOptions SetPlainTableFactory(uint p1, int p2, double p3, ulong p4)
        {
            columnFamilyOptions.SetPlainTableFactory(p1, p2, p3, p4);
            return this;
        }
        public RocksDbOptions SetPrefixExtractor(SliceTransform sliceTransform)
        {
            columnFamilyOptions.SetPrefixExtractor(sliceTransform);
            return this;
        }
        public RocksDbOptions SetPrefixExtractor(IntPtr sliceTransform)
        {
            columnFamilyOptions.SetPrefixExtractor(sliceTransform);
            return this;
        }
        [Obsolete]
        public RocksDbOptions SetPurgeRedundantKvsWhileFlush(bool value)
        {
            columnFamilyOptions.SetPurgeRedundantKvsWhileFlush(value);
            return this;
        }
        [Obsolete("no longer used")]
        public RocksDbOptions SetRateLimitDelayMaxMilliseconds(uint value)
        {
            columnFamilyOptions.SetRateLimitDelayMaxMilliseconds(value);
            return this;
        }
        public RocksDbOptions SetReportBgIoStats(bool value)
        {
            columnFamilyOptions.SetReportBgIoStats(value);
            return this;
        }
        [Obsolete("no longer used")]
        public RocksDbOptions SetSkipLogErrorOnRecovery(bool value)
        {
            columnFamilyOptions.SetSkipLogErrorOnRecovery(value);
            return this;
        }
        public RocksDbOptions SetSoftPendingCompactionBytesLimit(ulong value)
        {
            columnFamilyOptions.SetSoftPendingCompactionBytesLimit(value);
            return this;
        }
        [Obsolete("no longer used")]
        public RocksDbOptions SetSoftRateLimit(double value)
        {
            columnFamilyOptions.SetSoftRateLimit(value);
            return this;
        }
        public RocksDbOptions SetTargetFileSizeBase(ulong value)
        {
            columnFamilyOptions.SetTargetFileSizeBase(value);
            return this;
        }
        public RocksDbOptions SetTargetFileSizeMultiplier(int value)
        {
            columnFamilyOptions.SetTargetFileSizeMultiplier(value);
            return this;
        }
        public RocksDbOptions SetUint64addMergeOperator()
        {
            columnFamilyOptions.SetUint64addMergeOperator();
            return this;
        }
        public RocksDbOptions SetUniversalCompactionOptions(IntPtr universalCompactionOptions)
        {
            columnFamilyOptions.SetUniversalCompactionOptions(universalCompactionOptions);
            return this;
        }
        public RocksDbOptions SetWriteBufferSize(ulong value)
        {
            columnFamilyOptions.SetWriteBufferSize(value);
            return this;
        }
        #endregion
    }
}