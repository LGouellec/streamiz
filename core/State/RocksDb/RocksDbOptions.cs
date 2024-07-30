using RocksDbSharp;
using System;

namespace Streamiz.Kafka.Net.State
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
        
        /// <summary>
        /// Enable rocksdb statistics
        /// </summary>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions EnableStatistics()
        {
            dbOptions.EnableStatistics();
            return this;
        }
        
        /// <summary>
        /// Get the current statistic informations.
        /// </summary>
        public string StatisticsString => dbOptions.GetStatisticsString();
        
        /// <summary>
        /// By default, RocksDB uses only one background thread for flush and compaction. Calling this function will set it up such that total of `total_threads` is used.
        /// You almost definitely want to call this function if your system is bottlenecked by RocksDB.
        /// </summary>
        /// <param name="totalThreads">number of thread allocated to rocksdb instance</param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions IncreaseParallelism(int totalThreads)
        {
            dbOptions.IncreaseParallelism(totalThreads);
            return this;
        }
        
        /// <summary>
        /// Set appropriate parameters for bulk loading. The reason that this is a function that returns "this" instead of a constructor is to enable chaining of multiple similar calls in the future.
        /// All data will be in level 0 without any automatic compaction. It's recommended to manually call CompactRange(NULL, NULL) before reading from the database, because otherwise the read can be very slow.
        /// </summary>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions PrepareForBulkLoad()
        {
            dbOptions.PrepareForBulkLoad();
            return this;
        }
        
        /// <summary>
        /// Specify the file access pattern once a compaction is started. It will be applied to all input files of a compaction. Default: 1
        /// </summary>
        /// <param name="value">The access hint integer</param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions SetAccessHintOnCompactionStart(int value)
        {
            dbOptions.SetAccessHintOnCompactionStart(value);
            return this;
        }
        
        /// <summary>
        /// If set true, will hint the underlying file system that the file access pattern is random, when a sst file is opened.
        /// Default: true
        /// </summary>
        /// <param name="value">true if hinting random access is on</param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions SetAdviseRandomOnOpen(bool value)
        {
            dbOptions.SetAdviseRandomOnOpen(value);
            return this;
        }
        
        /// <summary>
        /// If true, allow multi-writers to update mem tables in parallel.
        /// Only some memtable factorys support concurrent writes; currently it is implemented only for SkipListFactory.
        /// Concurrent memtable writes are not compatible with inplace_update_support or filter_deletes.
        /// It is strongly recommended to set <see cref="SetEnableWriteThreadAdaptiveYield"/> if you are going to use this feature.
        /// Default: true
        /// </summary>
        /// <param name="value">true to enable concurrent writes for the memtable</param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions SetAllowConcurrentMemtableWrite(bool value)
        {
            dbOptions.SetAllowConcurrentMemtableWrite(value);
            return this;
        }
        
        /// <summary>
        /// Allow the OS to mmap file for reading sst tables.
        /// Default: false
        /// </summary>
        /// <param name="value"> true if mmap reads are allowed</param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions SetAllowMmapReads(bool value)
        {
            dbOptions.SetAllowMmapReads(value);
            return this;
        }
        
        /// <summary>
        /// Allow the OS to mmap file for writing. Default: false
        /// </summary>
        /// <param name="value">true if mmap writes are allowed</param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions SetAllowMmapWrites(bool value)
        {
            dbOptions.SetAllowMmapWrites(value);
            return this;
        }

        /// <summary>
        /// Allows OS to incrementally sync files to disk while they are being written, asynchronously, in the background.
        /// Issue one request for every bytes_per_sync written. 0 turns it off.
        /// Default: 0
        /// </summary>
        /// <param name="value">size in bytes</param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions SetBytesPerSync(ulong value)
        {
            dbOptions.SetBytesPerSync(value);
            return this;
        }
        
        /// <summary>
        /// If this value is set to true, then the database will be created if it is missing during RocksDB.open().
        /// Default: false
        /// </summary>
        /// <param name="value">a flag indicating whether to create a database the specified database in <see cref="RocksDbKeyValueStore.OpenDatabase"/> operation is missing.</param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions SetCreateIfMissing(bool value = true)
        {
            dbOptions.SetCreateIfMissing(value);
            return this;
        }
        
        /// <summary>
        /// If true, missing column families will be automatically created
        /// Default: false
        /// </summary>
        /// <param name="value">a flag indicating if missing column families shall be created automatically</param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions SetCreateMissingColumnFamilies(bool value = true)
        {
            dbOptions.SetCreateMissingColumnFamilies(value);
            return this;
        }
        
        /// <summary>
        /// This specifies the info LOG dir.
        /// If it is empty, the log files will be in the same dir as data.
        /// If it is non empty, the log files will be in the specified dir, and the db data dir's absolute path will be used as the log file name's prefix.
        /// </summary>
        /// <param name="value">the path to the info log directory</param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions SetDbLogDir(string value)
        {
            dbOptions.SetDbLogDir(value);
            return this;
        }
        
        /// <summary>
        /// Amount of data to build up in memtables across all column families before writing to disk.
        /// This is distinct from ColumnFamilyOptions.writeBufferSize(), which enforces a limit for a single memtable.
        /// This feature is disabled by default. Specify a non-zero value to enable it. Default: 0 (disabled)
        /// </summary>
        /// <param name="size">the size of the write buffer</param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions SetDbWriteBufferSize(ulong size)
        {
            dbOptions.SetDbWriteBufferSize(size);
            return this;
        }
        
        /// <summary>
        /// The periodicity when obsolete files get deleted.
        /// The default value is 6 hours.
        /// The files that get out of scope by compaction process will still get automatically delete on every compaction, regardless of this setting
        /// </summary>
        /// <param name="value">the time interval in micros</param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions SetDeleteObsoleteFilesPeriodMicros(ulong value)
        {
            dbOptions.SetDeleteObsoleteFilesPeriodMicros(value);
            return this;
        }
        
        /// <summary>
        /// If true, threads synchronizing with the write batch group leader will wait before blocking on a mutex.
        /// This can substantially improve throughput for concurrent workloads, regardless of whether <see cref="SetAllowConcurrentMemtableWrite"/> is enabled.
        /// Default: true
        /// </summary>
        /// <param name="value">true to enable adaptive yield for the write threads</param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions SetEnableWriteThreadAdaptiveYield(bool value)
        {
            dbOptions.SetEnableWriteThreadAdaptiveYield(value);
            return this;
        }
        
        /// <summary>
        /// Use the specified object to interact with the environment, e.g. to read/write files, schedule background work, etc
        /// </summary>
        /// <param name="env">Pointer to <see cref="RocksDbSharp.Env"/> instance</param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions SetEnv(IntPtr env)
        {
            dbOptions.SetEnv(env);
            return this;
        }
        
        /// <summary>
        /// If true, an error will be thrown during <see cref="RocksDbKeyValueStore.OpenDatabase"/> if the database already exists.
        /// Default: false
        /// </summary>
        /// <param name="value">if true, an exception will be thrown during open if the database already exists.</param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions SetErrorIfExists(bool value = true)
        {
            dbOptions.SetErrorIfExists(value);
            return this;
        }
        
        /// <summary>
        /// Any internal progress/error information generated by the db will be written to the Logger if it is non-nullptr, or to a file stored in the same directory as the DB contents if info_log is nullptr.
        /// </summary>
        /// <param name="logger">Pointer to logger instance</param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions SetInfoLog(IntPtr logger)
        {
            dbOptions.SetInfoLog(logger);
            return this;
        }
        
        /// <summary>
        /// Disable child process inherit open files. Default: true
        /// </summary>
        /// <param name="value">true if child process inheriting open files is disabled</param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions SetIsFdCloseOnExec(bool value)
        {
            dbOptions.SetIsFdCloseOnExec(value);
            return this;
        }
        
        /// <summary>
        /// Specifies the maximum number of info log files to be kept.
        /// Default: 1000
        /// </summary>
        /// <param name="value">the maximum number of info log files to be kept</param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions SetKeepLogFileNum(ulong value)
        {
            dbOptions.SetKeepLogFileNum(value);
            return this;
        }
        
        /// <summary>
        /// Specifies the time interval for the info log file to roll (in seconds). If specified with non-zero value, log file will be rolled if it has been active longer than `log_file_time_to_roll`.
        /// Default: 0 (disabled)
        /// </summary>
        /// <param name="value">the time interval in seconds</param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions SetLogFileTimeToRoll(ulong value)
        {
            dbOptions.SetLogFileTimeToRoll(value);
            return this;
        }
        
        /// <summary>
        /// Number of bytes to preallocate (via fallocate) the manifest files. Default is 4mb, which is reasonable to reduce random IO as well as prevent overallocation for mounts that preallocate large amounts of data (such as xfs's allocsize option)
        /// </summary>
        /// <param name="value">the size in byte</param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions SetManifestPreallocationSize(ulong value)
        {
            dbOptions.SetManifestPreallocationSize(value);
            return this;
        }
        
        /// <summary>
        /// NOT SUPPORTED ANYMORE. Will remove in future release.
        /// </summary>
        /// <param name="value"></param>
        /// <returns>the instance of the current object</returns>
        [Obsolete]
        public RocksDbOptions SetMaxBackgroundCompactions(int value)
        {
            dbOptions.SetMaxBackgroundCompactions(value);
            return this;
        }
        
        /// <summary>
        /// NOT SUPPORTED ANYMORE. Will remove in future release.
        /// </summary>
        /// <param name="value"></param>
        /// <returns>the instance of the current object</returns>
        [Obsolete]
        public RocksDbOptions SetMaxBackgroundFlushes(int value)
        {
            dbOptions.SetMaxBackgroundFlushes(value);
            return this;
        }
        
        /// <summary>
        /// If <see cref="SetMaxOpenFiles"/> is -1, DB will open all files on DB::Open().
        /// You can use this option to increase the number of threads used to open the files.
        /// Default: 16
        /// </summary>
        /// <param name="value">the maximum number of threads to use to open files</param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions SetMaxFileOpeningThreads(int value)
        {
            dbOptions.SetMaxFileOpeningThreads(value);
            return this;
        }
        
        /// <summary>
        /// Specifies the maximum size of a info log file.
        /// If the current log file is larger than `max_log_file_size`, a new info log file will be created.
        /// If 0, all logs will be written to one log file.
        /// </summary>
        /// <param name="value">the maximum size of a info log file</param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions SetMaxLogFileSize(ulong value)
        {
            dbOptions.SetMaxLogFileSize(value);
            return this;
        }
        
        /// <summary>
        /// Manifest file is rolled over on reaching this limit.
        /// The older manifest file be deleted.
        /// The default value is 1GB so that the manifest file can grow, but not reach the limit of storage capacity.
        /// </summary>
        /// <param name="value">the size limit of a manifest file</param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions SetMaxManifestFileSize(ulong value)
        {
            dbOptions.SetMaxManifestFileSize(value);
            return this;
        }
        
        /// <summary>
        /// Number of open files that can be used by the DB.
        /// You may need to increase this if your database has a large working set.
        /// Value -1 means files opened are always kept open.
        /// You can estimate number of files based on target_file_size_base and target_file_size_multiplier for level-based compaction.
        /// For universal-style compaction, you can usually set it to -1. Default: -1
        /// </summary>
        /// <param name="value">the maximum number of open files</param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions SetMaxOpenFiles(int value)
        {
            dbOptions.SetMaxOpenFiles(value);
            return this;
        }
        
        /// <summary>
        /// Once write-ahead logs exceed this size, we will start forcing the flush of column families whose memtables are backed by the oldest live WAL file (i.e. the ones that are causing all the space amplification).
        /// If set to 0 (default), we will dynamically choose the WAL size limit to be [sum of all write_buffer_size * max_write_buffer_number] * 2
        /// This option takes effect only when there are more than one column family as otherwise the wal size is dictated by the write_buffer_size.
        /// Default: 0
        /// </summary>
        /// <param name="n">max total wal size</param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions SetMaxTotalWalSize(ulong n)
        {
            dbOptions.SetMaxTotalWalSize(n);
            return this;
        }
        
        /// <summary>
        /// If true, the implementation will do aggressive checking of the data it is processing and will stop early if it detects any errors.
        /// This may have unforeseen ramifications: for example, a corruption of one DB entry may cause a large number of entries to become unreadable or for the entire DB to become unopenable.
        /// If any of the writes to the database fails (Put, Delete, Merge, Write), the database will switch to read-only mode and fail all other Write operations.
        /// Default: true
        /// </summary>
        /// <param name="value">a flag to indicate whether paranoid-check is on</param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions SetParanoidChecks(bool value = true)
        {
            dbOptions.SetParanoidChecks(value);
            return this;
        }
        
        /// <summary>
        /// Recycle log files.
        /// If non-zero, we will reuse previously written log files for new logs, overwriting the old data.
        /// The value indicates how many such files we will keep around at any point in time for later use.
        /// This is more efficient because the blocks are already allocated and fdatasync does not need to update the inode after each write.
        /// Default: 0
        /// </summary>
        /// <param name="value">the number of log files to keep for recycling</param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions SetRecycleLogFileNum(ulong value)
        {
            dbOptions.SetRecycleLogFileNum(value);
            return this;
        }
        
        /// <summary>
        /// if not zero, dump rocksdb.stats to LOG every stats_dump_period_sec
        /// Default: 600 (10 minutes)
        /// </summary>
        /// <param name="value">time interval in seconds</param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions SetStatsDumpPeriodSec(uint value)
        {
            dbOptions.SetStatsDumpPeriodSec(value);
            return this;
        }
        
        /// <summary>
        /// Number of shards used for table cache
        /// </summary>
        /// <param name="value">the number of chards</param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions SetTableCacheNumShardbits(int value)
        {
            dbOptions.SetTableCacheNumShardbits(value);
            return this;
        }

        /// <summary>
        /// Use adaptive mutex, which spins in the user space before resorting to kernel.
        /// This could reduce context switch when the mutex is not heavily contended.
        /// However, if the mutex is hot, we could end up wasting spin time.
        /// Default: false
        /// </summary>
        /// <param name="value">true if adaptive mutex is used</param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions SetUseAdaptiveMutex(bool value)
        {
            dbOptions.SetUseAdaptiveMutex(value);
            return this;
        }
        
        /// <summary>
        /// Enable the OS to use direct reads and writes in flush and compaction.
        /// Default: false
        /// </summary>
        /// <param name="value">if true, then direct I/O will be enabled for background flush and compactions</param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions SetUseDirectIoForFlushAndCompaction(bool value)
        {
            dbOptions.SetUseDirectIoForFlushAndCompaction(value);
            return this;
        }
        
        /// <summary>
        /// Enable the OS to use direct I/O for reading sst tables.
        /// Default: false
        /// </summary>
        /// <param name="value">if true, then direct read is enabled</param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions SetUseDirectReads(bool value)
        {
            dbOptions.SetUseDirectReads(value);
            return this;
        }
        
        /// <summary>
        /// If true, then every store to stable storage will issue a fsync.
        /// If false, then every store to stable storage will issue a fdatasync.
        /// This parameter should be set to true while storing data to filesystem like ext3 that can lose files after a reboot.
        /// Default: false
        /// </summary>
        /// <param name="value">a boolean flag to specify whether to use fsync</param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions SetUseFsync(int value)
        {
            dbOptions.SetUseFsync(value);
            return this;
        }
        
        /// <summary>
        /// This specifies the absolute dir path for write-ahead logs (WAL).
        /// If it is empty, the log files will be in the same dir as data, dbname is used as the data dir by default If it is non empty, the log files will be in kept the specified dir.
        /// When destroying the db, all log files in wal_dir and the dir itself is deleted
        /// </summary>
        /// <param name="value">the path to the write-ahead-log directory</param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions SetWalDir(string value)
        {
            dbOptions.SetWalDir(value);
            return this;
        }
        
        /// <summary>
        /// Recovery mode to control the consistency while replaying WAL.
        /// </summary>
        /// <param name="mode">The WAL recover mode</param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions SetWalRecoveryMode(Recovery mode)
        {
            dbOptions.SetWalRecoveryMode(mode);
            return this;
        }
        
        /// <summary>
        /// <see cref="SetWALTtlSeconds"/> and <see cref="SetWALSizeLimitMB"/> affect how archived logs will be deleted.
        /// If both set to 0, logs will be deleted asap and will not get into the archive.
        ///    If WAL_ttl_seconds is 0 and WAL_size_limit_MB is not 0, WAL files will be checked every 10 min and if total size is greater then WAL_size_limit_MB, they will be deleted starting with the earliest until size_limit is met. All empty files will be deleted.
        ///    If WAL_ttl_seconds is not 0 and WAL_size_limit_MB is 0, then WAL files will be checked every WAL_ttl_secondsi / 2 and those that are older than WAL_ttl_seconds will be deleted.
        ///    If both are not 0, WAL files will be checked every 10 min and both checks will be performed with ttl being first.
        /// </summary>
        /// <param name="value">size limit in mega-bytes</param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions SetWALSizeLimitMB(ulong value)
        {
            dbOptions.SetWALSizeLimitMB(value);
            return this;
        }
        
        /// <summary>
        /// <see cref="SetWALTtlSeconds"/> and <see cref="SetWALSizeLimitMB"/> affect how archived logs will be deleted.
        /// If both set to 0, logs will be deleted asap and will not get into the archive.
        ///    If WAL_ttl_seconds is 0 and WAL_size_limit_MB is not 0, WAL files will be checked every 10 min and if total size is greater then WAL_size_limit_MB, they will be deleted starting with the earliest until size_limit is met. All empty files will be deleted.
        ///    If WAL_ttl_seconds is not 0 and WAL_size_limit_MB is 0, then WAL files will be checked every WAL_ttl_secondsi / 2 and those that are older than WAL_ttl_seconds will be deleted.
        ///    If both are not 0, WAL files will be checked every 10 min and both checks will be performed with ttl being first.
        /// </summary>
        /// <param name="value">the ttl seconds</param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions SetWALTtlSeconds(ulong value)
        {
            dbOptions.SetWALTtlSeconds(value);
            return this;
        }
        
        /// <summary>
        /// If true, then <see cref="RocksDbKeyValueStore.OpenDatabase"/> will not update the statistics used to optimize compaction decision by loading table properties from many files.
        /// Turning off this feature will improve DBOpen time especially in disk environment.
        /// Default: false
        /// </summary>
        /// <param name="val">true if updating stats will be skipped</param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions SkipStatsUpdateOnOpen(bool val = false)
        {
            dbOptions.SkipStatsUpdateOnOpen(val);
            return this;
        }
        #endregion

        #region ColumnFamilyOptions
        
        /// <summary>
        /// Use this if you don't need to keep the data sorted, i.e. you'll never use an iterator, only Put() and Get() API calls.
        /// </summary>
        /// <param name="blockCacheSizeMb">Block cache size in MB</param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions OptimizeForPointLookup(ulong blockCacheSizeMb)
        {
            columnFamilyOptions.OptimizeForPointLookup(blockCacheSizeMb);
            return this;
        }
        
        /// <summary>
        /// Default values for some parameters in ColumnFamilyOptions are not optimized for heavy workloads and big datasets, which means you might observe write stalls under some conditions.
        /// As a starting point for tuning RocksDB options, use the following for level style compaction.
        /// Make sure to also call <see cref="IncreaseParallelism"/>, which will provide the biggest performance gains.
        /// Note: we might use more memory than during high write rate period
        /// </summary>
        /// <param name="memtableMemoryBudget">memtable memory budget used</param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions OptimizeLevelStyleCompaction(ulong memtableMemoryBudget)
        {
            columnFamilyOptions.OptimizeLevelStyleCompaction(memtableMemoryBudget);
            return this;
        }
        
        /// <summary>
        /// Default values for some parameters in ColumnFamilyOptions are not optimized for heavy workloads and big datasets, which means you might observe write stalls under some conditions.
        /// As a starting point for tuning RocksDB options, use the following for universal style compaction.
        /// Universal style compaction is focused on reducing Write Amplification Factor for big data sets, but increases Space Amplification.
        /// Make sure to also call <see cref="IncreaseParallelism"/>, which will provide the biggest performance gains.
        /// Note: we might use more memory than memtable_memory_budget during high write rate period
        /// </summary>
        /// <param name="memtableMemoryBudget">memtable memory budget used</param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions OptimizeUniversalStyleCompaction(ulong memtableMemoryBudget)
        {
            columnFamilyOptions.OptimizeUniversalStyleCompaction(memtableMemoryBudget);
            return this;
        }
        
        /// <summary>
        /// The size of one block in arena memory allocation.
        /// If ≤ 0, a proper value is automatically calculated (usually 1/10 of writer_buffer_size).
        /// There are two additional restriction of the specified size: (1) size should be in the range of [4096, 2 &lt;@lt; 30] and (2) be the multiple of the CPU word (which helps with the memory alignment).
        /// We will automatically check and adjust the size number to make sure it conforms to the restrictions.
        /// Default: 0
        /// </summary>
        /// <param name="value">the size of an arena block</param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions SetArenaBlockSize(ulong value)
        {
            columnFamilyOptions.SetArenaBlockSize(value);
            return this;
        }
        
        /// <summary>
        /// Set the block based table factory with specified options.
        /// </summary>
        /// <param name="table_options">block table options</param>
        /// <returns></returns>
        public RocksDbOptions SetBlockBasedTableFactory(BlockBasedTableOptions table_options)
        {
            columnFamilyOptions.SetBlockBasedTableFactory(table_options);
            return this;
        }
        
        /// <summary>
        /// Control locality of bloom filter probes to improve cache miss rate.
        /// This option only applies to memtable prefix bloom and plaintable prefix bloom.
        /// It essentially limits the max number of cache lines each bloom filter check can touch.
        /// This optimization is turned off when set to 0.
        /// The number should never be greater than number of probes.
        /// This option can boost performance for in-memory workload but should use with care since it can cause higher false positive rate.
        /// Default: 0
        /// </summary>
        /// <param name="value">the level of locality of bloom-filter probes.</param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions SetBloomLocality(uint value)
        {
            columnFamilyOptions.SetBloomLocality(value);
            return this;
        }
        
        /// <summary>
        /// A single CompactionFilter instance to call into during compaction. Allows an application to modify/delete a key-value during background compaction
        /// </summary>
        /// <param name="compactionFilter">Pointer to <see cref="CompactionFilter"/> instance</param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions SetCompactionFilter(IntPtr compactionFilter)
        {
            columnFamilyOptions.SetCompactionFilter(compactionFilter);
            return this;
        }
        
        /// <summary>
        /// This is a factory that provides <see cref="CompactionFilter"/> objects which allow an application to modify/delete a key-value during background compaction.
        /// A new filter will be created on each compaction run.
        /// If multi-threaded compaction is being used, each created CompactionFilter will only be used from a single thread and so does not need to be thread-safe
        /// </summary>
        /// <param name="compactionFilterFactory">compaction filter factory instance</param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions SetCompactionFilterFactory(IntPtr compactionFilterFactory)
        {
            columnFamilyOptions.SetCompactionFilterFactory(compactionFilterFactory);
            return this;
        }
        
        /// <summary>
        /// 
        /// </summary>
        /// <param name="size"></param>
        /// <returns></returns>
        public RocksDbOptions SetCompactionReadaheadSize(ulong size)
        {
            columnFamilyOptions.SetCompactionReadaheadSize(size);
            return this;
        }
        
        /// <summary>
        /// Set compaction style for DB.
        /// Default: <see cref="Compaction.Level"/>
        /// </summary>
        /// <param name="value">Compaction style</param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions SetCompactionStyle(Compaction value)
        {
            columnFamilyOptions.SetCompactionStyle(value);
            return this;
        }
        
        /// <summary>
        /// Use the specified comparator for key ordering.
        /// Comparator should not be disposed before options instances using this comparator is disposed.
        /// If dispose() function is not called, then comparator object will be GC'd automatically.
        /// Comparator instance can be re-used in multiple options instances.
        /// </summary>
        /// <param name="comparator">Pointer to <see cref="Comparator"/> instance</param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions SetComparator(IntPtr comparator)
        {
            columnFamilyOptions.SetComparator(comparator);
            return this;
        }
        
        /// <summary>
        /// Use the specified comparator for key ordering.
        /// Comparator should not be disposed before options instances using this comparator is disposed.
        /// If dispose() function is not called, then comparator object will be GC'd automatically.
        /// Comparator instance can be re-used in multiple options instances.
        /// </summary>
        /// <param name="comparator"><see cref="Comparator"/>'s instance</param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions SetComparator(Comparator comparator)
        {
            columnFamilyOptions.SetComparator(comparator);
            return this;
        }
        
        /// <summary>
        /// Set the different options for compression algorithms
        /// </summary>
        /// <param name="value">compression options</param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions SetCompression(Compression value)
        {
            columnFamilyOptions.SetCompression(value);
            return this;
        }
        
        /// <summary>
        /// Set the different options for compression algorithms
        /// </summary>
        /// <param name="p1"></param>
        /// <param name="p2"></param>
        /// <param name="p3"></param>
        /// <param name="p4"></param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions SetCompressionOptions(int p1, int p2, int p3, int p4)
        {
            columnFamilyOptions.SetCompressionOptions(p1, p2, p3, p4);
            return this;
        }
        
        /// <summary>
        /// Different levels can have different compression policies. There are cases where most lower levels would like to use quick compression algorithms while the higher levels (which have more data) use compression algorithms that have better compression but could be slower. This array, if non-empty, should have an entry for each level of the database; these override the value specified in the previous field 'compression'.
        /// <para>
        /// NOTICE
        ///     If level_compaction_dynamic_level_bytes=true, compression_per_level[0] still determines L0, but other elements of the array are based on base level (the level L0 files are merged to), and may not match the level users see from info log for metadata.
        ///     If L0 files are merged to level - n, then, for i&gt;0, compression_per_level[i] determines compaction type for level n+i-1.
        /// </para>
        /// <para>
        /// Example
        ///     For example, if we have 5 levels, and we determine to merge L0 data to L4 (which means L1..L3 will be empty), then the new files go to L4 uses compression type compression_per_level[1].
        ///     If now L0 is merged to L2. Data goes to L2 will be compressed according to compression_per_level[1], L3 using compression_per_level[2]and L4 using compression_per_level[3]. Compaction for each level can change when data grows.
        /// </para>
        /// </summary>
        /// <param name="levelValues">list of compression</param>
        /// <param name="numLevels">size of list</param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions SetCompressionPerLevel(Compression[] levelValues, ulong numLevels)
        {
            columnFamilyOptions.SetCompressionPerLevel(levelValues, numLevels);
            return this;
        }
        
        /// <summary>
        /// 
        /// </summary>
        /// <param name="levelValues"></param>
        /// <param name="numLevels"></param>
        /// <returns>the instance of the current object</returns>
        [Obsolete]
        public RocksDbOptions SetCompressionPerLevel(Compression[] levelValues, UIntPtr numLevels)
        {
            columnFamilyOptions.SetCompressionPerLevel(levelValues, numLevels);
            return this;
        }
        
        /// <summary>
        /// Disable automatic compactions. Manual compactions can still be issued on this column family.
        /// </summary>
        /// <param name="value">true if auto-compactions are disabled</param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions SetDisableAutoCompactions(int value)
        {
            columnFamilyOptions.SetDisableAutoCompactions(value);
            return this;
        }
        
        /// <summary>
        /// The options for FIFO compaction style
        /// </summary>
        /// <param name="fifoCompactionOptions">The FIFO compaction options</param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions SetFifoCompactionOptions(IntPtr fifoCompactionOptions)
        {
            columnFamilyOptions.SetFifoCompactionOptions(fifoCompactionOptions);
            return this;
        }
        
        /// <summary>
        /// All writes are stopped if estimated bytes needed to be compaction exceed this threshold.
        /// Default: 256GB
        /// </summary>
        /// <param name="value">The hard limit to impose on compaction</param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions SetHardPendingCompactionBytesLimit(ulong value)
        {
            columnFamilyOptions.SetHardPendingCompactionBytesLimit(value);
            return this;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions SetHashLinkListRep(ulong value)
        {
            columnFamilyOptions.SetHashLinkListRep(value);
            return this;
        }
        
        /// <summary>
        /// 
        /// </summary>
        /// <param name="p1"></param>
        /// <param name="p2"></param>
        /// <param name="p3"></param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions SetHashSkipListRep(ulong p1, int p2, int p3)
        {
            columnFamilyOptions.SetHashSkipListRep(p1, p2, p3);
            return this;
        }
        
        /// <summary>
        /// Sets the RocksDB log level.
        /// Default level is <see cref="RocksLogLevel.INFO"/>
        /// </summary>
        /// <param name="value">log level to set</param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions SetInfoLogLevel(InfoLogLevel value)
        {
            columnFamilyOptions.SetInfoLogLevel(value);
            return this;
        }
        
        /// <summary>
        /// Number of locks used for inplace update Default: 10000, if inplace_update_support = true, else 0.
        /// </summary>
        /// <param name="value">the number of locks used for inplace updates</param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions SetInplaceUpdateNumLocks(ulong value)
        {
            columnFamilyOptions.SetInplaceUpdateNumLocks(value);
            return this;
        }
        
        /// <summary>
        /// Allows thread-safe inplace updates.
        /// If inplace_callback function is not set, Put(key, new_value) will update inplace the existing_value iff * key exists in current memtable * new sizeof(new_value) ≤ sizeof(existing_value) * existing_value for that key is a put i.e. kTypeValue If inplace_callback function is set, check doc for inplace_callback.
        /// Default: false.
        /// </summary>
        /// <param name="value">true if thread-safe inplace updates are allowed</param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions SetInplaceUpdateSupport(bool value)
        {
            columnFamilyOptions.SetInplaceUpdateSupport(value);
            return this;
        }
        
        /// <summary>
        /// Number of files to trigger level-0 compaction.
        /// A value &lt; 0 means that level-0 compaction will not be triggered by number of files at all.
        /// Default: 4
        /// </summary>
        /// <param name="value">The number of files to trigger level-0 compaction</param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions SetLevel0FileNumCompactionTrigger(int value)
        {
            columnFamilyOptions.SetLevel0FileNumCompactionTrigger(value);
            return this;
        }
        
        /// <summary>
        /// Soft limit on number of level-0 files. We start slowing down writes at this point.
        /// A value &lt; 0 means that no writing slow down will be triggered by number of files in level-0
        /// </summary>
        /// <param name="value">The soft limit on the number of level-0 files</param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions SetLevel0SlowdownWritesTrigger(int value)
        {
            columnFamilyOptions.SetLevel0SlowdownWritesTrigger(value);
            return this;
        }
        
        /// <summary>
        /// Maximum number of level-0 files. We stop writes at this point.
        /// </summary>
        /// <param name="value">The maximum number of level-0 files</param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions SetLevel0StopWritesTrigger(int value)
        {
            columnFamilyOptions.SetLevel0StopWritesTrigger(value);
            return this;
        }
        
        /// <summary>
        /// If true, RocksDB will pick target size of each level dynamically. We will pick a base level b >= 1. L0 will be directly merged into level b, instead of always into level 1. Level 1 to b-1 need to be empty. We try to pick b and its target size so that
        /// target size is in the range of (max_bytes_for_level_base / max_bytes_for_level_multiplier, max_bytes_for_level_base]
        /// target size of the last level (level num_levels-1) equals to extra size of the level.
        /// At the same time max_bytes_for_level_multiplier and max_bytes_for_level_multiplier_additional are still satisfied.
        /// With this option on, from an empty DB, we make last level the base level, which means merging L0 data into the last level, until it exceeds max_bytes_for_level_base. And then we make the second last level to be base level, to start to merge L0 data to second last level, with its target size to be 1/max_bytes_for_level_multiplier of the last levels extra size. After the data accumulates more so that we need to move the base level to the third last one, and so on.
        /// <para>
        /// Example
        /// For example, assume max_bytes_for_level_multiplier=10, num_levels=6, and max_bytes_for_level_base=10MB.
        /// Target sizes of level 1 to 5 starts with:
        /// [- - - - 10MB]
        /// with base level is level. Target sizes of level 1 to 4 are not applicable because they will not be used. Until the size of Level 5 grows to more than 10MB, say 11MB, we make base target to level 4 and now the targets looks like:
        /// [- - - 1.1MB 11MB]
        /// While data are accumulated, size targets are tuned based on actual data of level 5. When level 5 has 50MB of data, the target is like:
        /// [- - - 5MB 50MB]
        /// Until level 5's actual size is more than 100MB, say 101MB. Now if we keep level 4 to be the base level, its target size needs to be 10.1MB, which doesn't satisfy the target size range. So now we make level 3 the target size and the target sizes of the levels look like:
        /// [- - 1.01MB 10.1MB 101MB]
        /// In the same way, while level 5 further grows, all levels' targets grow, like
        /// [- - 5MB 50MB 500MB]
        /// Until level 5 exceeds 1000MB and becomes 1001MB, we make level 2 the base level and make levels' target sizes like this:
        /// [- 1.001MB 10.01MB 100.1MB 1001MB]
        /// and go on...
        /// By doing it, we give max_bytes_for_level_multiplier a priority against max_bytes_for_level_base, for a more predictable LSM tree shape. It is useful to limit worse case space amplification.
        /// max_bytes_for_level_multiplier_additional is ignored with this flag on.
        /// Turning this feature on or off for an existing DB can cause unexpected LSM tree structure so it's not recommended.
        /// </para>
        /// </summary>
        /// <param name="value">boolean value indicating if LevelCompactionDynamicLevelBytes shall be enabled</param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions SetLevelCompactionDynamicLevelBytes(bool value)
        {
            columnFamilyOptions.SetLevelCompactionDynamicLevelBytes(value);
            return this;
        }
        
        /// <summary>
        /// The upper-bound of the total size of level-1 files in bytes.
        /// Maximum number of bytes for level L can be calculated as (maxBytesForLevelBase) * (maxBytesForLevelMultiplier ^ (L-1)) For example, if maxBytesForLevelBase is 20MB, and if max_bytes_for_level_multiplier is 10, total data size for level-1 will be 200MB, total file size for level-2 will be 2GB, and total file size for level-3 will be 20GB. by default 'maxBytesForLevelBase' is 256MB.
        /// </summary>
        /// <param name="value">maximum bytes for level base</param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions SetMaxBytesForLevelBase(ulong value)
        {
            columnFamilyOptions.SetMaxBytesForLevelBase(value);
            return this;
        }
        
        /// <summary>
        /// The ratio between the total size of level-(L+1) files and the total size of level-L files for all L.
        /// Default: 10
        /// </summary>
        /// <param name="value">the ratio between the total size of level-(L+1) files and the total size of level-L files for all L</param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions SetMaxBytesForLevelMultiplier(double value)
        {
            columnFamilyOptions.SetMaxBytesForLevelMultiplier(value);
            return this;
        }
        
        /// <summary>
        /// Different max-size multipliers for different levels.
        /// These are multiplied by max_bytes_for_level_multiplier to arrive at the max-size of each level.
        /// Default: 1
        /// </summary>
        /// <param name="levelValues">The max-size multipliers for each level</param>
        /// <param name="numLevels">num levels</param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions SetMaxBytesForLevelMultiplierAdditional(int[] levelValues, ulong numLevels)
        {
            columnFamilyOptions.SetMaxBytesForLevelMultiplierAdditional(levelValues, numLevels);
            return this;
        }
        
        /// <summary>
        /// We try to limit number of bytes in one compaction to be lower than this threshold.
        /// But it's not guaranteed.
        /// Value 0 will be sanitized.
        /// </summary>
        /// <param name="bytes">max bytes in a compaction</param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions SetMaxCompactionBytes(ulong bytes)
        {
            columnFamilyOptions.SetMaxCompactionBytes(bytes);
            return this;
        }

        /// <summary>
        /// An iteration->Next() sequentially skips over keys with the same user-key unless this option is set.
        /// This number specifies the number of keys (with the same userkey) that will be sequentially skipped before a reseek is issued.
        /// Default: 8
        /// </summary>
        /// <param name="value">the number of keys could be skipped in a iteration</param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions SetMaxSequentialSkipInIterations(ulong value)
        {
            columnFamilyOptions.SetMaxSequentialSkipInIterations(value);
            return this;
        }
        
        /// <summary>
        /// Maximum number of successive merge operations on a key in the memtable.
        /// When a merge operation is added to the memtable and the maximum number of successive merges is reached, the value of the key will be calculated and inserted into the memtable instead of the merge operation.
        /// This will ensure that there are never more than max_successive_merges merge operations in the memtable.
        /// Default: 0 (disabled)
        /// </summary>
        /// <param name="value">the maximum number of successive merges</param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions SetMaxSuccessiveMerges(ulong value)
        {
            columnFamilyOptions.SetMaxSuccessiveMerges(value);
            return this;
        }
        
        /// <summary>
        /// The maximum number of write buffers that are built up in memory.
        /// The default is 2, so that when 1 write buffer is being flushed to storage, new writes can continue to the other write buffer.
        /// Default: 2
        /// </summary>
        /// <param name="value">maximum number of write buffers</param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions SetMaxWriteBufferNumber(int value)
        {
            columnFamilyOptions.SetMaxWriteBufferNumber(value);
            return this;
        }
        
        /// <summary>
        /// The total maximum number of write buffers to maintain in memory including copies of buffers that have already been flushed.
        /// </summary>
        /// <param name="value">The maximum number of write buffers to maintain</param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions SetMaxWriteBufferNumberToMaintain(int value)
        {
            columnFamilyOptions.SetMaxWriteBufferNumberToMaintain(value);
            return this;
        }
        
        /// <summary>
        /// Page size for huge page TLB for bloom in memtable.
        /// If ≤ 0, not allocate from huge page TLB but from malloc.
        /// Need to reserve huge pages for it to be allocated.
        /// For example: sysctl -w vm.nr_hugepages=20 See linux doc Documentation/vm/hugetlbpage.txt
        /// </summary>
        /// <param name="size">The page size of the huge page tlb</param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions SetMemtableHugePageSize(ulong size)
        {
            columnFamilyOptions.SetMemtableHugePageSize(size);
            return this;
        }
        
        /// <summary>
        /// If prefix_extractor is set and memtable_prefix_bloom_size_ratio is not 0, create prefix bloom for memtable with the size of write_buffer_size * memtable_prefix_bloom_size_ratio.
        /// If it is larger than 0.25, it is santinized to 0.25.
        /// Default: 0 (disable)
        /// </summary>
        /// <param name="ratio">The ratio</param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions SetMemtablePrefixBloomSizeRatio(double ratio)
        {
            columnFamilyOptions.SetMemtablePrefixBloomSizeRatio(ratio);
            return this;
        }
        
        /// <summary>
        /// 
        /// </summary>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions SetMemtableVectorRep()
        {
            columnFamilyOptions.SetMemtableVectorRep();
            return this;
        }
        
        /// <summary>
        /// Set the merge operator to be used for merging two different key/value pairs that share the same key.
        /// The merge function is invoked during compaction and at lookup time, if multiple key/value pairs belonging to the same key are found in the database.
        /// </summary>
        /// <param name="mergeOperator">Pointer to <see cref="MergeOperator"/> instance</param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions SetMergeOperator(IntPtr mergeOperator)
        {
            columnFamilyOptions.SetMergeOperator(mergeOperator);
            return this;
        }
        
        /// <summary>
        /// Set the merge operator to be used for merging two different key/value pairs that share the same key.
        /// The merge function is invoked during compaction and at lookup time, if multiple key/value pairs belonging to the same key are found in the database.
        /// </summary>
        /// <param name="mergeOperator"><see cref="MergeOperator"/> instance</param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions SetMergeOperator(MergeOperator mergeOperator)
        {
            columnFamilyOptions.SetMergeOperator(mergeOperator);
            return this;
        }
        
        /// <summary>
        /// 
        /// </summary>
        /// <param name="level"></param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions SetMinLevelToCompress(int level)
        {
            columnFamilyOptions.SetMinLevelToCompress(level);
            return this;
        }
        
        /// <summary>
        /// The minimum number of write buffers that will be merged together before writing to storage.
        /// If set to 1, then all write buffers are flushed to L0 as individual files and this increases read amplification because a get request has to check in all of these files.
        /// Also, an in-memory merge may result in writing lesser data to storage if there are duplicate records in each of these individual write buffers.
        /// Default: 1
        /// </summary>
        /// <param name="value">the minimum number of write buffers that will be merged together</param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions SetMinWriteBufferNumberToMerge(int value)
        {
            columnFamilyOptions.SetMinWriteBufferNumberToMerge(value);
            return this;
        }
        
        /// <summary>
        /// Set the number of levels for this database If level-styled compaction is used, then this number determines the total number of levels.
        /// </summary>
        /// <param name="value">the number of levels</param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions SetNumLevels(int value)
        {
            columnFamilyOptions.SetNumLevels(value);
            return this;
        }
        
        /// <summary>
        /// Set the state of the optimize_filters_for_hits.
        /// </summary>
        /// <param name="value">true if optimize filter, false otherwise</param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions SetOptimizeFiltersForHits(int value)
        {
            columnFamilyOptions.SetOptimizeFiltersForHits(value);
            return this;
        }
        
        /// <summary>
        /// 
        /// </summary>
        /// <param name="p1"></param>
        /// <param name="p2"></param>
        /// <param name="p3"></param>
        /// <param name="p4"></param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions SetPlainTableFactory(uint user_key_len, int bloom_bits_per_key, double hash_table_ratio, int index_sparseness, int huge_page_tlb_size, char encoding_type, bool full_scan_mode, bool store_index_in_file)
        {
            columnFamilyOptions.SetPlainTableFactory(user_key_len, bloom_bits_per_key, hash_table_ratio, index_sparseness, huge_page_tlb_size, encoding_type, full_scan_mode, store_index_in_file);
            return this;
        }
        
        /// <summary>
        /// 
        /// </summary>
        /// <param name="sliceTransform"></param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions SetPrefixExtractor(SliceTransform sliceTransform)
        {
            columnFamilyOptions.SetPrefixExtractor(sliceTransform);
            return this;
        }
        
        /// <summary>
        /// 
        /// </summary>
        /// <param name="sliceTransform"></param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions SetPrefixExtractor(IntPtr sliceTransform)
        {
            columnFamilyOptions.SetPrefixExtractor(sliceTransform);
            return this;
        }
        
        
        /// <summary>
        /// Measure IO stats in compactions and flushes, if true.
        /// Default: false
        /// </summary>
        /// <param name="value">true to enable reporting</param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions SetReportBgIoStats(bool value)
        {
            columnFamilyOptions.SetReportBgIoStats(value);
            return this;
        }

        /// <summary>
        /// All writes will be slowed down to at least delayed_write_rate if estimated bytes needed to be compaction exceed this threshold.
        /// Default: 64GB
        /// </summary>
        /// <param name="value">The soft limit to impose on compaction</param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions SetSoftPendingCompactionBytesLimit(ulong value)
        {
            columnFamilyOptions.SetSoftPendingCompactionBytesLimit(value);
            return this;
        }

        /// <summary>
        /// The target file size for compaction. This targetFileSizeBase determines a level-1 file size. Target file size for level L can be calculated by targetFileSizeBase * (targetFileSizeMultiplier ^ (L-1)) For example, if targetFileSizeBase is 2MB and target_file_size_multiplier is 10, then each file on level-1 will be 2MB, and each file on level 2 will be 20MB, and each file on level-3 will be 200MB. by default targetFileSizeBase is 64MB
        /// </summary>
        /// <param name="value">the target size of a level-0 file</param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions SetTargetFileSizeBase(ulong value)
        {
            columnFamilyOptions.SetTargetFileSizeBase(value);
            return this;
        }
        
        /// <summary>
        /// targetFileSizeMultiplier defines the size ratio between a level-L file and level-(L+1) file. By default target_file_size_multiplier is 1, meaning files in different levels have the same target
        /// </summary>
        /// <param name="value">the size ratio between a level-(L+1) file and level-L file</param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions SetTargetFileSizeMultiplier(int value)
        {
            columnFamilyOptions.SetTargetFileSizeMultiplier(value);
            return this;
        }
        
        /// <summary>
        /// 
        /// </summary>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions SetUint64addMergeOperator()
        {
            columnFamilyOptions.SetUint64addMergeOperator();
            return this;
        }
        
        /// <summary>
        /// Set the options needed to support Universal Style compactions
        /// </summary>
        /// <param name="universalCompactionOptions"></param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions SetUniversalCompactionOptions(IntPtr universalCompactionOptions)
        {
            columnFamilyOptions.SetUniversalCompactionOptions(universalCompactionOptions);
            return this;
        }
        
        /// <summary>
        /// Amount of data to build up in memory (backed by an unsorted log on disk) before converting to a sorted on-disk file.
        /// Larger values increase performance, especially during bulk loads.
        /// Up to max_write_buffer_number write buffers may be held in memory at the same time, so you may wish to adjust this parameter to control memory usage.
        /// Also, a larger write buffer will result in a longer recovery time the next time the database is opened.
        /// Default: 64MB
        /// </summary>
        /// <param name="value">the size of write buffer</param>
        /// <returns>the instance of the current object</returns>
        public RocksDbOptions SetWriteBufferSize(ulong value)
        {
            columnFamilyOptions.SetWriteBufferSize(value);
            return this;
        }
        
        #endregion
    }
}