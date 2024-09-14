using System;
using System.Collections.Generic;
using System.Numerics;
using System.Threading;
using Streamiz.Kafka.Net.Processors.Internal;

namespace Streamiz.Kafka.Net.Metrics.Internal
{
    internal class RocksDbMetrics
    {
        #region Constants

        private static String BYTES_WRITTEN_TO_DB = "bytes-written";
        private static String BYTES_READ_FROM_DB = "bytes-read";
        private static String MEMTABLE_BYTES_FLUSHED = "memtable-bytes-flushed";
        private static String MEMTABLE_HIT_RATIO = "memtable-hit" + StreamMetricsRegistry.RATIO_SUFFIX;
        private static String MEMTABLE_FLUSH_TIME = "memtable-flush-time";
        private static String WRITE_STALL_DURATION = "write-stall-duration";
        private static String BLOCK_CACHE_DATA_HIT_RATIO = "block-cache-data-hit" + StreamMetricsRegistry.RATIO_SUFFIX;

        private static String BLOCK_CACHE_INDEX_HIT_RATIO =
            "block-cache-index-hit" + StreamMetricsRegistry.RATIO_SUFFIX;

        private static String BLOCK_CACHE_FILTER_HIT_RATIO =
            "block-cache-filter-hit" + StreamMetricsRegistry.RATIO_SUFFIX;

        private static String BYTES_READ_DURING_COMPACTION = "bytes-read-compaction";
        private static String BYTES_WRITTEN_DURING_COMPACTION = "bytes-written-compaction";
        private static String COMPACTION_TIME = "compaction-time";
        private static String NUMBER_OF_OPEN_FILES = "number-open-files";
        private static String NUMBER_OF_FILE_ERRORS = "number-file-errors";
        
        internal static String NUMBER_OF_ENTRIES_ACTIVE_MEMTABLE = "num-entries-active-mem-table";
        internal static String NUMBER_OF_DELETES_ACTIVE_MEMTABLE = "num-deletes-active-mem-table";
        internal static String NUMBER_OF_ENTRIES_IMMUTABLE_MEMTABLES = "num-entries-imm-mem-tables";
        internal static String NUMBER_OF_DELETES_IMMUTABLE_MEMTABLES = "num-deletes-imm-mem-tables";
        internal static String NUMBER_OF_IMMUTABLE_MEMTABLES = "num-immutable-mem-table";
        internal static String CURRENT_SIZE_OF_ACTIVE_MEMTABLE = "cur-size-active-mem-table";
        internal static String CURRENT_SIZE_OF_ALL_MEMTABLES = "cur-size-all-mem-tables";
        internal static String SIZE_OF_ALL_MEMTABLES = "size-all-mem-tables";
        internal static String MEMTABLE_FLUSH_PENDING = "mem-table-flush-pending";
        internal static String NUMBER_OF_RUNNING_FLUSHES = "num-running-flushes";
        internal static String COMPACTION_PENDING = "compaction-pending";
        internal static String NUMBER_OF_RUNNING_COMPACTIONS = "num-running-compactions";
        internal static String ESTIMATED_BYTES_OF_PENDING_COMPACTION = "estimate-pending-compaction-bytes";
        internal static String TOTAL_SST_FILES_SIZE = "total-sst-files-size";
        internal static String LIVE_SST_FILES_SIZE = "live-sst-files-size";
        internal static String NUMBER_OF_LIVE_VERSIONS = "num-live-versions";
        internal static String CAPACITY_OF_BLOCK_CACHE = "block-cache-capacity";
        internal static String USAGE_OF_BLOCK_CACHE = "block-cache-usage";
        internal static String PINNED_USAGE_OF_BLOCK_CACHE = "block-cache-pinned-usage";
        internal static String ESTIMATED_NUMBER_OF_KEYS = "estimate-num-keys";
        internal static String ESTIMATED_MEMORY_OF_TABLE_READERS = "estimate-table-readers-mem";
        internal static String NUMBER_OF_BACKGROUND_ERRORS = "background-errors";

        private static String BYTES_WRITTEN_TO_DB_DESCRIPTION =
            "Number of bytes written per second to the RocksDB state store";

        private static String BYTES_WRITTEN_TO_DB_RATE_DESCRIPTION =
            "Average number of bytes written per second to the RocksDB state store";

        private static String BYTES_WRITTEN_TO_DB_TOTAL_DESCRIPTION =
            "Total number of bytes written to the RocksDB state store";

        private static String BYTES_READ_FROM_DB_DESCRIPTION =
            "Number of bytes read per second from the RocksDB state store";

        private static String BYTES_READ_FROM_DB_RATE_DESCRIPTION =
            "Average number of bytes read per second from the RocksDB state store";

        private static String BYTES_READ_FROM_DB_TOTAL_DESCRIPTION =
            "Total number of bytes read from the RocksDB state store";

        private static String MEMTABLE_BYTES_FLUSHED_DESCRIPTION =
            "Number of bytes flushed per second from the memtable to disk";

        private static String MEMTABLE_BYTES_FLUSHED_RATE_DESCRIPTION =
            "Average number of bytes flushed per second from the memtable to disk";

        private static String MEMTABLE_BYTES_FLUSHED_TOTAL_DESCRIPTION =
            "Total number of bytes flushed from the memtable to disk";

        private static String MEMTABLE_HIT_RATIO_DESCRIPTION =
            "Ratio of memtable hits relative to all lookups to the memtable";

        private static String MEMTABLE_FLUSH_TIME_AVG_DESCRIPTION =
            "Average time spent on flushing the memtable to disk in ms";

        private static String MEMTABLE_FLUSH_TIME_MIN_DESCRIPTION =
            "Minimum time spent on flushing the memtable to disk in ms";

        private static String MEMTABLE_FLUSH_TIME_MAX_DESCRIPTION =
            "Maximum time spent on flushing the memtable to disk in ms";

        private static String WRITE_STALL_DURATION_DESCRIPTION = "Duration of write stalls in ms";
        private static String WRITE_STALL_DURATION_AVG_DESCRIPTION = "Average duration of write stalls in ms";
        private static String WRITE_STALL_DURATION_TOTAL_DESCRIPTION = "Total duration of write stalls in ms";

        private static String BLOCK_CACHE_DATA_HIT_RATIO_DESCRIPTION =
            "Ratio of block cache hits for data relative to all lookups for data to the block cache";

        private static String BLOCK_CACHE_INDEX_HIT_RATIO_DESCRIPTION =
            "Ratio of block cache hits for indexes relative to all lookups for indexes to the block cache";

        private static String BLOCK_CACHE_FILTER_HIT_RATIO_DESCRIPTION =
            "Ratio of block cache hits for filters relative to all lookups for filters to the block cache";

        private static String BYTES_READ_DURING_COMPACTION_DESCRIPTION =
            "Average number of bytes read per second during compaction";

        private static String BYTES_WRITTEN_DURING_COMPACTION_DESCRIPTION =
            "Average number of bytes written per second during compaction";

        private static String COMPACTION_TIME_AVG_DESCRIPTION = "Average time spent on compaction in ms";
        private static String COMPACTION_TIME_MIN_DESCRIPTION = "Minimum time spent on compaction in ms";
        private static String COMPACTION_TIME_MAX_DESCRIPTION = "Maximum time spent on compaction in ms";
        private static String NUMBER_OF_OPEN_FILES_DESCRIPTION = "Number of currently open files";
        private static String NUMBER_OF_FILE_ERRORS_DESCRIPTION = "Total number of file errors occurred";

        private static String NUMBER_OF_ENTRIES_ACTIVE_MEMTABLE_DESCRIPTION =
            "Total number of entries in the active memtable";

        private static String NUMBER_OF_DELETES_ACTIVE_MEMTABLES_DESCRIPTION =
            "Total number of delete entries in the active memtable";

        private static String NUMBER_OF_ENTRIES_IMMUTABLE_MEMTABLES_DESCRIPTION =
            "Total number of entries in the unflushed immutable memtables";

        private static String NUMBER_OF_DELETES_IMMUTABLE_MEMTABLES_DESCRIPTION =
            "Total number of delete entries in the unflushed immutable memtables";

        private static String NUMBER_OF_IMMUTABLE_MEMTABLES_DESCRIPTION =
            "Number of immutable memtables that have not yet been flushed";

        private static String CURRENT_SIZE_OF_ACTIVE_MEMTABLE_DESCRIPTION =
            "Approximate size of active memtable in bytes";

        private static String CURRENT_SIZE_OF_ALL_MEMTABLES_DESCRIPTION =
            "Approximate size of active and unflushed immutable memtables in bytes";

        private static String SIZE_OF_ALL_MEMTABLES_DESCRIPTION =
            "Approximate size of active, unflushed immutable, and pinned immutable memtables in bytes";

        private static String MEMTABLE_FLUSH_PENDING_DESCRIPTION =
            "Reports 1 if a memtable flush is pending, otherwise it reports 0";

        private static String NUMBER_OF_RUNNING_FLUSHES_DESCRIPTION = "Number of currently running flushes";

        private static String COMPACTION_PENDING_DESCRIPTION =
            "Reports 1 if at least one compaction is pending, otherwise it reports 0";

        private static String NUMBER_OF_RUNNING_COMPACTIONS_DESCRIPTION =
            "Number of currently running compactions";

        private static String ESTIMATED_BYTES_OF_PENDING_COMPACTION_DESCRIPTION =
            "Estimated total number of bytes a compaction needs to rewrite on disk to get all levels down to under target size";

        private static String TOTAL_SST_FILE_SIZE_DESCRIPTION = "Total size in bytes of all SST files";

        private static String LIVE_SST_FILES_SIZE_DESCRIPTION =
            "Total size in bytes of all SST files that belong to the latest LSM tree";

        private static String NUMBER_OF_LIVE_VERSIONS_DESCRIPTION = "Number of live versions of the LSM tree";
        private static String CAPACITY_OF_BLOCK_CACHE_DESCRIPTION = "Capacity of the block cache in bytes";

        private static String USAGE_OF_BLOCK_CACHE_DESCRIPTION =
            "Memory size of the entries residing in block cache in bytes";

        private static String PINNED_USAGE_OF_BLOCK_CACHE_DESCRIPTION =
            "Memory size for the entries being pinned in the block cache in bytes";

        private static String ESTIMATED_NUMBER_OF_KEYS_DESCRIPTION =
            "Estimated number of keys in the active and unflushed immutable memtables and storage";

        private static String ESTIMATED_MEMORY_OF_TABLE_READERS_DESCRIPTION =
            "Estimated memory in bytes used for reading SST tables, excluding memory used in block cache";

        private static String TOTAL_NUMBER_OF_BACKGROUND_ERRORS_DESCRIPTION = "Total number of background errors";

        #endregion

        #region KIP-471 in progress 
        public static Sensor BytesWrittenToDatabaseSensor(StreamMetricsRegistry streamRegistry,
            TaskId taskId,
            string storeType,
            string storeName)
        {
            Sensor sensor = CreateSensor(
                streamRegistry,
                taskId,
                storeName,
                BYTES_WRITTEN_TO_DB,
                BYTES_WRITTEN_TO_DB_DESCRIPTION,
                MetricsRecordingLevel.DEBUG);

            IDictionary<string, string> tags =
                streamRegistry.StoreLevelTags(GetThreadId(), taskId.ToString(), storeName, storeType);

            SensorHelper.AddRateOfSumAndSumMetricsToSensor(
                sensor,
                StreamMetricsRegistry.STATE_STORE_LEVEL_GROUP,
                tags,
                BYTES_WRITTEN_TO_DB,
                BYTES_WRITTEN_TO_DB_RATE_DESCRIPTION,
                BYTES_WRITTEN_TO_DB_TOTAL_DESCRIPTION);

            return sensor;
        }

        public static Sensor BytesReadFromDatabaseSensor(StreamMetricsRegistry streamRegistry,
            TaskId taskId,
            string storeType,
            string storeName)
        {
            Sensor sensor = CreateSensor(
                streamRegistry,
                taskId,
                storeName,
                BYTES_READ_FROM_DB,
                BYTES_READ_FROM_DB_DESCRIPTION,
                MetricsRecordingLevel.DEBUG);

            IDictionary<string, string> tags =
                streamRegistry.StoreLevelTags(GetThreadId(), taskId.ToString(), storeName, storeType);

            SensorHelper.AddRateOfSumAndSumMetricsToSensor(
                sensor,
                StreamMetricsRegistry.STATE_STORE_LEVEL_GROUP,
                tags,
                BYTES_READ_FROM_DB,
                BYTES_READ_FROM_DB_RATE_DESCRIPTION,
                BYTES_READ_FROM_DB_TOTAL_DESCRIPTION);

            return sensor;
        }

        public static Sensor MemtableBytesFlushedSensor(StreamMetricsRegistry streamRegistry,
            TaskId taskId,
            string storeType,
            string storeName)
        {
            Sensor sensor = CreateSensor(
                streamRegistry,
                taskId,
                storeName,
                MEMTABLE_BYTES_FLUSHED,
                MEMTABLE_BYTES_FLUSHED_DESCRIPTION,
                MetricsRecordingLevel.DEBUG);

            IDictionary<string, string> tags =
                streamRegistry.StoreLevelTags(GetThreadId(), taskId.ToString(), storeName, storeType);

            SensorHelper.AddRateOfSumAndSumMetricsToSensor(
                sensor,
                StreamMetricsRegistry.STATE_STORE_LEVEL_GROUP,
                tags,
                MEMTABLE_BYTES_FLUSHED,
                MEMTABLE_BYTES_FLUSHED_RATE_DESCRIPTION,
                MEMTABLE_BYTES_FLUSHED_TOTAL_DESCRIPTION);

            return sensor;
        }

        public static Sensor MemtableHitRatioSensor(StreamMetricsRegistry streamRegistry,
            TaskId taskId,
            string storeType,
            string storeName)
        {
            Sensor sensor = CreateSensor(
                streamRegistry,
                taskId,
                storeName,
                MEMTABLE_HIT_RATIO,
                MEMTABLE_HIT_RATIO_DESCRIPTION,
                MetricsRecordingLevel.DEBUG);

            IDictionary<string, string> tags =
                streamRegistry.StoreLevelTags(GetThreadId(), taskId.ToString(), storeName, storeType);

            SensorHelper.AddValueMetricToSensor(
                sensor,
                StreamMetricsRegistry.STATE_STORE_LEVEL_GROUP,
                tags,
                MEMTABLE_HIT_RATIO,
                MEMTABLE_HIT_RATIO_DESCRIPTION);

            return sensor;
        }

        public static Sensor MemtableAvgFlushTimeSensor(StreamMetricsRegistry streamRegistry,
            TaskId taskId,
            string storeType,
            string storeName)
        {
            Sensor sensor = CreateSensor(
                streamRegistry,
                taskId,
                storeName,
                MEMTABLE_FLUSH_TIME + StreamMetricsRegistry.AVG_SUFFIX,
                MEMTABLE_FLUSH_TIME_AVG_DESCRIPTION,
                MetricsRecordingLevel.DEBUG);

            IDictionary<string, string> tags =
                streamRegistry.StoreLevelTags(GetThreadId(), taskId.ToString(), storeName, storeType);

            SensorHelper.AddValueMetricToSensor(
                sensor,
                StreamMetricsRegistry.STATE_STORE_LEVEL_GROUP,
                tags,
                MEMTABLE_FLUSH_TIME + StreamMetricsRegistry.AVG_SUFFIX,
                MEMTABLE_FLUSH_TIME_AVG_DESCRIPTION);

            return sensor;
        }

        public static Sensor MemtableMinFlushTimeSensor(StreamMetricsRegistry streamRegistry,
            TaskId taskId,
            string storeType,
            string storeName)
        {
            Sensor sensor = CreateSensor(
                streamRegistry,
                taskId,
                storeName,
                MEMTABLE_FLUSH_TIME + StreamMetricsRegistry.MIN_SUFFIX,
                MEMTABLE_FLUSH_TIME_MIN_DESCRIPTION,
                MetricsRecordingLevel.DEBUG);

            IDictionary<string, string> tags =
                streamRegistry.StoreLevelTags(GetThreadId(), taskId.ToString(), storeName, storeType);

            SensorHelper.AddValueMetricToSensor(
                sensor,
                StreamMetricsRegistry.STATE_STORE_LEVEL_GROUP,
                tags,
                MEMTABLE_FLUSH_TIME + StreamMetricsRegistry.MIN_SUFFIX,
                MEMTABLE_FLUSH_TIME_MIN_DESCRIPTION);

            return sensor;
        }

        public static Sensor MemtableMaxFlushTimeSensor(StreamMetricsRegistry streamRegistry,
            TaskId taskId,
            string storeType,
            string storeName)
        {
            Sensor sensor = CreateSensor(
                streamRegistry,
                taskId,
                storeName,
                MEMTABLE_FLUSH_TIME + StreamMetricsRegistry.MAX_SUFFIX,
                MEMTABLE_FLUSH_TIME_MAX_DESCRIPTION,
                MetricsRecordingLevel.DEBUG);

            IDictionary<string, string> tags =
                streamRegistry.StoreLevelTags(GetThreadId(), taskId.ToString(), storeName, storeType);

            SensorHelper.AddValueMetricToSensor(
                sensor,
                StreamMetricsRegistry.STATE_STORE_LEVEL_GROUP,
                tags,
                MEMTABLE_FLUSH_TIME + StreamMetricsRegistry.MAX_SUFFIX,
                MEMTABLE_FLUSH_TIME_MAX_DESCRIPTION);

            return sensor;
        }

        public static Sensor WriteStallDurationSensor(StreamMetricsRegistry streamRegistry,
            TaskId taskId,
            string storeType,
            string storeName)
        {
            Sensor sensor = CreateSensor(
                streamRegistry,
                taskId,
                storeName,
                WRITE_STALL_DURATION,
                WRITE_STALL_DURATION_DESCRIPTION,
                MetricsRecordingLevel.DEBUG);

            IDictionary<string, string> tags =
                streamRegistry.StoreLevelTags(GetThreadId(), taskId.ToString(), storeName, storeType);

            SensorHelper.AddAvgAndSumMetricsToSensor(
                sensor,
                StreamMetricsRegistry.STATE_STORE_LEVEL_GROUP,
                tags,
                WRITE_STALL_DURATION,
                WRITE_STALL_DURATION_AVG_DESCRIPTION,
                WRITE_STALL_DURATION_TOTAL_DESCRIPTION
            );

            return sensor;
        }

        public static Sensor BlockCacheDataHitRatioSensor(StreamMetricsRegistry streamRegistry,
            TaskId taskId,
            string storeType,
            string storeName)
        {
            Sensor sensor = CreateSensor(
                streamRegistry,
                taskId,
                storeName,
                BLOCK_CACHE_DATA_HIT_RATIO,
                BLOCK_CACHE_DATA_HIT_RATIO_DESCRIPTION,
                MetricsRecordingLevel.DEBUG);

            IDictionary<string, string> tags =
                streamRegistry.StoreLevelTags(GetThreadId(), taskId.ToString(), storeName, storeType);

            SensorHelper.AddValueMetricToSensor(
                sensor,
                StreamMetricsRegistry.STATE_STORE_LEVEL_GROUP,
                tags,
                BLOCK_CACHE_DATA_HIT_RATIO,
                BLOCK_CACHE_DATA_HIT_RATIO_DESCRIPTION
            );

            return sensor;
        }

        public static Sensor BlockCacheIndexHitRatioSensor(StreamMetricsRegistry streamRegistry,
            TaskId taskId,
            string storeType,
            string storeName)
        {
            Sensor sensor = CreateSensor(
                streamRegistry,
                taskId,
                storeName,
                BLOCK_CACHE_INDEX_HIT_RATIO,
                BLOCK_CACHE_INDEX_HIT_RATIO_DESCRIPTION,
                MetricsRecordingLevel.DEBUG);

            IDictionary<string, string> tags =
                streamRegistry.StoreLevelTags(GetThreadId(), taskId.ToString(), storeName, storeType);

            SensorHelper.AddValueMetricToSensor(
                sensor,
                StreamMetricsRegistry.STATE_STORE_LEVEL_GROUP,
                tags,
                BLOCK_CACHE_INDEX_HIT_RATIO,
                BLOCK_CACHE_INDEX_HIT_RATIO_DESCRIPTION
            );

            return sensor;
        }

        public static Sensor BlockCacheFilterHitRatioSensor(StreamMetricsRegistry streamRegistry,
            TaskId taskId,
            string storeType,
            string storeName)
        {
            Sensor sensor = CreateSensor(
                streamRegistry,
                taskId,
                storeName,
                BLOCK_CACHE_FILTER_HIT_RATIO,
                BLOCK_CACHE_FILTER_HIT_RATIO_DESCRIPTION,
                MetricsRecordingLevel.DEBUG);

            IDictionary<string, string> tags =
                streamRegistry.StoreLevelTags(GetThreadId(), taskId.ToString(), storeName, storeType);

            SensorHelper.AddValueMetricToSensor(
                sensor,
                StreamMetricsRegistry.STATE_STORE_LEVEL_GROUP,
                tags,
                BLOCK_CACHE_FILTER_HIT_RATIO,
                BLOCK_CACHE_FILTER_HIT_RATIO_DESCRIPTION
            );

            return sensor;
        }

        public static Sensor BytesReadDuringCompactionSensor(StreamMetricsRegistry streamRegistry,
            TaskId taskId,
            string storeType,
            string storeName)
        {
            Sensor sensor = CreateSensor(
                streamRegistry,
                taskId,
                storeName,
                BYTES_READ_DURING_COMPACTION,
                BYTES_READ_DURING_COMPACTION_DESCRIPTION,
                MetricsRecordingLevel.DEBUG);

            IDictionary<string, string> tags =
                streamRegistry.StoreLevelTags(GetThreadId(), taskId.ToString(), storeName, storeType);

            SensorHelper.AddRateOfSumMetricToSensor(
                sensor,
                StreamMetricsRegistry.STATE_STORE_LEVEL_GROUP,
                tags,
                BYTES_READ_DURING_COMPACTION,
                BYTES_READ_DURING_COMPACTION_DESCRIPTION
            );

            return sensor;
        }

        public static Sensor BytesWrittenDuringCompactionSensor(StreamMetricsRegistry streamRegistry,
            TaskId taskId,
            string storeType,
            string storeName)
        {
            Sensor sensor = CreateSensor(
                streamRegistry,
                taskId,
                storeName,
                BYTES_WRITTEN_DURING_COMPACTION,
                BYTES_WRITTEN_DURING_COMPACTION_DESCRIPTION,
                MetricsRecordingLevel.DEBUG);

            IDictionary<string, string> tags =
                streamRegistry.StoreLevelTags(GetThreadId(), taskId.ToString(), storeName, storeType);

            SensorHelper.AddRateOfSumMetricToSensor(
                sensor,
                StreamMetricsRegistry.STATE_STORE_LEVEL_GROUP,
                tags,
                BYTES_WRITTEN_DURING_COMPACTION,
                BYTES_WRITTEN_DURING_COMPACTION_DESCRIPTION
            );
            return sensor;
        }

        public static Sensor CompactionTimeAvgSensor(StreamMetricsRegistry streamRegistry,
            TaskId taskId,
            string storeType,
            string storeName)
        {
            Sensor sensor = CreateSensor(
                streamRegistry,
                taskId,
                storeName,
                COMPACTION_TIME + StreamMetricsRegistry.AVG_SUFFIX,
                COMPACTION_TIME_AVG_DESCRIPTION,
                MetricsRecordingLevel.DEBUG);

            IDictionary<string, string> tags =
                streamRegistry.StoreLevelTags(GetThreadId(), taskId.ToString(), storeName, storeType);

            SensorHelper.AddValueMetricToSensor(
                sensor,
                StreamMetricsRegistry.STATE_STORE_LEVEL_GROUP,
                tags,
                COMPACTION_TIME + StreamMetricsRegistry.AVG_SUFFIX,
                COMPACTION_TIME_AVG_DESCRIPTION
            );

            return sensor;
        }

        public static Sensor CompactionTimeMinSensor(StreamMetricsRegistry streamRegistry,
            TaskId taskId,
            string storeType,
            string storeName)
        {
            Sensor sensor = CreateSensor(
                streamRegistry,
                taskId,
                storeName,
                COMPACTION_TIME + StreamMetricsRegistry.MIN_SUFFIX,
                COMPACTION_TIME_MIN_DESCRIPTION,
                MetricsRecordingLevel.DEBUG);

            IDictionary<string, string> tags =
                streamRegistry.StoreLevelTags(GetThreadId(), taskId.ToString(), storeName, storeType);

            SensorHelper.AddValueMetricToSensor(
                sensor,
                StreamMetricsRegistry.STATE_STORE_LEVEL_GROUP,
                tags,
                COMPACTION_TIME + StreamMetricsRegistry.MIN_SUFFIX,
                COMPACTION_TIME_MIN_DESCRIPTION
            );

            return sensor;
        }

        public static Sensor CompactionTimeMaxSensor(StreamMetricsRegistry streamRegistry,
            TaskId taskId,
            string storeType,
            string storeName)
        {
            Sensor sensor = CreateSensor(
                streamRegistry,
                taskId,
                storeName,
                COMPACTION_TIME + StreamMetricsRegistry.MAX_SUFFIX,
                COMPACTION_TIME_MAX_DESCRIPTION,
                MetricsRecordingLevel.DEBUG);

            IDictionary<string, string> tags =
                streamRegistry.StoreLevelTags(GetThreadId(), taskId.ToString(), storeName, storeType);

            SensorHelper.AddValueMetricToSensor(
                sensor,
                StreamMetricsRegistry.STATE_STORE_LEVEL_GROUP,
                tags,
                COMPACTION_TIME + StreamMetricsRegistry.MAX_SUFFIX,
                COMPACTION_TIME_MAX_DESCRIPTION
            );

            return sensor;
        }

        public static Sensor NumberOfOpenFilesSensor(StreamMetricsRegistry streamRegistry,
            TaskId taskId,
            string storeType,
            string storeName)
        {
            Sensor sensor = CreateSensor(
                streamRegistry,
                taskId,
                storeName,
                NUMBER_OF_OPEN_FILES,
                NUMBER_OF_OPEN_FILES_DESCRIPTION,
                MetricsRecordingLevel.DEBUG);

            IDictionary<string, string> tags =
                streamRegistry.StoreLevelTags(GetThreadId(), taskId.ToString(), storeName, storeType);

            SensorHelper.AddSumMetricToSensor(
                sensor,
                StreamMetricsRegistry.STATE_STORE_LEVEL_GROUP,
                tags,
                NUMBER_OF_OPEN_FILES,
                NUMBER_OF_OPEN_FILES_DESCRIPTION,
                false
            );

            return sensor;
        }

        public static Sensor NumberOfFileErrorsSensor(StreamMetricsRegistry streamRegistry,
            TaskId taskId,
            string storeType,
            string storeName)
        {
            Sensor sensor = CreateSensor(
                streamRegistry,
                taskId,
                storeName,
                NUMBER_OF_FILE_ERRORS,
                NUMBER_OF_FILE_ERRORS_DESCRIPTION,
                MetricsRecordingLevel.DEBUG);

            IDictionary<string, string> tags =
                streamRegistry.StoreLevelTags(GetThreadId(), taskId.ToString(), storeName, storeType);

            SensorHelper.AddSumMetricToSensor(
                sensor,
                StreamMetricsRegistry.STATE_STORE_LEVEL_GROUP,
                tags,
                NUMBER_OF_FILE_ERRORS,
                NUMBER_OF_FILE_ERRORS_DESCRIPTION
            );

            return sensor;
        }
#endregion

        #region KIP-607 support
        
        public static void AddNumEntriesActiveMemTableMetric(StreamMetricsRegistry streamRegistry,
            TaskId taskId,
            string storeType,
            string storeName,
            Func<BigInteger> valueProvider)
        {
            AddMutableMetric(
                streamRegistry,
                taskId,
                storeType,
                storeName,
                valueProvider,
                NUMBER_OF_ENTRIES_ACTIVE_MEMTABLE,
                NUMBER_OF_ENTRIES_ACTIVE_MEMTABLE_DESCRIPTION
            );
        }

        public static void AddNumEntriesImmMemTablesMetric(StreamMetricsRegistry streamRegistry,
            TaskId taskId,
            string storeType,
            string storeName,
            Func<BigInteger> valueProvider)
        {
            AddMutableMetric(
                streamRegistry,
                taskId,
                storeType,
                storeName,
                valueProvider,
                NUMBER_OF_ENTRIES_IMMUTABLE_MEMTABLES,
                NUMBER_OF_ENTRIES_IMMUTABLE_MEMTABLES_DESCRIPTION
            );
        }

        public static void AddNumDeletesImmMemTablesMetric(StreamMetricsRegistry streamRegistry,
            TaskId taskId,
            string storeType,
            string storeName,
            Func<BigInteger> valueProvider)
        {
            AddMutableMetric(
                streamRegistry,
                taskId,
                storeType,
                storeName,
                valueProvider,
                NUMBER_OF_DELETES_IMMUTABLE_MEMTABLES,
                NUMBER_OF_DELETES_IMMUTABLE_MEMTABLES_DESCRIPTION
            );
        }

        public static void AddNumDeletesActiveMemTableMetric(StreamMetricsRegistry streamRegistry,
            TaskId taskId,
            string storeType,
            string storeName,
            Func<BigInteger> valueProvider)
        {
            AddMutableMetric(
                streamRegistry,
                taskId,
                storeType,
                storeName,
                valueProvider,
                NUMBER_OF_DELETES_ACTIVE_MEMTABLE,
                NUMBER_OF_DELETES_ACTIVE_MEMTABLES_DESCRIPTION
            );
        }

        public static void AddNumImmutableMemTableMetric(StreamMetricsRegistry streamRegistry,
            TaskId taskId,
            string storeType,
            string storeName,
            Func<BigInteger> valueProvider)
        {
            AddMutableMetric(
                streamRegistry,
                taskId,
                storeType,
                storeName,
                valueProvider,
                NUMBER_OF_IMMUTABLE_MEMTABLES,
                NUMBER_OF_IMMUTABLE_MEMTABLES_DESCRIPTION
            );
        }

        public static void AddCurSizeActiveMemTable(StreamMetricsRegistry streamRegistry,
            TaskId taskId,
            string storeType,
            string storeName,
            Func<BigInteger> valueProvider)
        {
            AddMutableMetric(
                streamRegistry,
                taskId,
                storeType,
                storeName,
                valueProvider,
                CURRENT_SIZE_OF_ACTIVE_MEMTABLE,
                CURRENT_SIZE_OF_ACTIVE_MEMTABLE_DESCRIPTION
            );
        }

        public static void AddCurSizeAllMemTables(StreamMetricsRegistry streamRegistry,
            TaskId taskId,
            string storeType,
            string storeName,
            Func<BigInteger> valueProvider)
        {
            AddMutableMetric(
                streamRegistry,
                taskId,
                storeType,
                storeName,
                valueProvider,
                CURRENT_SIZE_OF_ALL_MEMTABLES,
                CURRENT_SIZE_OF_ALL_MEMTABLES_DESCRIPTION
            );
        }

        public static void AddSizeAllMemTables(StreamMetricsRegistry streamRegistry,
            TaskId taskId,
            string storeType,
            string storeName,
            Func<BigInteger> valueProvider)
        {
            AddMutableMetric(
                streamRegistry,
                taskId,
                storeType,
                storeName,
                valueProvider,
                SIZE_OF_ALL_MEMTABLES,
                SIZE_OF_ALL_MEMTABLES_DESCRIPTION
            );
        }

        public static void AddMemTableFlushPending(StreamMetricsRegistry streamRegistry,
            TaskId taskId,
            string storeType,
            string storeName,
            Func<BigInteger> valueProvider)
        {
            AddMutableMetric(
                streamRegistry,
                taskId,
                storeType,
                storeName,
                valueProvider,
                MEMTABLE_FLUSH_PENDING,
                MEMTABLE_FLUSH_PENDING_DESCRIPTION
            );
        }

        public static void AddNumRunningFlushesMetric(StreamMetricsRegistry streamRegistry,
            TaskId taskId,
            string storeType,
            string storeName,
            Func<BigInteger> valueProvider)
        {
            AddMutableMetric(
                streamRegistry,
                taskId,
                storeType,
                storeName,
                valueProvider,
                NUMBER_OF_RUNNING_FLUSHES,
                NUMBER_OF_RUNNING_FLUSHES_DESCRIPTION
            );
        }

        public static void AddCompactionPendingMetric(StreamMetricsRegistry streamRegistry,
            TaskId taskId,
            string storeType,
            string storeName,
            Func<BigInteger> valueProvider)
        {
            AddMutableMetric(
                streamRegistry,
                taskId,
                storeType,
                storeName,
                valueProvider,
                COMPACTION_PENDING,
                COMPACTION_PENDING_DESCRIPTION
            );
        }

        public static void AddNumRunningCompactionsMetric(StreamMetricsRegistry streamRegistry,
            TaskId taskId,
            string storeType,
            string storeName,
            Func<BigInteger> valueProvider)
        {
            AddMutableMetric(
                streamRegistry,
                taskId,
                storeType,
                storeName,
                valueProvider,
                NUMBER_OF_RUNNING_COMPACTIONS,
                NUMBER_OF_RUNNING_COMPACTIONS_DESCRIPTION
            );
        }

        public static void AddEstimatePendingCompactionBytesMetric(StreamMetricsRegistry streamRegistry,
            TaskId taskId,
            string storeType,
            string storeName,
            Func<BigInteger> valueProvider)
        {
            AddMutableMetric(
                streamRegistry,
                taskId,
                storeType,
                storeName,
                valueProvider,
                ESTIMATED_BYTES_OF_PENDING_COMPACTION,
                ESTIMATED_BYTES_OF_PENDING_COMPACTION_DESCRIPTION
            );
        }

        public static void AddTotalSstFilesSizeMetric(StreamMetricsRegistry streamRegistry,
            TaskId taskId,
            string storeType,
            string storeName,
            Func<BigInteger> valueProvider)
        {
            AddMutableMetric(
                streamRegistry,
                taskId,
                storeType,
                storeName,
                valueProvider,
                TOTAL_SST_FILES_SIZE,
                TOTAL_SST_FILE_SIZE_DESCRIPTION
            );
        }

        public static void AddLiveSstFilesSizeMetric(StreamMetricsRegistry streamRegistry,
            TaskId taskId,
            string storeType,
            string storeName,
            Func<BigInteger> valueProvider)
        {
            AddMutableMetric(
                streamRegistry,
                taskId,
                storeType,
                storeName,
                valueProvider,
                LIVE_SST_FILES_SIZE,
                LIVE_SST_FILES_SIZE_DESCRIPTION
            );
        }

        public static void AddNumLiveVersionMetric(StreamMetricsRegistry streamRegistry,
            TaskId taskId,
            string storeType,
            string storeName,
            Func<BigInteger> valueProvider)
        {
            AddMutableMetric(
                streamRegistry,
                taskId,
                storeType,
                storeName,
                valueProvider,
                NUMBER_OF_LIVE_VERSIONS,
                NUMBER_OF_LIVE_VERSIONS_DESCRIPTION
            );
        }

        public static void AddBlockCacheCapacityMetric(StreamMetricsRegistry streamRegistry,
            TaskId taskId,
            string storeType,
            string storeName,
            Func<BigInteger> valueProvider)
        {
            AddMutableMetric(
                streamRegistry,
                taskId,
                storeType,
                storeName,
                valueProvider,
                CAPACITY_OF_BLOCK_CACHE,
                CAPACITY_OF_BLOCK_CACHE_DESCRIPTION
            );
        }

        public static void AddBlockCacheUsageMetric(StreamMetricsRegistry streamRegistry,
            TaskId taskId,
            string storeType,
            string storeName,
            Func<BigInteger> valueProvider)
        {
            AddMutableMetric(
                streamRegistry,
                taskId,
                storeType,
                storeName,
                valueProvider,
                USAGE_OF_BLOCK_CACHE,
                USAGE_OF_BLOCK_CACHE_DESCRIPTION
            );
        }

        public static void AddBlockCachePinnedUsageMetric(StreamMetricsRegistry streamRegistry,
            TaskId taskId,
            string storeType,
            string storeName,
            Func<BigInteger> valueProvider)
        {
            AddMutableMetric(
                streamRegistry,
                taskId,
                storeType,
                storeName,
                valueProvider,
                PINNED_USAGE_OF_BLOCK_CACHE,
                PINNED_USAGE_OF_BLOCK_CACHE_DESCRIPTION
            );
        }

        public static void AddEstimateNumKeysMetric(StreamMetricsRegistry streamRegistry,
            TaskId taskId,
            string storeType,
            string storeName,
            Func<BigInteger> valueProvider)
        {
            AddMutableMetric(
                streamRegistry,
                taskId,
                storeType,
                storeName,
                valueProvider,
                ESTIMATED_NUMBER_OF_KEYS,
                ESTIMATED_NUMBER_OF_KEYS_DESCRIPTION
            );
        }

        public static void AddEstimateTableReadersMemMetric(StreamMetricsRegistry streamRegistry,
            TaskId taskId,
            string storeType,
            string storeName,
            Func<BigInteger> valueProvider)
        {
            AddMutableMetric(
                streamRegistry,
                taskId,
                storeType,
                storeName, valueProvider,
                ESTIMATED_MEMORY_OF_TABLE_READERS,
                ESTIMATED_MEMORY_OF_TABLE_READERS_DESCRIPTION
            );
        }

        public static void AddBackgroundErrorsMetric(StreamMetricsRegistry streamRegistry,
            TaskId taskId,
            string storeType,
            string storeName,
            Func<BigInteger> valueProvider)
        {
            AddMutableMetric(
                streamRegistry,
                taskId,
                storeType,
                storeName,
                valueProvider,
                NUMBER_OF_BACKGROUND_ERRORS,
                TOTAL_NUMBER_OF_BACKGROUND_ERRORS_DESCRIPTION
            );
        }
        #endregion

        private static void AddMutableMetric<T>(StreamMetricsRegistry streamRegistry,
            TaskId taskId,
            string storeType,
            string storeName,
            Func<T> valueProvider,
            String name,
            String description)
        {
            var sensor = CreateSensor(streamRegistry, taskId, storeName, name, description, MetricsRecordingLevel.INFO);
            IDictionary<string, string> tags =
                streamRegistry.StoreLevelTags(GetThreadId(), taskId.ToString(), storeName, storeType);

            SensorHelper.AddMutableValueMetricToSensor(
                sensor,
                StreamMetricsRegistry.STATE_STORE_LEVEL_GROUP,
                tags,
                name,
                description,
                valueProvider);
        }

        private static Sensor CreateSensor(
            StreamMetricsRegistry streamRegistry,
            TaskId taskId,
            string storeName,
            string sensorName,
            string description,
            MetricsRecordingLevel metricsRecordingLevel)
        {
            return streamRegistry.StoreLevelSensor(
                GetThreadId(),
                taskId,
                storeName,
                sensorName,
                description,
                metricsRecordingLevel);
        }

        private static string GetThreadId() => Thread.CurrentThread.Name;
    }
}