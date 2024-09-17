using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Numerics;
using Microsoft.Extensions.Logging;
using RocksDbSharp;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Processors.Internal;

namespace Streamiz.Kafka.Net.Metrics.Internal
{
    internal class RocksDbMetricsRecorder
    {
        private const String ROCKSDB_PROPERTIES_PREFIX = "rocksdb.";
        private static ILogger logger = Logger.GetLogger(typeof(RocksDbMetricsRecorder));

        private class DbAndCacheAndStatistics
        {
            public RocksDb db;
            // public Cache cache;
            // public Statistics statistics;

            public DbAndCacheAndStatistics(RocksDb db /*, final Cache cache, final Statistics statistics*/)
            {
                this.db = db;
                /* this.cache = cache;
                 if (statistics != null) {
                     statistics.setStatsLevel(StatsLevel.EXCEPT_DETAILED_TIMERS);
                 }
                 this.statistics = statistics;*/
            }
        }

        private readonly ConcurrentDictionary<string, DbAndCacheAndStatistics> storeToValueProviders = new();
        //private bool singleCache = true;

        private readonly string metricsScope;
        private readonly string storeName;
        private TaskId taskId;
        private StreamMetricsRegistry streamMetricsRegistry;

        internal string Name => $"{taskId}-{storeName}";

    public void Init(StreamMetricsRegistry streamMetricsRegistry, TaskId taskId)
        {
            this.streamMetricsRegistry = streamMetricsRegistry;
            this.taskId = taskId;

            InitGauges();
        }

        public void AddValueProviders(string segmentName, RocksDb db/*,
            final Cache cache,
            final Statistics statistics*/)
        {
            if (!storeToValueProviders.Any()) {
                logger.LogDebug($"Adding metrics recorder of task {taskId} to metrics recording trigger");
                streamMetricsRegistry.RocksDbMetricsRecordingTrigger.AddMetricsRecorder(this);
            } else if (storeToValueProviders.ContainsKey(segmentName)) {
                throw new IllegalStateException($"Value providers for store {segmentName} of task {taskId}" +
                                                " has been already added. This is a bug in Streamiz. Please open a bug.");
            }
            //verifyDbAndCacheAndStatistics(segmentName, db, cache, statistics);
            logger.LogDebug($"Adding value providers for store {segmentName} of task {taskId}");
            storeToValueProviders.TryAdd(segmentName, new DbAndCacheAndStatistics(db/*, cache, statistics*/));
        }
        
        public void RemoveValueProviders(String segmentName) {
            logger.LogDebug("$Removing value providers for store {segmentName} of task {taskId}");
            bool result = storeToValueProviders.TryRemove(segmentName, out DbAndCacheAndStatistics db);
            if (!result)
            {
                throw new IllegalStateException($"No value providers for store {segmentName}of task {taskId}" +
                                                " could be found. This is a bug in Kafka Streamiz. Please open a bug");
            }
            if (!storeToValueProviders.Any()) {
                logger.LogDebug(
                    $"Removing metrics recorder for store {storeName} of task {taskId} from metrics recording trigger");
                streamMetricsRegistry.RocksDbMetricsRecordingTrigger.RemoveMetricsRecorder(this);
            }
        }

        public void Record(long now)
        {
            
        }


        private void InitGauges()
        {
            RocksDbMetrics.AddNumImmutableMemTableMetric(
                streamMetricsRegistry,
                taskId,
                metricsScope,
                storeName,
                GaugeToComputeSumOfProperties(RocksDbMetrics.NUMBER_OF_IMMUTABLE_MEMTABLES)
            );
            RocksDbMetrics.AddCurSizeActiveMemTable(
                streamMetricsRegistry,
                taskId,
                metricsScope,
                storeName,
                GaugeToComputeSumOfProperties(RocksDbMetrics.CURRENT_SIZE_OF_ACTIVE_MEMTABLE)
            );
            RocksDbMetrics.AddCurSizeAllMemTables(
                streamMetricsRegistry,
                taskId,
                metricsScope,
                storeName,
                GaugeToComputeSumOfProperties(RocksDbMetrics.CURRENT_SIZE_OF_ALL_MEMTABLES)
            );
            RocksDbMetrics.AddSizeAllMemTables(
                streamMetricsRegistry,
                taskId,
                metricsScope,
                storeName,
                GaugeToComputeSumOfProperties(RocksDbMetrics.SIZE_OF_ALL_MEMTABLES)
            );
            RocksDbMetrics.AddNumEntriesActiveMemTableMetric(
                streamMetricsRegistry,
                taskId,
                metricsScope,
                storeName,
                GaugeToComputeSumOfProperties(RocksDbMetrics.NUMBER_OF_ENTRIES_ACTIVE_MEMTABLE)
            );
            RocksDbMetrics.AddNumDeletesActiveMemTableMetric(
                streamMetricsRegistry,
                taskId,
                metricsScope,
                storeName,
                GaugeToComputeSumOfProperties(RocksDbMetrics.NUMBER_OF_DELETES_ACTIVE_MEMTABLE)
            );
            RocksDbMetrics.AddNumEntriesImmMemTablesMetric(
                streamMetricsRegistry,
                taskId,
                metricsScope,
                storeName,
                GaugeToComputeSumOfProperties(RocksDbMetrics.NUMBER_OF_ENTRIES_IMMUTABLE_MEMTABLES)
            );
            RocksDbMetrics.AddNumDeletesImmMemTablesMetric(
                streamMetricsRegistry,
                taskId,
                metricsScope,
                storeName,
                GaugeToComputeSumOfProperties(RocksDbMetrics.NUMBER_OF_DELETES_IMMUTABLE_MEMTABLES)
            );
            RocksDbMetrics.AddMemTableFlushPending(
                streamMetricsRegistry,
                taskId,
                metricsScope,
                storeName,
                GaugeToComputeSumOfProperties(RocksDbMetrics.MEMTABLE_FLUSH_PENDING)
            );
            RocksDbMetrics.AddNumRunningFlushesMetric(
                streamMetricsRegistry,
                taskId,
                metricsScope,
                storeName,
                GaugeToComputeSumOfProperties(RocksDbMetrics.NUMBER_OF_RUNNING_FLUSHES)
            );
            RocksDbMetrics.AddCompactionPendingMetric(
                streamMetricsRegistry,
                taskId,
                metricsScope,
                storeName,
                GaugeToComputeSumOfProperties(RocksDbMetrics.COMPACTION_PENDING)
            );
            RocksDbMetrics.AddNumRunningCompactionsMetric(
                streamMetricsRegistry,
                taskId,
                metricsScope,
                storeName,
                GaugeToComputeSumOfProperties(RocksDbMetrics.NUMBER_OF_RUNNING_COMPACTIONS)
            );
            RocksDbMetrics.AddEstimatePendingCompactionBytesMetric(
                streamMetricsRegistry,
                taskId,
                metricsScope,
                storeName,
                GaugeToComputeSumOfProperties(RocksDbMetrics.ESTIMATED_BYTES_OF_PENDING_COMPACTION)
            );
            RocksDbMetrics.AddTotalSstFilesSizeMetric(
                streamMetricsRegistry,
                taskId,
                metricsScope,
                storeName,
                GaugeToComputeSumOfProperties(RocksDbMetrics.TOTAL_SST_FILES_SIZE)
            );
            RocksDbMetrics.AddLiveSstFilesSizeMetric(
                streamMetricsRegistry,
                taskId,
                metricsScope,
                storeName,
                GaugeToComputeSumOfProperties(RocksDbMetrics.LIVE_SST_FILES_SIZE)
            );
            RocksDbMetrics.AddNumLiveVersionMetric(
                streamMetricsRegistry,
                taskId,
                metricsScope,
                storeName,
                GaugeToComputeSumOfProperties(RocksDbMetrics.NUMBER_OF_LIVE_VERSIONS)
            );
            RocksDbMetrics.AddEstimateNumKeysMetric(
                streamMetricsRegistry,
                taskId,
                metricsScope,
                storeName,
                GaugeToComputeSumOfProperties(RocksDbMetrics.ESTIMATED_NUMBER_OF_KEYS)
            );
            RocksDbMetrics.AddEstimateTableReadersMemMetric(
                streamMetricsRegistry,
                taskId,
                metricsScope,
                storeName,
                GaugeToComputeSumOfProperties(RocksDbMetrics.ESTIMATED_MEMORY_OF_TABLE_READERS)
            );
            RocksDbMetrics.AddBackgroundErrorsMetric(
                streamMetricsRegistry,
                taskId,
                metricsScope,
                storeName,
                GaugeToComputeSumOfProperties(RocksDbMetrics.NUMBER_OF_BACKGROUND_ERRORS)
            );
            RocksDbMetrics.AddBlockCacheCapacityMetric(
                streamMetricsRegistry,
                taskId,
                metricsScope,
                storeName,
                GaugeToComputeBlockCacheMetrics(RocksDbMetrics.CAPACITY_OF_BLOCK_CACHE)
            );
            RocksDbMetrics.AddBlockCacheUsageMetric(
                streamMetricsRegistry,
                taskId,
                metricsScope,
                storeName,
                GaugeToComputeBlockCacheMetrics(RocksDbMetrics.USAGE_OF_BLOCK_CACHE)
            );
            RocksDbMetrics.AddBlockCachePinnedUsageMetric(
                streamMetricsRegistry,
                taskId,
                metricsScope,
                storeName,
                GaugeToComputeBlockCacheMetrics(RocksDbMetrics.PINNED_USAGE_OF_BLOCK_CACHE)
            );
        }

        private Func<BigInteger> GaugeToComputeSumOfProperties(String propertyName)
        {
            return () =>
            {
                // BigInteger result = BigInteger.valueOf(0);
                foreach(var valueProvider in storeToValueProviders.Values) {
                    try
                    {
                        valueProvider.db.GetProperty(ROCKSDB_PROPERTIES_PREFIX + propertyName);
                        // values of RocksDB properties are of type unsigned long in C++, i.e., in Java we need to use
                        // BigInteger and construct the object from the byte representation of the value
                        //result = result.add(new BigInteger(1, longToBytes(
                        //    valueProvider.db.getAggregatedLongProperty(ROCKSDB_PROPERTIES_PREFIX + propertyName)
                        //)));
                    }
                    catch (Exception e) {
                        throw new ProcessorStateException("Error recording RocksDB metric " + propertyName, e);
                    }
                }
                return BigInteger.One;
            };
        }


        private Func<BigInteger> GaugeToComputeBlockCacheMetrics(String propertyName) => () => BigInteger.One;
        
        /*private Func<BigInteger> GaugeToComputeBlockCacheMetrics(String propertyName) {
            return (metricsConfig, now) -> {
                BigInteger result = BigInteger.valueOf(0);
                for (final DbAndCacheAndStatistics valueProvider : storeToValueProviders.values()) {
                    try {
                        if (singleCache) {
                            // values of RocksDB properties are of type unsigned long in C++, i.e., in Java we need to use
                            // BigInteger and construct the object from the byte representation of the value
                            result = new BigInteger(1, longToBytes(
                                valueProvider.db.getLongProperty(ROCKSDB_PROPERTIES_PREFIX + propertyName)
                            ));
                            break;
                        } else {
                            // values of RocksDB properties are of type unsigned long in C++, i.e., in Java we need to use
                            // BigInteger and construct the object from the byte representation of the value
                            result = result.add(new BigInteger(1, longToBytes(
                                valueProvider.db.getLongProperty(ROCKSDB_PROPERTIES_PREFIX + propertyName)
                            )));
                        }
                    } catch (final RocksDBException e) {
                        throw new ProcessorStateException("Error recording RocksDB metric " + propertyName, e);
                    }
                }
                return result;
            };
        }*/
    }
}