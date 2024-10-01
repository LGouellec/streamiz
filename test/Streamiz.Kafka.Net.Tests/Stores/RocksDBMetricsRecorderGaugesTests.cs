using System;
using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;
using Moq;
using Streamiz.Kafka.Net.Metrics;
using Streamiz.Kafka.Net.Metrics.Internal;
using Streamiz.Kafka.Net.Processors.Internal;

namespace Streamiz.Kafka.Net.Tests.Stores;

// KIP-610 Unit Tests
public class RocksDbMetricsRecorderGaugesTests
{
    private static string METRICS_SCOPE = "metrics-scope";
    private static TaskId TASK_ID = new() { Id = 0, Partition = 0 };
    private static string STORE_NAME = "store-name";
    private static String ROCKSDB_PROPERTIES_PREFIX = "rocksdb.";
    
    private readonly Dictionary<string, long> dicMetrics = new()
    {
        { ROCKSDB_PROPERTIES_PREFIX + RocksDbMetrics.NUMBER_OF_ENTRIES_ACTIVE_MEMTABLE, RandomGenerator.GetInt32(100) },
        { ROCKSDB_PROPERTIES_PREFIX + RocksDbMetrics.NUMBER_OF_DELETES_ACTIVE_MEMTABLE, RandomGenerator.GetInt32(100) },
        { ROCKSDB_PROPERTIES_PREFIX + RocksDbMetrics.NUMBER_OF_ENTRIES_IMMUTABLE_MEMTABLES, RandomGenerator.GetInt32(100) },
        { ROCKSDB_PROPERTIES_PREFIX + RocksDbMetrics.NUMBER_OF_DELETES_IMMUTABLE_MEMTABLES, RandomGenerator.GetInt32(100) },
        { ROCKSDB_PROPERTIES_PREFIX + RocksDbMetrics.NUMBER_OF_IMMUTABLE_MEMTABLES, RandomGenerator.GetInt32(100) },
        { ROCKSDB_PROPERTIES_PREFIX + RocksDbMetrics.CURRENT_SIZE_OF_ACTIVE_MEMTABLE, RandomGenerator.GetInt32(100) },
        { ROCKSDB_PROPERTIES_PREFIX + RocksDbMetrics.CURRENT_SIZE_OF_ALL_MEMTABLES, RandomGenerator.GetInt32(100) },
        { ROCKSDB_PROPERTIES_PREFIX + RocksDbMetrics.SIZE_OF_ALL_MEMTABLES, RandomGenerator.GetInt32(100) },
        { ROCKSDB_PROPERTIES_PREFIX + RocksDbMetrics.MEMTABLE_FLUSH_PENDING, RandomGenerator.GetInt32(100) },
        { ROCKSDB_PROPERTIES_PREFIX + RocksDbMetrics.NUMBER_OF_RUNNING_FLUSHES, RandomGenerator.GetInt32(100) },
        { ROCKSDB_PROPERTIES_PREFIX + RocksDbMetrics.COMPACTION_PENDING, RandomGenerator.GetInt32(100) },
        { ROCKSDB_PROPERTIES_PREFIX + RocksDbMetrics.NUMBER_OF_RUNNING_COMPACTIONS, RandomGenerator.GetInt32(100) },
        { ROCKSDB_PROPERTIES_PREFIX + RocksDbMetrics.ESTIMATED_BYTES_OF_PENDING_COMPACTION, RandomGenerator.GetInt32(100) },
        { ROCKSDB_PROPERTIES_PREFIX + RocksDbMetrics.TOTAL_SST_FILES_SIZE, RandomGenerator.GetInt32(100) },
        { ROCKSDB_PROPERTIES_PREFIX + RocksDbMetrics.LIVE_SST_FILES_SIZE, RandomGenerator.GetInt32(100) },
        { ROCKSDB_PROPERTIES_PREFIX + RocksDbMetrics.NUMBER_OF_LIVE_VERSIONS, RandomGenerator.GetInt32(100) },
        { ROCKSDB_PROPERTIES_PREFIX + RocksDbMetrics.CAPACITY_OF_BLOCK_CACHE, RandomGenerator.GetInt32(100) },
        { ROCKSDB_PROPERTIES_PREFIX + RocksDbMetrics.USAGE_OF_BLOCK_CACHE, RandomGenerator.GetInt32(100) },
        { ROCKSDB_PROPERTIES_PREFIX + RocksDbMetrics.PINNED_USAGE_OF_BLOCK_CACHE, RandomGenerator.GetInt32(100) },
        { ROCKSDB_PROPERTIES_PREFIX + RocksDbMetrics.ESTIMATED_NUMBER_OF_KEYS, RandomGenerator.GetInt32(100) },
        { ROCKSDB_PROPERTIES_PREFIX + RocksDbMetrics.ESTIMATED_MEMORY_OF_TABLE_READERS, RandomGenerator.GetInt32(100) },
        { ROCKSDB_PROPERTIES_PREFIX + RocksDbMetrics.NUMBER_OF_BACKGROUND_ERRORS, RandomGenerator.GetInt32(100) }
    };

    private StreamMetricsRegistry metricsRegistry;

    [SetUp]
    public void Init()
    {
        metricsRegistry = new StreamMetricsRegistry("client", MetricsRecordingLevel.DEBUG);

        Mock<IDbProperyProvider> mockDbProvider = new Mock<IDbProperyProvider>();
        mockDbProvider
            .Setup(d => d.GetProperty(It.IsAny<string>()))
            .Returns((string propertyName) => dicMetrics[propertyName]);

        var recorder = new RocksDbMetricsRecorder(METRICS_SCOPE, STORE_NAME);
        recorder.Init(metricsRegistry, TASK_ID);

        recorder.AddValueProviders(STORE_NAME, mockDbProvider.Object);
    }

    [TearDown]
    public void Dispose()
    {
    }

    [Test]
    public void TestNUMBER_OF_ENTRIES_ACTIVE_MEMTABLE()
        => VerifySumOfProperties(RocksDbMetrics.NUMBER_OF_ENTRIES_ACTIVE_MEMTABLE);

    [Test]
    public void TestNUMBER_OF_DELETES_ACTIVE_MEMTABLE()
        => VerifySumOfProperties(RocksDbMetrics.NUMBER_OF_DELETES_ACTIVE_MEMTABLE);

    [Test]
    public void TestNUMBER_OF_ENTRIES_IMMUTABLE_MEMTABLES()
        => VerifySumOfProperties(RocksDbMetrics.NUMBER_OF_ENTRIES_IMMUTABLE_MEMTABLES);

    [Test]
    public void TestNUMBER_OF_DELETES_IMMUTABLE_MEMTABLES()
        => VerifySumOfProperties(RocksDbMetrics.NUMBER_OF_DELETES_IMMUTABLE_MEMTABLES);

    [Test]
    public void TestNUMBER_OF_IMMUTABLE_MEMTABLES()
        => VerifySumOfProperties(RocksDbMetrics.NUMBER_OF_IMMUTABLE_MEMTABLES);

    [Test]
    public void TestCURRENT_SIZE_OF_ACTIVE_MEMTABLE()
        => VerifySumOfProperties(RocksDbMetrics.CURRENT_SIZE_OF_ACTIVE_MEMTABLE);

    [Test]
    public void TestCURRENT_SIZE_OF_ALL_MEMTABLES()
        => VerifySumOfProperties(RocksDbMetrics.CURRENT_SIZE_OF_ALL_MEMTABLES);

    [Test]
    public void TestSIZE_OF_ALL_MEMTABLES()
        => VerifySumOfProperties(RocksDbMetrics.SIZE_OF_ALL_MEMTABLES);

    [Test]
    public void TestMEMTABLE_FLUSH_PENDING()
        => VerifySumOfProperties(RocksDbMetrics.MEMTABLE_FLUSH_PENDING);

    [Test]
    public void TestNUMBER_OF_RUNNING_FLUSHES()
        => VerifySumOfProperties(RocksDbMetrics.NUMBER_OF_RUNNING_FLUSHES);

    [Test]
    public void TestCOMPACTION_PENDING()
        => VerifySumOfProperties(RocksDbMetrics.COMPACTION_PENDING);

    [Test]
    public void TestNUMBER_OF_RUNNING_COMPACTIONS()
        => VerifySumOfProperties(RocksDbMetrics.NUMBER_OF_RUNNING_COMPACTIONS);

    [Test]
    public void TestESTIMATED_BYTES_OF_PENDING_COMPACTION()
        => VerifySumOfProperties(RocksDbMetrics.ESTIMATED_BYTES_OF_PENDING_COMPACTION);

    [Test]
    public void TestTOTAL_SST_FILES_SIZE()
        => VerifySumOfProperties(RocksDbMetrics.TOTAL_SST_FILES_SIZE);

    [Test]
    public void TestLIVE_SST_FILES_SIZE()
        => VerifySumOfProperties(RocksDbMetrics.LIVE_SST_FILES_SIZE);

    [Test]
    public void TestNUMBER_OF_LIVE_VERSIONS()
        => VerifySumOfProperties(RocksDbMetrics.NUMBER_OF_LIVE_VERSIONS);

    [Test]
    public void TestCAPACITY_OF_BLOCK_CACHE()
        => VerifySumOfProperties(RocksDbMetrics.CAPACITY_OF_BLOCK_CACHE);

    [Test]
    public void TestUSAGE_OF_BLOCK_CACHE()
        => VerifySumOfProperties(RocksDbMetrics.USAGE_OF_BLOCK_CACHE);

    [Test]
    public void TestPINNED_USAGE_OF_BLOCK_CACHE()
        => VerifySumOfProperties(RocksDbMetrics.PINNED_USAGE_OF_BLOCK_CACHE);

    [Test]
    public void TestESTIMATED_NUMBER_OF_KEYS()
        => VerifySumOfProperties(RocksDbMetrics.ESTIMATED_NUMBER_OF_KEYS);

    [Test]
    public void TestESTIMATED_MEMORY_OF_TABLE_READERS()
        => VerifySumOfProperties(RocksDbMetrics.ESTIMATED_MEMORY_OF_TABLE_READERS);

    [Test]
    public void TestNUMBER_OF_BACKGROUND_ERRORS()
        => VerifySumOfProperties(RocksDbMetrics.NUMBER_OF_BACKGROUND_ERRORS);

    private void VerifySumOfProperties(string propertyName)
    {
        var expected = dicMetrics[ROCKSDB_PROPERTIES_PREFIX + propertyName];
        VerifyMetrics(propertyName, expected);
    }

    private void VerifyMetrics(string propertyName, long expectedValue)
    {
        var sensor = metricsRegistry
            .GetSensors()
            .FirstOrDefault(s => s.Name.Contains("sensor." + propertyName));

        Assert.NotNull(sensor);

        var metricValue = sensor.Metrics.First().Value.Value;

        Assert.NotNull(metricValue);
        Assert.AreEqual(expectedValue, (long)metricValue);
    }
}