﻿using Confluent.Kafka;
using Moq;
using NUnit.Framework;
using RocksDbSharp;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Tests.Helpers;
using System;
using System.Collections.Generic;
using System.IO;
using Streamiz.Kafka.Net.Metrics;


namespace Streamiz.Kafka.Net.Tests.Public
{
    public class RocksDbOptionsTests
    {
        #region Cst
        private const int parallelism = 1;
        private const uint bloomLocality = 50;
        private const int baseBackgroundCompactions = 100;
        private const int accessHintOnCompactionStart = 1;
        private const ulong blockCacheSizeMb = 100;
        private const ulong memTableMemoryBudget = 5;
        private const ulong arenaBlockSize = 100;
        private const ulong memtableMemoryBudget2 = 6;
        private const ulong bytesPerSync = 600;
        private const bool adviseRandomOnOpen = false;
        private const bool allowConcurrentMemtableWrite = false;
        #endregion

        private StreamConfig config = null;
        private RocksDbKeyValueStore store = null;
        private ProcessorContext context = null;
        private TaskId id = null;
        private TopicPartition partition = null;
        private ProcessorStateManager stateManager = null;
        private Mock<AbstractTask> task = null;

        [SetUp]
        public void Begin()
        {
            Random rd = new Random();
            config = new StreamConfig();
            config.ApplicationId = $"RocksDbOptionsTests";
            config.UseRandomRocksDbConfigForTest();
            config.RocksDbConfigHandler = (name, options) =>
            {
                options
                    .EnableStatistics()
                    .IncreaseParallelism(parallelism)
                    .OptimizeForPointLookup(blockCacheSizeMb)
                    .OptimizeLevelStyleCompaction(memTableMemoryBudget)
                    .OptimizeUniversalStyleCompaction(memtableMemoryBudget2)
                    .PrepareForBulkLoad()
                    .SetAdviseRandomOnOpen(adviseRandomOnOpen)
                    .SetAllowConcurrentMemtableWrite(allowConcurrentMemtableWrite)
                    .SetAllowMmapReads(false)
                    .SetAllowMmapWrites(false)
                    .SetArenaBlockSize(arenaBlockSize)
                    .SetBloomLocality(bloomLocality)
                    .SetBytesPerSync(bytesPerSync)
                    .SetCompactionReadaheadSize(1200)
                    .SetCompactionStyle(RocksDbSharp.Compaction.Level)
                    .SetCompression(RocksDbSharp.Compression.Lz4)
                    .SetCompressionOptions(1, 2, 3, 4)
                    .SetCompressionPerLevel(new[] { RocksDbSharp.Compression.Lz4 }, 1)
                    .SetCreateIfMissing()
                    .SetCreateMissingColumnFamilies()
                    .SetDbLogDir("test")
                    .SetDbWriteBufferSize(1000)
                    .SetDeleteObsoleteFilesPeriodMicros(50)
                    .SetDisableAutoCompactions(1)
                    .SetEnableWriteThreadAdaptiveYield(true)
                    .SetErrorIfExists(false)
                    .SetHardPendingCompactionBytesLimit(1)
                    .SetHashLinkListRep(12)
                    .SetHashSkipListRep(56, 4, 2)
                    //.SetInfoLog(IntPtr.Zero)
                    .SetInfoLogLevel(InfoLogLevel.Debug)
                    .SetInplaceUpdateNumLocks(134)
                    .SetIsFdCloseOnExec(false)
                    .SetKeepLogFileNum(1)
                    .SetLevel0FileNumCompactionTrigger(14)
                    .SetLevel0SlowdownWritesTrigger(144)
                    .SetInplaceUpdateSupport(true)
                    .SetLevel0StopWritesTrigger(24)
                    .SetLevelCompactionDynamicLevelBytes(true)
                    .SetLogFileTimeToRoll(154)
                    .SetManifestPreallocationSize(153)
                    .SetMaxBackgroundFlushes(3)
                    .SetMaxBytesForLevelBase(1453)
                    .SetMaxBytesForLevelMultiplier(2)
                    .SetMaxBytesForLevelMultiplierAdditional(new[] { 1 }, 1)
                    .SetMaxCompactionBytes(345678)
                    .SetMaxFileOpeningThreads(2)
                    .SetMaxLogFileSize(1)
                    .SetMaxManifestFileSize(131)
                    .SetMaxOpenFiles(20)
                    .SetMaxSequentialSkipInIterations(13)
                    .SetMaxSuccessiveMerges(12)
                    .SetMaxTotalWalSize(12)
                    .SetMaxWriteBufferNumber(15543)
                    .SetMaxWriteBufferNumberToMaintain(126)
                    .SetMemtableHugePageSize(64317)
                    .SetMemtableHugePageSize(64317)
                    .SetMemtablePrefixBloomSizeRatio(12)
                    //.SetMergeOperator(IntPtr.Zero)
                    .SetMinLevelToCompress(1)
                    .SetMemtableVectorRep()
                    .SetMinWriteBufferNumberToMerge(1312)
                    .SetNumLevels(1)
                    .SetOptimizeFiltersForHits(56)
                    .SetParanoidChecks()
                    .SetSoftPendingCompactionBytesLimit(134)
                    .SetStatsDumpPeriodSec(532)
                    .SetTableCacheNumShardbits(12)
                    .SetTargetFileSizeBase(6)
                    .SetTargetFileSizeMultiplier(2)
                    .SetUint64addMergeOperator()
                    //.SetUniversalCompactionOptions(IntPtr.Zero)
                    .SetUseAdaptiveMutex(false)
                    .SetUseDirectIoForFlushAndCompaction(true)
                    .SetUseDirectReads(true)
                    .SetUseFsync(1)
                    .SetWalRecoveryMode(RocksDbSharp.Recovery.SkipAnyCorruptedRecords)
                    .SetWALSizeLimitMB(40)
                    .SetWALTtlSeconds(1454151413)
                    .SetWriteBufferSize(45678976543)
                    .SetPrefixExtractor(SliceTransform.CreateNoOp())
                    .SetRecycleLogFileNum(1)
                    .SetReportBgIoStats(true)
                    .SetMaxBackgroundCompactions(1);
            };

            id = new TaskId { Id = 0, Partition = 0 };
            partition = new TopicPartition("source", 0);
            stateManager = new ProcessorStateManager(
                id,
                new List<TopicPartition> { partition },
                null,
                new MockChangelogRegister(),
                new MockOffsetCheckpointManager());

            task = new Mock<AbstractTask>();
            task.Setup(k => k.Id).Returns(id);

            context = new ProcessorContext(task.Object, config, stateManager, new StreamMetricsRegistry());

            store = new RocksDbKeyValueStore("test-store", "rocksdb");
            store.Init(context, store);
        }

        [TearDown]
        public void End()
        {
            store.Flush();
            stateManager.Close();
            Directory.Delete(Path.Combine(config.StateDir, config.ApplicationId), true);
        }

        [Test]
        public void RocksDbOptionsTest()
        {
            int a = 1;
        }

    }
}
