using System;
using RocksDbSharp;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Kafka.Net.State
{
    public class BoundMemoryRocksDbConfigHandler
    {
        private bool configured;
        private Compaction compactionStyle;
        private Compression compressionType;
        private int maxNumberOfThreads;
        
        private IntPtr writeBufferManager;
        private IntPtr blockCachePtr;

        public BoundMemoryRocksDbConfigHandler SetCompactionStyle(Compaction compaction)
        {
            compactionStyle = compaction;
            return this;
        }

        public BoundMemoryRocksDbConfigHandler SetCompressionType(Compression compression)
        {
            compressionType = compression;
            return this;
        }

        public BoundMemoryRocksDbConfigHandler LimitTotalMemory(CacheSize maximumCacheSize)
            => LimitTotalMemory(Convert.ToUInt32(maximumCacheSize.CacheSizeBytes));
        
        public BoundMemoryRocksDbConfigHandler ConfigureNumThreads(int numberOfThreads)
        {
            maxNumberOfThreads = numberOfThreads;
            return this;
        }
        
        private BoundMemoryRocksDbConfigHandler LimitTotalMemory(ulong totalUnManagedMemory)
        {
            if (configured)
                throw new IllegalStateException(
                    "BoundMemoryRocksDbConfigHandler is already configured ! To avoid multiple block cache and writer buffer manager allocation, an inner exception is throw. It was due to a bug or misconfiguration in your side");
            
            configured = true;
            ulong blockCacheSize = totalUnManagedMemory / 2;
            ulong totalMemtableMemory = totalUnManagedMemory / 2;
            
            // block cache allocator
            Native.Instance.rocksdb_jemalloc_nodump_allocator_create(out IntPtr jemallocPtr);
            IntPtr LRUCacheOptionsPtr = Native.Instance.rocksdb_lru_cache_options_create();
            Native.Instance.rocksdb_lru_cache_options_set_capacity(LRUCacheOptionsPtr, new UIntPtr(blockCacheSize));
            Native.Instance.rocksdb_lru_cache_options_set_num_shard_bits(LRUCacheOptionsPtr, -1);
            Native.Instance.rocksdb_lru_cache_options_set_memory_allocator(LRUCacheOptionsPtr, jemallocPtr);
            
            blockCachePtr = Native.Instance.rocksdb_cache_create_lru_opts(LRUCacheOptionsPtr);
            
            // wbm allocator
            IntPtr LRUWriteCacheOptionsPtr = Native.Instance.rocksdb_lru_cache_options_create();
            Native.Instance.rocksdb_lru_cache_options_set_capacity(LRUWriteCacheOptionsPtr, new UIntPtr(totalMemtableMemory));
            Native.Instance.rocksdb_lru_cache_options_set_num_shard_bits(LRUWriteCacheOptionsPtr, -1);
            Native.Instance.rocksdb_lru_cache_options_set_memory_allocator(LRUWriteCacheOptionsPtr, jemallocPtr);

            writeBufferManager = Native.Instance.rocksdb_write_buffer_manager_create_with_cache(
                new UIntPtr(totalMemtableMemory),
                LRUWriteCacheOptionsPtr,
                false);
            return this;
        }
        
        
        public void Handle(string storeName, RocksDbOptions options)
        {
            options.SetCompactionStyle(compactionStyle);
            options.SetCompression(compressionType);
            
            var tableConfig = new BlockBasedTableOptions();
            tableConfig.SetBlockCache(blockCachePtr); // use the same block cache for each state store
            tableConfig.SetBlockSize(4096L); // 4Kb
            tableConfig.SetFilterPolicy(BloomFilterPolicy.Create());
            Native.Instance.rocksdb_block_based_options_set_cache_index_and_filter_blocks_with_high_priority(
                tableConfig.Handle, true);
            Native.Instance.rocksdb_block_based_options_set_pin_top_level_index_and_filter(tableConfig.Handle, true);
            options.SetBlockBasedTableFactory(tableConfig);
            
            options.SetWriteBufferManager(writeBufferManager);

            options.SetStatsDumpPeriodSec(0);
            options.IncreaseParallelism(Math.Max(maxNumberOfThreads, 2));
        }
    }
}