using RocksDbSharp;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Processors;
using System;
using System.Collections.Generic;
using System.Text;

namespace Streamiz.Kafka.Net.State.RocksDb
{
    public class RocksDbKeyValueStore : IKeyValueStore<Bytes, byte[]>
    {
        private RocksDbOptions rocksDbOptions;

        internal ProcessorContext InternalProcessorContext { get; set; }

        public RocksDbKeyValueStore(string name)
        {
            Name = name;
        }

        #region Store Impl

        public string Name { get; }

        public bool Persistent => true;

        public bool IsOpen { get; private set; }


        public IEnumerable<KeyValuePair<Bytes, byte[]>> All()
        {
            throw new NotImplementedException();
        }

        public long ApproximateNumEntries()
        {
            throw new NotImplementedException();
        }

        public void Close()
        {
            throw new NotImplementedException();
        }

        public byte[] Delete(Bytes key)
        {
            throw new NotImplementedException();
        }

        public void Flush()
        {
            throw new NotImplementedException();
        }

        public byte[] Get(Bytes key)
        {
            throw new NotImplementedException();
        }

        public void Init(ProcessorContext context, IStateStore root)
        {
            InternalProcessorContext = context;
            OpenDatabase(context);

            // TODO : batch behavior
            context.Register(root, (k, v) => Put(k, v));
        }

        public void Put(Bytes key, byte[] value)
        {
            throw new NotImplementedException();
        }

        public void PutAll(IEnumerable<KeyValuePair<Bytes, byte[]>> entries)
        {
            throw new NotImplementedException();
        }

        public byte[] PutIfAbsent(Bytes key, byte[] value)
        {
            throw new NotImplementedException();
        }

        #endregion

        #region Private

        private void OpenDatabase(ProcessorContext context)
        {
            // TODO : open rocksdb database
            DbOptions dbOptions = new DbOptions();
            ColumnFamilyOptions columnFamilyOptions = new ColumnFamilyOptions();

            rocksDbOptions = new RocksDbOptions(dbOptions, columnFamilyOptions);

            // userSpecifiedOptions = new RocksDBGenericOptionsToDbOptionsColumnFamilyOptionsAdapter(dbOptions, columnFamilyOptions);

            BlockBasedTableConfigWithAccessibleCache tableConfig = new BlockBasedTableConfigWithAccessibleCache();
            cache = new LRUCache(BLOCK_CACHE_SIZE);
            tableConfig.setBlockCache(cache);
            tableConfig.setBlockSize(BLOCK_SIZE);

            filter = new BloomFilter();
            tableConfig.setFilter(filter);

            userSpecifiedOptions.optimizeFiltersForHits();
            userSpecifiedOptions.setTableFormatConfig(tableConfig);
            userSpecifiedOptions.setWriteBufferSize(WRITE_BUFFER_SIZE);
            userSpecifiedOptions.setCompressionType(COMPRESSION_TYPE);
            userSpecifiedOptions.setCompactionStyle(COMPACTION_STYLE);
            userSpecifiedOptions.setMaxWriteBufferNumber(MAX_WRITE_BUFFERS);
            userSpecifiedOptions.setCreateIfMissing(true);
            userSpecifiedOptions.setErrorIfExists(false);
            userSpecifiedOptions.setInfoLogLevel(InfoLogLevel.ERROR_LEVEL);
            // this is the recommended way to increase parallelism in RocksDb
            // note that the current implementation of setIncreaseParallelism affects the number
            // of compaction threads but not flush threads (the latter remains one). Also
            // the parallelism value needs to be at least two because of the code in
            // https://github.com/facebook/rocksdb/blob/62ad0a9b19f0be4cefa70b6b32876e764b7f3c11/util/options.cc#L580
            // subtracts one from the value passed to determine the number of compaction threads
            // (this could be a bug in the RocksDB code and their devs have been contacted).
            userSpecifiedOptions.setIncreaseParallelism(Math.max(Runtime.getRuntime().availableProcessors(), 2));

            wOptions = new WriteOptions();
            wOptions.setDisableWAL(true);

            fOptions = new FlushOptions();
            fOptions.setWaitForFlush(true);

             Class<RocksDBConfigSetter> configSetterClass =
                (Class<RocksDBConfigSetter>)configs.get(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG);

            if (configSetterClass != null)
            {
                configSetter = Utils.newInstance(configSetterClass);
                configSetter.setConfig(name, userSpecifiedOptions, configs);
            }

            dbDir = new File(new File(stateDir, parentDir), name);

            try
            {
                Files.createDirectories(dbDir.getParentFile().toPath());
                Files.createDirectories(dbDir.getAbsoluteFile().toPath());
            }
            catch ( IOException fatal) {
                throw new ProcessorStateException(fatal);
            }

            // Setup statistics before the database is opened, otherwise the statistics are not updated
            // with the measurements from Rocks DB
            maybeSetUpStatistics(configs);

            openRocksDB(dbOptions, columnFamilyOptions);
            open = true;

            addValueProvidersToMetricsRecorder();


            IsOpen = true;
        }

        private void CheckStateStoreOpen()
        {
            if (!IsOpen)
                throw new InvalidStateStoreException($"Store {Name} is currently closed");
        }

        #endregion
    }
}
