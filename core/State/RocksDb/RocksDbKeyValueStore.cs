using RocksDbSharp;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Processors;
using System;
using System.Collections.Generic;
using System.IO;

namespace Streamiz.Kafka.Net.State.RocksDb
{
    public class RocksDbKeyValueStore : IKeyValueStore<Bytes, byte[]>
    {
        private RocksDbOptions rocksDbOptions;

        private const Compression COMPRESSION_TYPE = Compression.No;
        private const Compaction COMPACTION_STYLE = Compaction.Universal;
        private const long WRITE_BUFFER_SIZE = 16 * 1024 * 1024L;
        private const long BLOCK_CACHE_SIZE = 50 * 1024 * 1024L;
        private const long BLOCK_SIZE = 4096L;
        private const int MAX_WRITE_BUFFERS = 3;
        private const String DB_FILE_DIR = "rocksdb";

        private WriteOptions writeOptions;

        internal DirectoryInfo DbDir { get; private set; }
        internal RocksDbSharp.RocksDb Db { get; set; }
        internal IRocksDbAdapter DbAdapter { get; private set; }
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
            CheckStateStoreOpen();
            long num = 0;
            try
            {
                num = DbAdapter.ApproximateNumEntries();
            }
            catch (RocksDbSharp.RocksDbException e)
            {
                throw new ProcessorStateException("Error while getting value for key from store {Name}", e);
            }

            if (num < 0)
                num = Int64.MaxValue;

            return num;
        }

        public void Close()
        {
            if (!IsOpen)
                return;
            
            IsOpen = false;
            DbAdapter.Close();
            Db.Dispose();

            DbAdapter = null;
            Db = null;
        }

        public byte[] Delete(Bytes key)
        {
            CheckStateStoreOpen();
            byte[] oldValue = null;

            try
            {
                oldValue = DbAdapter.GetOnly(key.Get);
            }
            catch (RocksDbSharp.RocksDbException e){
                throw new ProcessorStateException("Error while getting value for key from store {Name}", e);
            }

            Put(key, null);
            return oldValue;
        }

        public void Flush()
        {
            CheckStateStoreOpen();
            if (Db == null)
                return;
            
            try
            {
                DbAdapter.Flush();
            }
            catch (RocksDbSharp.RocksDbException e)
            {
                throw new ProcessorStateException("Error while getting value for key from store {Name}", e);
            }
        }

        public byte[] Get(Bytes key)
        {
            CheckStateStoreOpen();
            try
            {
                return DbAdapter.Get(key.Get);
            }
            catch (RocksDbSharp.RocksDbException e) {
                throw new ProcessorStateException($"Error while getting value for key from store {Name}", e);
            }
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
            CheckStateStoreOpen();
            DbAdapter.Put(key.Get, value);
        }

        public void PutAll(IEnumerable<KeyValuePair<Bytes, byte[]>> entries)
        {
           
        }

        public byte[] PutIfAbsent(Bytes key, byte[] value)
        {
            var originalValue = Get(key);
            if (originalValue == null)
                Put(key, value);

            return originalValue;
        }

        #endregion

        #region Private

        private void OpenDatabase(ProcessorContext context)
        {
            DbOptions dbOptions = new DbOptions();
            ColumnFamilyOptions columnFamilyOptions = new ColumnFamilyOptions();
            BlockBasedTableOptions tableConfig = new BlockBasedTableOptions();

            rocksDbOptions = new RocksDbOptions(dbOptions, columnFamilyOptions);

            tableConfig.SetBlockCache(RocksDbSharp.Cache.CreateLru(BLOCK_CACHE_SIZE));
            tableConfig.SetBlockSize(BLOCK_SIZE);
            tableConfig.SetFilterPolicy(BloomFilterPolicy.Create());

            rocksDbOptions.SetOptimizeFiltersForHits(1);
            rocksDbOptions.SetBlockBasedTableFactory(tableConfig);
            rocksDbOptions.SetCompression(COMPRESSION_TYPE);
            rocksDbOptions.SetCompactionStyle(COMPACTION_STYLE);
            rocksDbOptions.SetMaxWriteBufferNumber(MAX_WRITE_BUFFERS);
            rocksDbOptions.SetCreateIfMissing(true);
            rocksDbOptions.SetErrorIfExists(false);
            rocksDbOptions.SetInfoLogLevel(RocksLogLevel.ERROR);
            // this is the recommended way to increase parallelism in RocksDb
            // note that the current implementation of setIncreaseParallelism affects the number
            // of compaction threads but not flush threads (the latter remains one). Also
            // the parallelism value needs to be at least two because of the code in
            // https://github.com/facebook/rocksdb/blob/62ad0a9b19f0be4cefa70b6b32876e764b7f3c11/util/options.cc#L580
            // subtracts one from the value passed to determine the number of compaction threads
            // (this could be a bug in the RocksDB code and their devs have been contacted).
            rocksDbOptions.IncreaseParallelism(Math.Max(Environment.ProcessorCount, 2));

            writeOptions = new WriteOptions();
            writeOptions.DisableWal(1);

            context.Configuration.RocksDbConfigHandler?.Invoke(Name, rocksDbOptions);

            DbDir = new DirectoryInfo(Path.Combine(context.StateDir, DB_FILE_DIR, Name));

            Directory.CreateDirectory(DbDir.FullName);

            OpenRocksDB(dbOptions, columnFamilyOptions);

            IsOpen = true;
        }

        private void OpenRocksDB(DbOptions dbOptions, ColumnFamilyOptions columnFamilyOptions)
        {
            var columnFamilyDescriptors = new ColumnFamilies(columnFamilyOptions);

            try
            {
                Db = RocksDbSharp.RocksDb.Open(
                    dbOptions,
                    DbDir.FullName,
                    columnFamilyDescriptors);

                var columnFamilyHandle = Db.GetDefaultColumnFamily();
                DbAdapter = new SingleColumnFamilyAdapter(
                    Name,
                    Db,
                    writeOptions,
                    columnFamilyHandle);
            }
            catch (RocksDbException e)
            {
                throw new ProcessorStateException("Error opening store " + Name + " at location " + DbDir.ToString(), e);
            }
        }

        private void CheckStateStoreOpen()
        {
            if (!IsOpen)
            {
                throw new InvalidStateStoreException($"Store {Name} is currently closed");
            }
        }

        #endregion
    }
}