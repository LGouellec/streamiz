using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using NUnit.Framework;
using RocksDbSharp;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State.Helper;
using Streamiz.Kafka.Net.State.RocksDb;

namespace Streamiz.Kafka.Net.Tests
{
    public class ReproducerIssue185
    {
        private const Compression COMPRESSION_TYPE = Compression.No;
        private const Compaction COMPACTION_STYLE = Compaction.Universal;
        private const long WRITE_BUFFER_SIZE = 16 * 1024 * 1024L;
        private const long BLOCK_CACHE_SIZE = 50 * 1024 * 1024L;
        private const long BLOCK_SIZE = 4096L;
        private const int MAX_WRITE_BUFFERS = 3;
        private const string DB_FILE_DIR = "rocksdb";
        
        private DirectoryInfo DbDir;
        private ColumnFamilyHandle columnFamilyHandle;
        private WriteOptions writeOptions;
        private int seq = 0;
        public RocksDb Db { get; set; }
        
        private void OpenRocksDb(DbOptions dbOptions, ColumnFamilyOptions columnFamilyOptions)
        {
            int maxRetries = 5;
            int i = 0;
            bool open = false;
            RocksDbException rocksDbException = null;

            var columnFamilyDescriptors = new ColumnFamilies(columnFamilyOptions);

            while (!open && i < maxRetries)
            {
                try
                {
                    Db = RocksDbSharp.RocksDb.Open(
                        dbOptions,
                        DbDir.FullName,
                        columnFamilyDescriptors);

                    columnFamilyHandle = Db.GetDefaultColumnFamily();
                    open = true;
                }
                catch (RocksDbException e)
                {
                    ++i;
                    rocksDbException = e;
                }
            }
        }

        [SetUp]
        public void Init()
        {
            DbOptions dbOptions = new DbOptions();
            ColumnFamilyOptions columnFamilyOptions = new ColumnFamilyOptions();
            writeOptions = new WriteOptions();
            BlockBasedTableOptions tableConfig = new BlockBasedTableOptions();

            RocksDbOptions rocksDbOptions = new RocksDbOptions(dbOptions, columnFamilyOptions);

            tableConfig.SetBlockCache(RocksDbSharp.Cache.CreateLru(BLOCK_CACHE_SIZE));
            tableConfig.SetBlockSize(BLOCK_SIZE);
            tableConfig.SetFilterPolicy(BloomFilterPolicy.Create());

            rocksDbOptions.SetOptimizeFiltersForHits(1);
            rocksDbOptions.SetBlockBasedTableFactory(tableConfig);
            rocksDbOptions.SetCompression(COMPRESSION_TYPE);
            rocksDbOptions.SetWriteBufferSize(WRITE_BUFFER_SIZE);
            rocksDbOptions.SetCompactionStyle(COMPACTION_STYLE);
            rocksDbOptions.SetMaxWriteBufferNumber(MAX_WRITE_BUFFERS);
            rocksDbOptions.SetCreateIfMissing(true);
            rocksDbOptions.SetErrorIfExists(false);
            rocksDbOptions.SetInfoLogLevel(InfoLogLevel.Error);
            // this is the recommended way to increase parallelism in RocksDb
            // note that the current implementation of setIncreaseParallelism affects the number
            // of compaction threads but not flush threads (the latter remains one). Also
            // the parallelism value needs to be at least two because of the code in
            // https://github.com/facebook/rocksdb/blob/62ad0a9b19f0be4cefa70b6b32876e764b7f3c11/util/options.cc#L580
            // subtracts one from the value passed to determine the number of compaction threads
            // (this could be a bug in the RocksDB code and their devs have been contacted).
            rocksDbOptions.IncreaseParallelism(Math.Max(Environment.ProcessorCount, 2));
            
            // TODO : wrap writeOptions in rocksDbOptions too
            writeOptions.DisableWal(1);
            rocksDbOptions.SetMinWriteBufferNumberToMerge(2);
            
            DbDir = new DirectoryInfo(Path.Combine(".", "test-reproducer-185", Guid.NewGuid().ToString()));

            Directory.CreateDirectory(DbDir.FullName);

            OpenRocksDb(dbOptions, columnFamilyOptions);
        }

        [TearDown]
        public void Dispose()
        {
            Db.Dispose();
            Directory.Delete(DbDir.FullName, true);
        }
        
        private byte[] StringToBytes(string value)
        {
            return Encoding.UTF8.GetBytes(value);
        }
        
        private byte[] Key(long timestamp, string key)
        {
            return WindowKeyHelper.ToStoreKeyBinary(StringToBytes(key), timestamp, ++seq).Get;
        }

        [Test]
        public void ReproducerIssueSeekIterator()
        {
            string key = "key";
            DateTime now = DateTime.Now, dt2 = now.AddMinutes(1);
            for (int i = 0; i < 100; ++i)
            {
                Db.Put(
                    Key(dt2.GetMilliseconds(), key),
                    StringToBytes("value"+i),
                    columnFamilyHandle,
                    writeOptions);
                dt2 = dt2.AddMinutes(1);
            }

            var data = new List<(string, string)>();

            var k = Key(now.GetMilliseconds(), key);
            var iterator = Db.NewIterator(columnFamilyHandle).Seek(k);
            
            while (iterator.Valid())
            {
                string currentKey = WindowKeyHelper.ExtractStoreKey(iterator.Key(), new StringSerDes());
                long currentTS = WindowKeyHelper.ExtractStoreTimestamp(iterator.Key());
                data.Add(($"Key : {currentKey}, Ts : {currentTS.FromMilliseconds()}", iterator.StringValue()));
                iterator.Next();
            }
            
            iterator.Detach();
            iterator.Dispose();
            
            Console.WriteLine(data.Count);
            foreach(var kv in data)
                Console.WriteLine(kv.Item1 + "|" + kv.Item2);

        }
    }
}