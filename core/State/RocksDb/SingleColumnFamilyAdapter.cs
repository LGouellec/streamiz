using RocksDbSharp;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.State.Enumerator;
using System;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.State.RocksDb
{
    internal class SingleColumnFamilyAdapter : IRocksDbAdapter
    {
        private readonly string name;
        private readonly RocksDbSharp.RocksDb db;
        private readonly WriteOptions writeOptions;
        private readonly ColumnFamilyHandle columnFamilyHandle;

        public SingleColumnFamilyAdapter(string name, RocksDbSharp.RocksDb db, WriteOptions writeOptions, ColumnFamilyHandle columnFamilyHandle)
        {
            this.name = name;
            this.db = db;
            this.writeOptions = writeOptions;
            this.columnFamilyHandle = columnFamilyHandle;
        }

        public void AddToBatch(byte[] key, byte[] value, WriteBatch batch)
        {
            throw new NotImplementedException();
        }

        public IKeyValueEnumerator<Bytes, byte[]> All()
        {
            throw new NotImplementedException();
        }
        
        public long ApproximateNumEntries()
            => long.Parse(db.GetProperty("rocksdb.estimate-num-keys", columnFamilyHandle));

        public void Close() { }

        public void Flush() { }

        public byte[] Get(byte[] key)
            => db.Get(key, columnFamilyHandle);

        public byte[] GetOnly(byte[] key)
            => db.Get(key, columnFamilyHandle);

        public void PrepareBatch(List<KeyValuePair<Bytes, byte[]>> entries, WriteBatch batch)
        {
            throw new NotImplementedException();
        }

        public void PrepareBatchForRestore(List<KeyValuePair<byte[], byte[]>> records, WriteBatch batch)
        {
            throw new NotImplementedException();
        }

        public void Put(byte[] key, byte[] value)
        {
            if (value == null)
            {
                try
                {
                    db.Remove(key, columnFamilyHandle, writeOptions);
                }
                catch (RocksDbException e)
                {
                    throw new ProcessorStateException($"Error while removing key from store {name}", e);
                }
            }
            else
            {
                try
                {
                    db.Put(key, value, columnFamilyHandle, writeOptions);
                }
                catch (RocksDbException e)
                {
                    throw new ProcessorStateException($"Error while putting key/value into store {name}", e);
                }
            }
        }

        public IKeyValueEnumerator<Bytes, byte[]> Range(Bytes from, Bytes to)
        {
            throw new NotImplementedException();
        }

        public void ToggleDbForBulkLoading()
        {
            throw new NotImplementedException();
        }
    }
}