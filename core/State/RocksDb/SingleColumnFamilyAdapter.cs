using RocksDbSharp;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.State.Enumerator;
using System;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.State
{
    internal class SingleColumnFamilyAdapter : IRocksDbAdapter
    {
        private readonly string name;
        private readonly RocksDbSharp.RocksDb db;
        private readonly WriteOptions writeOptions;
        private readonly FlushOptions flushOptions;
        private readonly Func<byte[], byte[], int> keyComparator;
        private readonly ColumnFamilyHandle columnFamilyHandle;

        public SingleColumnFamilyAdapter(string name, RocksDbSharp.RocksDb db, WriteOptions writeOptions, Func<byte[], byte[], int> keyComparator, ColumnFamilyHandle columnFamilyHandle)
        {
            this.name = name;
            this.db = db;
            this.writeOptions = writeOptions;
            this.keyComparator = keyComparator;
            this.columnFamilyHandle = columnFamilyHandle;
            flushOptions = new();
            flushOptions.SetWaitForFlush(true);
        }

        public void AddToBatch(byte[] key, byte[] value, WriteBatch batch)
        {
            if (value == null)
                batch.Delete(key, columnFamilyHandle);
            else
                batch.Put(key, value, columnFamilyHandle);
        }

        public IKeyValueEnumerator<Bytes, byte[]> All(bool forward)
        {
            var iterator = db.NewIterator(columnFamilyHandle);
            if (forward)
                iterator.SeekToFirst();
            else
                iterator.SeekToLast();
            return new RocksDbEnumerator(iterator, name, forward);
        }
        
        public long ApproximateNumEntries()
            => long.Parse(db.GetProperty("rocksdb.estimate-num-keys", columnFamilyHandle));

        public void Close() { }

        public void Flush() => db.Flush(flushOptions);

        public byte[] Get(byte[] key)
            => db.Get(key, columnFamilyHandle);

        public byte[] GetOnly(byte[] key)
            => db.Get(key, columnFamilyHandle);

        public void PrepareBatch(IEnumerable<KeyValuePair<Bytes, byte[]>> entries, WriteBatch batch)
        {
            foreach (var entry in entries)
                AddToBatch(entry.Key.Get, entry.Value, batch);
        }

        public void PrepareBatchForRestore(IEnumerable<KeyValuePair<byte[], byte[]>> records, WriteBatch batch)
        {
            foreach (var entry in records)
                AddToBatch(entry.Key, entry.Value, batch);
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

        public IKeyValueEnumerator<Bytes, byte[]> Range(Bytes from, Bytes to, bool forward)
        {
            //ReadOptions readOptions = new ReadOptions();
            //readOptions.SetTotalOrderSeek(true);
           // readOptions.SetIterateLowerBound(from.Get, (ulong)from.Get.Length);
           // readOptions.SetIterateUpperBound(to.Get, (ulong) to.Get.Length);
           return new RocksDbRangeEnumerator(
                db.NewIterator(columnFamilyHandle/*, readOptions*/),
                name,
                from,
                to,
                keyComparator,
                forward);
        }
    }
}