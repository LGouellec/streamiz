using System;
using System.Collections.Generic;
using System.IO;
using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.State.RocksDb;
using Streamiz.Kafka.Net.Table;
using Moq;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Kafka.Net.Tests.Stores;

public class BoundMemoryRocksDbConfigHandlerTests
{
    private void MemoryBoundedOptionsImpl(BoundMemoryRocksDbConfigHandler handler)
    {
        Bytes Key(string k)
        {
            StringSerDes s = new StringSerDes();
            return new Bytes(s.Serialize(k, SerializationContext.Empty));
        }

        byte[] Value(Int32 number)
        {
            Int32SerDes i = new Int32SerDes();
            return i.Serialize(number, SerializationContext.Empty);
        }
        
        bool stop = false;

        var config = new StreamConfig();
        config.RocksDbConfigHandler = handler.Handle;

        var stateManager = new Mock<IStateManager>();
        stateManager.Setup(s => 
            s.Register(It.IsAny<IStateStore>(), It.IsAny<Action<ConsumeResult<byte[], byte[]>>>()));
        
        var context = new Mock<ProcessorContext>();
        context.Setup(c => c.Id)
            .Returns(new TaskId { Id = 0, Partition = 0 });
        context.Setup(c => c.StateDir)
            .Returns("./bound-test/");
        context.Setup(c => c.Configuration)
            .Returns(config);
        context.Setup(c => c.States)
            .Returns(() => stateManager.Object);

        ulong totalLogged = 0L;
        
        RocksDbKeyValueStore store = new RocksDbKeyValueStore("store", "rocksdb");
        store.Init(context.Object, store);

        List<Bytes> allKeys = new List<Bytes>();
        while (!stop)
        {
            var key = RandomGenerator.GetRandomString();
            var value = RandomGenerator.GetInt32(Int32.MaxValue);

            var rawKey = Key(key);
            store.Put(Key(key), Value(value));
            allKeys.Add(rawKey);
            totalLogged += Convert.ToUInt32(key.Length + sizeof(Int32));
            stop = totalLogged > 2 * handler.CacheSizeCapacity; // stop if 2 * totalUnmanagedMemory has pushed
        }

        long memtableSizeAfterWrite = Convert.ToInt32(store.Db.GetProperty("rocksdb.cur-size-all-mem-tables"));
        long blockCacheSizeBeforeRead = Convert.ToInt32(store.Db.GetProperty("rocksdb.block-cache-usage"));
        
        store.Flush();
        
        foreach (var key in allKeys)
            store.Get(key);
        
        long memtableSizeAfterRead = Convert.ToInt32(store.Db.GetProperty("rocksdb.cur-size-all-mem-tables"));
        long blockCacheSizeAfterRead = Convert.ToInt32(store.Db.GetProperty("rocksdb.block-cache-usage"));
        
        store.Close();
        Directory.Delete(context.Object.StateDir, true);
        
        Assert.IsTrue(Convert.ToUInt32(memtableSizeAfterWrite + blockCacheSizeBeforeRead) < handler.CacheSizeCapacity);
        Assert.IsTrue(Convert.ToUInt32(memtableSizeAfterRead + blockCacheSizeAfterRead) < handler.CacheSizeCapacity);
    }
    
    [Test]
    [NonParallelizable]
    public void CheckWithMemoryBoundedOptions()
    {
        CacheSize totalUnmanagedMemory = CacheSize.OfMb(10);
        BoundMemoryRocksDbConfigHandler handler = new BoundMemoryRocksDbConfigHandler();
        handler
            .LimitTotalMemory(totalUnmanagedMemory);
        
        MemoryBoundedOptionsImpl(handler);
    }
    
    [Test]
    [NonParallelizable]
    public void MutualizeCacheMemoryBoundedOptions()
    {
        CacheSize totalUnmanagedMemory = CacheSize.OfMb(10);
        BoundMemoryRocksDbConfigHandler handler = new BoundMemoryRocksDbConfigHandler();
        handler
            .LimitTotalMemory(totalUnmanagedMemory,true);
        
        MemoryBoundedOptionsImpl(handler);
    }
}