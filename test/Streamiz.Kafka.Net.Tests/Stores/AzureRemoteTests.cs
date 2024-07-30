using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading;
using Azure;
using Azure.Data.Tables;
using Moq;
using NUnit.Framework;
using Streamiz.Kafka.Net.Azure.RemoteStorage;
using Streamiz.Kafka.Net.Azure.RemoteStorage.Internal;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.State;

namespace Streamiz.Kafka.Net.Tests.Stores;

public class AzureRemoteTests
{
    private StreamConfig config;
    private Mock<ProcessorContext> context;
    private MockAzureStore store;

    private class MockAzureStore : AzureTableStore
    {
        public Dictionary<string, Dictionary<string, AzureTableEntity>> data = new();
        
        public MockAzureStore(
            AzureRemoteStorageOptions azureRemoteStorageOptions,
            string name) 
            : base(azureRemoteStorageOptions, name)
        {
        }

        protected override TableClient CreateTableClient()
        {
            var tableClient = new Mock<TableClient>();
            tableClient
                .Setup(client =>
                    client.UpsertEntity(It.IsAny<AzureTableEntity>(), TableUpdateMode.Replace, CancellationToken.None))
                .Returns((AzureTableEntity entity, TableUpdateMode mode, CancellationToken token) =>
                {
                    entity.Timestamp = DateTimeOffset.Now;
                    if (!data.ContainsKey(entity.PartitionKey))
                        data.Add(entity.PartitionKey, new Dictionary<string, AzureTableEntity>());
                    
                    data[entity.PartitionKey].AddOrUpdate(entity.RowKey, entity);
                   return Moq.Mock.Of<Response>();
                });

            tableClient
                .Setup(client => client.DeleteEntity(It.IsAny<string>(), It.IsAny<string>(), default, default))
                .Returns((string partitionKey, string rowKey, ETag tag, CancellationToken token) =>
                {
                    if (data.ContainsKey(partitionKey))
                        data[partitionKey].Remove(rowKey);
                    return Moq.Mock.Of<Response>();
                });

            tableClient
                .Setup(client =>
                    client.GetEntityIfExists<AzureTableEntity>(It.IsAny<string>(), It.IsAny<string>(), null, default))
                .Returns((string partitionKey, string rowKey, IEnumerable<string> select, CancellationToken token) =>
                {
                    if (data.ContainsKey(partitionKey) && data[partitionKey].ContainsKey(rowKey))
                        return Response.FromValue(data[partitionKey][rowKey], Moq.Mock.Of<Response>());

                    return Moq.Mock.Of<NullableResponse<AzureTableEntity>>();
                });

            tableClient
                .Setup(client => client.SubmitTransaction(It.IsAny<IEnumerable<TableTransactionAction>>(), default))
                .Returns((IEnumerable<TableTransactionAction> transactionActions, CancellationToken token) =>
                {
                    foreach (var action in transactionActions)
                        tableClient.Object.UpsertEntity((AzureTableEntity)action.Entity, TableUpdateMode.Replace,
                            token);
                    return Moq.Mock.Of<Response<IReadOnlyList<Response>>>();
                });
            
            tableClient.Setup(client =>
                    client.Query<AzureTableEntity>(It.IsAny<Expression<Func<AzureTableEntity, bool>>>(), null, null,
                        default))
                .Returns((Expression<Func<AzureTableEntity, bool>> express, int? maxParge, IEnumerable<string> select,
                    CancellationToken token) =>
                {
                    List<AzureTableEntity> entities = new List<AzureTableEntity>();
                    var expressCompile = express.Compile();
                    
                    foreach(var kv in data)
                        foreach(var innerKv in kv.Value)
                            if(expressCompile.Invoke(innerKv.Value))
                                entities.Add(innerKv.Value);
                    
                    var page = Page<AzureTableEntity>.FromValues(entities, null, Moq.Mock.Of<Response>());
                    return Pageable<AzureTableEntity>.FromPages(new List<Page<AzureTableEntity>> { page });
                });
            
            tableClient
                .Setup(client => client.CreateIfNotExists(CancellationToken.None))
                .Returns(() => null);
            
            return tableClient.Object;
        }
    }

    [SetUp]
    public void Init()
    {
        config = new StreamConfig();
        config.AddConfig(AzureRemoteStorageOptions.storageUriCst, "uri");
        config.AddConfig(AzureRemoteStorageOptions.storageAccountKeyCst, "key");
        config.AddConfig(AzureRemoteStorageOptions.accountNameCst, "account");
        
        context = new Mock<ProcessorContext>();
        context.Setup(c => c.Id)
            .Returns(new TaskId() { Id = 0, Partition = 0 });
        context.Setup(c => c.Configuration)
            .Returns(config);
        
        store = new MockAzureStore(null, "name");
        store.Init(context.Object, null);
    }
    
    
    [Test]
    public void InsertOneElement()
    {
        store.Put(Bytes.Wrap(Encoding.UTF8.GetBytes("test")), Encoding.UTF8.GetBytes("value"));
        Assert.AreEqual(1, store.data.Count);
    }
    
    [Test]
    public void InsertAndUpdateOneElement()
    {
        store.Put(Bytes.Wrap(Encoding.UTF8.GetBytes("test")), Encoding.UTF8.GetBytes("value"));
        store.Put(Bytes.Wrap(Encoding.UTF8.GetBytes("test")), Encoding.UTF8.GetBytes("value2"));
        Assert.AreEqual(1, store.data.Count);
    }
    
    [Test]
    public void InsertAndDeleteOneElement()
    {
        store.Put(Bytes.Wrap(Encoding.UTF8.GetBytes("test")), Encoding.UTF8.GetBytes("value"));
        store.Delete(Bytes.Wrap(Encoding.UTF8.GetBytes("test")));
        Assert.AreEqual(0, store.data[context.Object.Id.ToString()].Count);
    }
    
    [Test]
    public void InsertAndGettingOneElement()
    {
        store.Put(Bytes.Wrap(Encoding.UTF8.GetBytes("test")), Encoding.UTF8.GetBytes("value"));
        var item = store.Get(Bytes.Wrap(Encoding.UTF8.GetBytes("test")));
        Assert.IsNotNull(item);
        Assert.AreEqual(Encoding.UTF8.GetBytes("value"), item);
    }
    
    [Test]
    public void GettingNullElement()
    {
        var item = store.Get(Bytes.Wrap(Encoding.UTF8.GetBytes("test")));
        Assert.IsNull(item);
    }
    
    [Test]
    public void FlushClose()
    {
        store.Put(Bytes.Wrap(Encoding.UTF8.GetBytes("test")), Encoding.UTF8.GetBytes("value"));
        store.Flush();
        store.Close();
        Assert.AreEqual(1, store.data.Count); // nothing is deleted
    }
    
    [Test]
    public void GetAllElement()
    {
        store.Put(Bytes.Wrap(Encoding.UTF8.GetBytes("test")), Encoding.UTF8.GetBytes("value"));
        store.Put(Bytes.Wrap(Encoding.UTF8.GetBytes("test1")), Encoding.UTF8.GetBytes("value1"));
        store.Put(Bytes.Wrap(Encoding.UTF8.GetBytes("test2")), Encoding.UTF8.GetBytes("value2"));
        var items = store.All();
        Assert.AreEqual(3, items.Count());
    }
    
    [Test]
    public void GetReverseAllElement()
    {
        store.Put(Bytes.Wrap(Encoding.UTF8.GetBytes("test")), Encoding.UTF8.GetBytes("value"));
        store.Put(Bytes.Wrap(Encoding.UTF8.GetBytes("test1")), Encoding.UTF8.GetBytes("value1"));
        store.Put(Bytes.Wrap(Encoding.UTF8.GetBytes("test2")), Encoding.UTF8.GetBytes("value2"));
        var items = store.ReverseAll();
        Assert.AreEqual(3, items.Count());
    }
    
    [Test]
    public void GetRangeElement()
    {
        store.Put(Bytes.Wrap(Encoding.UTF8.GetBytes("test")), Encoding.UTF8.GetBytes("value"));
        store.Put(Bytes.Wrap(Encoding.UTF8.GetBytes("test1")), Encoding.UTF8.GetBytes("value1"));
        store.Put(Bytes.Wrap(Encoding.UTF8.GetBytes("test2")), Encoding.UTF8.GetBytes("value2"));
        var items = store.Range(Bytes.Wrap(Encoding.UTF8.GetBytes("test")), Bytes.Wrap(Encoding.UTF8.GetBytes("test1")))
            .ToList();
        Assert.AreEqual(2, items.Count());
    }
    
    [Test]
    public void GetReverseRangeElement()
    {
        store.Put(Bytes.Wrap(Encoding.UTF8.GetBytes("test")), Encoding.UTF8.GetBytes("value"));
        store.Put(Bytes.Wrap(Encoding.UTF8.GetBytes("test1")), Encoding.UTF8.GetBytes("value1"));
        store.Put(Bytes.Wrap(Encoding.UTF8.GetBytes("test2")), Encoding.UTF8.GetBytes("value2"));
        var items = store.ReverseRange(Bytes.Wrap(Encoding.UTF8.GetBytes("test")), Bytes.Wrap(Encoding.UTF8.GetBytes("test1")))
            .ToList();
        Assert.AreEqual(2, items.Count());
    }
    
    [Test]
    public void PutAllElement()
    {
        var elements = new List<KeyValuePair<Bytes, byte[]>>
        {
            KeyValuePair.Create(Bytes.Wrap(Encoding.UTF8.GetBytes("test")), Encoding.UTF8.GetBytes("value")),
            KeyValuePair.Create(Bytes.Wrap(Encoding.UTF8.GetBytes("test1")), Encoding.UTF8.GetBytes("value1")),
            KeyValuePair.Create(Bytes.Wrap(Encoding.UTF8.GetBytes("test2")), Encoding.UTF8.GetBytes("value2"))
        };
        
        store.PutAll(elements);
        var items = store.All();
        Assert.AreEqual(3, items.Count());
    }
}