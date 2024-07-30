using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using NUnit.Framework;
using Streamiz.Kafka.Net.State.InMemory.Internal;

namespace Streamiz.Kafka.Net.Tests.Private;

public class ConcurrentSetTests
{
    private ConcurrentSet<string> concurrentSet;

    [SetUp]
    public void Init()
    {
        concurrentSet = new();
    }

    [TearDown]
    public void Dispose()
    {
        concurrentSet.Clear();
    }
    
    [TestCase(1000)]
    public void ConcurrencyAdded(int numberTasks)
    {
        var taskList = new List<Task>();
        for (int i = 0; i < numberTasks; i++)
        {
            taskList.Add(Task.Factory.StartNew((Object obj) =>
            {
                concurrentSet.Add(Guid.NewGuid().ToString());
            }, null));
        }
        Task.WaitAll(taskList.ToArray());
        Assert.AreEqual(numberTasks, concurrentSet.Count);
    }
    
    [TestCase(1000)]
    public void ConcurrencyRemoved(int numberTasks)
    {
        for (int i = 0; i < numberTasks; i++)
            concurrentSet.Add(i.ToString());
        
        var taskList = new List<Task>();
        for (int i = 0; i < numberTasks; i++)
        {
            taskList.Add(Task.Factory.StartNew((Object obj) =>
            {
                concurrentSet.Remove(obj.ToString());
            }, i));
        }
        
        Task.WaitAll(taskList.ToArray());
        Assert.AreEqual(0, concurrentSet.Count);
    }
    
    [TestCase(10000)]
    public void ConcurrencyAddedAndForeach(int numberTasks)
    {
        var taskList = new List<Task>();
        for (int i = 0; i < numberTasks; i++)
        {
            taskList.Add(Task.Factory.StartNew((Object obj) =>
            {
                concurrentSet.Add(Guid.NewGuid().ToString());
                foreach (var c in concurrentSet)
                    ;
            }, null));
        }
        Task.WaitAll(taskList.ToArray());
        Assert.AreEqual(numberTasks, concurrentSet.Count);
    }
    
    [TestCase(10000)]
    public void ConcurrencyAddedAndContains(int numberTasks)
    {
        var taskList = new List<Task>();
        for (int i = 0; i < numberTasks; i++)
        {
            taskList.Add(Task.Factory.StartNew((Object obj) =>
            {
                var guid = Guid.NewGuid().ToString();
                concurrentSet.Add(guid);
                Assert.IsTrue(concurrentSet.Contains(guid));
            }, null));
        }
        Task.WaitAll(taskList.ToArray());
        Assert.AreEqual(numberTasks, concurrentSet.Count);
    }

}