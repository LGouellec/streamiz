using System;
using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Kafka.Net.Tests.TestDriver;

public class Issue390Tests
{
    private class UserAccount
    {
        public string UserId { get; set; }
        public string Login { get; set; }
    }
    
    [Test]
    public void ReproducerTest()
    {
        var streamConfig = new StreamConfig();
        streamConfig.ApplicationId = "test-reproducer-issue390";
        
        var builder = new StreamBuilder();
        
        var account_users =  builder
            .Table("user_account_stream", new Int32SerDes(), new JsonSerDes<UserAccount>())
            .GroupBy<int, UserAccount, Int32SerDes, JsonSerDes<UserAccount>>((k, v, r) => KeyValuePair.Create(k, v))
            .Aggregate
            (
                () => new HashSet<string>(),
                (k, v, old) =>
                {
                    old.Add(v.UserId);
                    return old;
                },
                (k, v, old) => old,
                RocksDb.As<int, HashSet<string>>("agg-store")
                    .WithKeySerdes<Int32SerDes>()
                    .WithValueSerdes(new JsonSerDes<HashSet<string>>())
            );

        using var driver = new TopologyTestDriver(builder.Build(), streamConfig);
        var users = driver.CreateInputTopic<Int32, UserAccount, Int32SerDes, JsonSerDes<UserAccount>>("user_account_stream");
        users.PipeInput(1, new UserAccount(){UserId = Guid.NewGuid().ToString(), Login = "slegouellec"});
        users.PipeInput(1, new UserAccount(){UserId = Guid.NewGuid().ToString(), Login = "mlga"});

        var store = driver.GetKeyValueStore<int, HashSet<string>>("agg-store");
        var items = store.All().ToList();
        Assert.AreEqual(1, items.Count);
        Assert.AreEqual(1, items[0].Key);
        Assert.AreEqual(2, items[0].Value.Count);
    }
}