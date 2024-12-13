using NUnit.Framework;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Kafka.Net.Tests.Reproducer;

public class FIxIssue319Tests
{
    public class SelfId
    {
        public int Id { get; set; }
    }

    public class SelfRelation
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public int? Relation { get; set; }
    }

    public class Container
    {
        public SelfRelation Node { get; set; }
        public SelfRelation Dependency { get; set; }
    }

    [Test]
    public void SelfRelationshipTest()
    {
        var builder = new StreamBuilder();
        
        var stream =
            builder.Stream("self", new JsonSerDes<SelfId>(), new JsonSerDes<SelfRelation>());

        var filtrate = stream.Filter((k, v, _) => v.Relation.HasValue);
        var withRelationKeyStream = filtrate.SelectKey((k, v, _) => new SelfId { Id = v.Relation!.Value });
        var withRelationKeyTable = withRelationKeyStream.ToTable(
            InMemory.As<SelfId, SelfRelation>()
                .WithKeySerdes(new JsonSerDes<SelfId>())
                .WithValueSerdes(new JsonSerDes<SelfRelation>()));

        var table = stream.ToTable(
            InMemory.As<SelfId, SelfRelation>()
                .WithKeySerdes(new JsonSerDes<SelfId>())
                .WithValueSerdes(new JsonSerDes<SelfRelation>()));

        withRelationKeyTable
            .Join(table,
                (left, right) => new Container { Node = left, Dependency = right })
            .ToStream()
            .To("output", new JsonSerDes<SelfId>(), new JsonSerDes<Container>());

        var topology = builder.Build();
        
        var config = new StreamConfig();
        config.ApplicationId = "test-issue-319-driver-app";
        
        var driver = new TopologyTestDriver(topology, config);

        // create the test input topic
        var inputTopic =
            driver.CreateInputTopic(
                "self", new JsonSerDes<SelfId>(), new JsonSerDes<SelfRelation>());

        var outputTopic =
            driver.CreateOuputTopic<SelfId, Container, JsonSerDes<SelfId>, JsonSerDes<Container>>(
                "output");
        
        inputTopic.PipeInput(new SelfId { Id = 1 }, new SelfRelation { Id = 1, Name = "self", Relation = 2 });
        inputTopic.PipeInput(new SelfId { Id = 2 }, new SelfRelation { Id = 2, Name = "rel" });

        var output = outputTopic.ReadValue();
        
        Assert.AreEqual(1, output.Node.Id);
        Assert.AreEqual("self", output.Node.Name);
        Assert.IsNotNull(output.Node.Relation);
        Assert.AreEqual(2, output.Node.Relation.Value);
        Assert.AreEqual(2, output.Dependency.Id);
        Assert.AreEqual("rel", output.Dependency.Name);
        Assert.IsNull(output.Dependency.Relation);
    }
    
}