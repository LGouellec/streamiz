# .NET Stream Processing Library for Apache Kafka <sup>TM</sup> &middot; [![GitHub license](https://img.shields.io/badge/license-MIT-green.svg)](https://github.com/LGouellec/streamiz-kafka-net/blob/master/LICENSE) ![build](https://github.com/LGouellec/kafka-streams-dotnet/workflows/build/badge.svg?branch=master)

## Sonarcloud statistics

[![Sonar Cloud Quality Gate](https://sonarcloud.io/api/project_badges/measure?branch=master&project=LGouellec_kafka-streams-dotnet&metric=alert_status)](https://sonarcloud.io/dashboard?branch=master&id=LGouellec_kafka-streams-dotnet)
[![Sonar Cloud Quality Gate](https://sonarcloud.io/api/project_badges/measure?branch=master&project=LGouellec_kafka-streams-dotnet&metric=coverage)](https://sonarcloud.io/dashboard?branch=master&id=LGouellec_kafka-streams-dotnet)
[![Sonar Cloud Reliability Rate](https://sonarcloud.io/api/project_badges/measure?branch=master&project=LGouellec_kafka-streams-dotnet&metric=reliability_rating)](https://sonarcloud.io/dashboard?branch=master&id=LGouellec_kafka-streams-dotnet)
[![Sonar Cloud Security Rate](https://sonarcloud.io/api/project_badges/measure?branch=master&project=LGouellec_kafka-streams-dotnet&metric=security_rating)](https://sonarcloud.io/dashboard?branch=master&id=LGouellec_kafka-streams-dotnet)
[![Sonar Cloud Maintainability Rate](https://sonarcloud.io/api/project_badges/measure?branch=master&project=LGouellec_kafka-streams-dotnet&metric=sqale_rating)](https://sonarcloud.io/dashboard?branch=master&id=LGouellec_kafka-streams-dotnet)
[![Sonar Cloud Duplicated Code](https://sonarcloud.io/api/project_badges/measure?branch=master&project=LGouellec_kafka-streams-dotnet&metric=duplicated_lines_density)](https://sonarcloud.io/dashboard?branch=master&id=LGouellec_kafka-streams-dotnet)

## GitHub statistics
<div>
    <img alt="GitHub issues" src="https://img.shields.io/github/issues/LGouellec/kafka-streams-dotnet">
    <img alt="GitHub pull requests" src="https://img.shields.io/github/issues-pr/LGouellec/kafka-streams-dotnet">
</div>
<br/>

<img src="./resources/logo-kafka-stream-net.png" width="100">

----

Streamiz Kafka .NET is .NET stream processing library for Apache Kafka. 

It's allowed to develop .NET applications that transform input Kafka topics into output Kafka topics. 
It's supported .NET Standard 2.1.

It's a rewriting inspired by [Kafka Streams](https://github.com/apache/kafka).

Finally it will provide the same functionality as [Kafka Streams](https://github.com/apache/kafka).

At moment, this project is being written. Thanks for you contribution !

# Usage

Sample code
``` csharp
static void Main(string[] args)
{
    CancellationTokenSource source = new CancellationTokenSource();
    
    var config = new StreamConfig<StringSerDes, StringSerDes>();
    config.ApplicationId = "test-app";
    config.BootstrapServers = "192.168.56.1:9092";
    config.SaslMechanism = SaslMechanism.Plain;
    config.SaslUsername = "admin";
    config.SaslPassword = "admin";
    config.SecurityProtocol = SecurityProtocol.SaslPlaintext;
    config.AutoOffsetReset = AutoOffsetReset.Earliest;
    config.NumStreamThreads = 2;
    
    StreamBuilder builder = new StreamBuilder();

    builder.Stream<string, string>("test")
        .FilterNot((k, v) => v.Contains("test"))
        .Peek((k,v) => Console.WriteLine($"Key : {k} | Value : {v}"))
        .To("test-output");

    builder.Table("topic", InMemory<string, string>.As("test-ktable-store"));

    Topology t = builder.Build();
    KafkaStream stream = new KafkaStream(t, config);

    Console.CancelKeyPress += (o, e) => {
        source.Cancel();
        stream.Close();
    };

    stream.Start(source.Token);
}
```

# Timeline

- End May 2020 - Beta 0.0.1 - All stateless processors, Exactly Once Semantic, InMemory store
- End October 2020 - Beta 0.0.2 - All statefull processors, Global Store, RocksDB Store
- End 2020 / Begin 2021 - 1.0.0 RC1 - Processor API, Metrics, Interactive Queries

# Stateless processor implemention

|Operator Name|Method|TODO|IMPLEMENTED|TESTED|DOCUMENTED|
|---|---|---|---|---|---|
|Branch|KStream -> KStream[]|   |   |   |&#9745;|
|Filter|KStream -> KStream|   |   |   |&#9745;|
|Filter|KTable -> KTable|   |   |   |&#9745;|
|InverseFilter|KStream -> KStream|   |   |   |&#9745;|
|InverseFilter|KTable -> KTable|   |   |   |&#9745;|
|FlatMap|KStream → KStream|   |   |   |&#9745;|
|FlatMapValues|KStream → KStream|   |   |   |&#9745;|
|Foreach|KStream → void|   |   |   |&#9745;|
|GroupByKey|KStream → KGroupedStream|   |   |   |&#9745;|
|GroupBy|KStream → KGroupedStream|   |   |   |&#9745;|
|GroupBy|KTable → KGroupedTable|   |   |   |&#9745;|
|Map|KStream → KStream|   |   |   |&#9745;|
|MapValues|KStream → KStream|   |   |   |&#9745;|
|MapValues|KTable → KTable|   |   |   |&#9745;|
|Peek|KStream → KStream|   |   |   |&#9745;|
|Print|KStream → void|   |   |   |&#9745;|
|SelectKey|KStream → KStream|   |   |   |&#9745;|
|Table to Steam|KTable → KStream|   |   |   |&#9745;|

# Statefull processor implementation

|Operator Name|Method|TODO|IMPLEMENTED|TESTED|DOCUMENTED|
|---|---|---|---|---|---|
|Aggregate|KGroupedStream -> KTable|&#9745;|   |   |   |
|Aggregate|KGroupedTable -> KTable|&#9745;|   |   |   |
|Aggregate(windowed)|KGroupedStream -> KTable|&#9745;|   |   |   |
|Count|KGroupedStream -> KTable|&#9745;|   |   |   |
|Count|KGroupedTable -> KTable|&#9745;|   |   |   |
|Count(windowed)|KGroupedStream → KStream|&#9745;|   |   |   |
|Reduce|KGroupedStream → KTable|&#9745;|   |   |   |
|Reduce|KGroupedTable → KTable|&#9745;|   |   |   |
|Reduce(windowed)|KGroupedStream → KTable|&#9745;|   |   |   |
|InnerJoin(windowed)|(KStream,KStream) → KStream|&#9745;|   |   |   |
|LeftJoin(windowed)|(KStream,KStream) → KStream|&#9745;|   |   |   |
|OuterJoin(windowed)|(KStream,KStream) → KStream|&#9745;|   |   |   |
|InnerJoin(windowed)|(KTable,KTable) → KTable|&#9745;|   |   |   |
|LeftJoin(windowed)|(KTable,KTable) → KTable|&#9745;|   |   |   |
|OuterJoin(windowed)|(KTable,KTable) → KTable|&#9745;|   |   |   |
|InnerJoin(windowed)|(KStream,KTable) → KStream|&#9745;|   |   |   |
|LeftJoin(windowed)|(KStream,KTable) → KStream|&#9745;|   |   |   |
|InnerJoin(windowed)|(KStream,GlobalKTable) → KStream|&#9745;|   |   |   |
|LeftJoin(windowed)|(KStream,GlobalKTable) → KStream|&#9745;|   |   |   |



# Test topology driver

Must be used for testing your stream topology.
Usage: 
``` csharp
static void Main(string[] args)
{
    var config = new StreamConfig<StringSerDes, StringSerDes>();
    config.ApplicationId = "test-test-driver-app";
    
    StreamBuilder builder = new StreamBuilder();

    builder.Stream<string, string>("test")
        .Filter((k, v) => v.Contains("test"))
        .To("test-output");

    Topology t = builder.Build();

    using (var driver = new TopologyTestDriver(t, config))
    {
        var inputTopic = driver.CreateInputTopic<string, string>("test");
        var outputTopic = driver.CreateOuputTopic<string, string>("test-output", TimeSpan.FromSeconds(5));
        inputTopic.PipeInput("test", "test-1234");
        var r = outputTopic.ReadKeyValue();
    }
}
```

# TODO implementation

- [x] Topology description
- [x] Refactor topology node processor builder
- [x] Subtopology impl
- [x] Unit tests (TestTopologyDriver, ...)
- [ ] Statefull processors impl
- [ ] Task restoring
- [WIP] Global state store
- [ ] Processor API
- [ ] Repartition impl
- [ ] Rocks DB state implementation
- [ ] Optimizing Kafka Streams Topologies
- [ ] Interactive Queries
- [ ] Metrics

Some documentations for help during implementation :
https://docs.confluent.io/current/streams/index.html

https://kafka.apache.org/20/documentation/streams/developer-guide/dsl-api.html#stateless-transformations

# Contribute members

Owners:

- [lgouellec](https://github.com/LGouellec)  [![Twitter Follow](https://img.shields.io/twitter/follow/LGouellec?style=social)](https://twitter.com/LGouellec)

Maintainers:

- [lgouellec](https://github.com/LGouellec)
- [mmoron](https://github.com/mmoron)

