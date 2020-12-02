# .NET Stream Processing Library for Apache Kafka <sup>TM</sup> &middot; [![GitHub license](https://img.shields.io/badge/license-MIT-green.svg)](https://github.com/LGouellec/streamiz-kafka-net/blob/master/LICENSE) &middot; [![Join the chat at https://discord.gg/J7Jtxum](https://img.shields.io/discord/704268523169382421.svg?logoColor=white)](https://discord.gg/J7Jtxum) ![build](https://github.com/LGouellec/kafka-streams-dotnet/workflows/build/badge.svg?branch=master) ![Nuget](https://img.shields.io/nuget/dt/Streamiz.Kafka.Net) ![Nuget (with prereleases)](https://img.shields.io/nuget/vpre/Streamiz.Kafka.Net)

## Quality Statistics

[![Sonar Cloud Quality Gate](https://sonarcloud.io/api/project_badges/measure?branch=master&project=LGouellec_kafka-streams-dotnet&metric=alert_status)](https://sonarcloud.io/dashboard?branch=master&id=LGouellec_kafka-streams-dotnet)
[![Sonar Cloud Quality Gate](https://sonarcloud.io/api/project_badges/measure?branch=master&project=LGouellec_kafka-streams-dotnet&metric=coverage)](https://sonarcloud.io/dashboard?branch=master&id=LGouellec_kafka-streams-dotnet)
[![Sonar Cloud Reliability Rate](https://sonarcloud.io/api/project_badges/measure?branch=master&project=LGouellec_kafka-streams-dotnet&metric=reliability_rating)](https://sonarcloud.io/dashboard?branch=master&id=LGouellec_kafka-streams-dotnet)
[![Sonar Cloud Security Rate](https://sonarcloud.io/api/project_badges/measure?branch=master&project=LGouellec_kafka-streams-dotnet&metric=security_rating)](https://sonarcloud.io/dashboard?branch=master&id=LGouellec_kafka-streams-dotnet)
[![Sonar Cloud Maintainability Rate](https://sonarcloud.io/api/project_badges/measure?branch=master&project=LGouellec_kafka-streams-dotnet&metric=sqale_rating)](https://sonarcloud.io/dashboard?branch=master&id=LGouellec_kafka-streams-dotnet)
[![Sonar Cloud Duplicated Code](https://sonarcloud.io/api/project_badges/measure?branch=master&project=LGouellec_kafka-streams-dotnet&metric=duplicated_lines_density)](https://sonarcloud.io/dashboard?branch=master&id=LGouellec_kafka-streams-dotnet)

## Project Statistics
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

This project is being written. Thanks for you contribution !

# Timeline

- End December 2020 - 1.2.0 - Persistent state store (eg: RocksDB Store), Repartition and Changelog topics
- End 2020 / Begin 2021 - 1.3.0 - Processor API, Metrics, Interactive Queries, Standby Replica

# Documentation

Read the full documentation on https://lgouellec.github.io/kafka-streams-dotnet/

# Installation

Nuget packages are listed to [nuget.org](https://www.nuget.org/packages/Streamiz.Kafka.Net/)

Install the last version with :
```shell
dotnet add package Streamiz.Kafka.Net
```

# Usage

There, a sample streamiz application :

``` csharp
static void Main(string[] args)
{ 
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

    var kstream = builder.Stream<string, string>("stream");
    var ktable = builder.Table("table", InMemory<string, string>.As("table-store"));

    kstream.Join<string, string, StringSerDes, StringSerDes>(ktable, (v, v1) => $"{v}-{v1}")
           .To("join-topic");

    Topology t = builder.Build();
    KafkaStream stream = new KafkaStream(t, config);

    Console.CancelKeyPress += (o, e) => {
        stream.Dispose();
    };

    await stream.StartAsync();
}
```


# TODO implementation

- [X] Global state store
- [X] Refactor Subtopology & StreamTask assignment
- [X] Refactor Topology description for corresponding to java implementation
- [X] Refactor Partition grouper + record queue
- [X] Statefull processors impl
- [ ] Transform and Through Processor
- [ ] Supress Processor (.suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded())))
- [ ] Rocks DB state implementation
- [ ] Changelog Topic
- [ ] Task restoring
- [ ] Repartition topic
- [ ] Repartition Processor [KAFKA-8611](https://issues.apache.org/jira/browse/KAFKA-8611) | [PR #7170](https://github.com/apache/kafka/pull/7170)
- [ ] Processor API
- [ ] Optimizing Kafka Streams Topologies
- [ ] Standby Replica
- [ ] Interactive Queries
- [ ] Metrics

# Contributing

Owners:

- [lgouellec](https://github.com/LGouellec)  [![Twitter Follow](https://img.shields.io/twitter/follow/LGouellec?style=social)](https://twitter.com/LGouellec)

Maintainers:

- [lgouellec](https://github.com/LGouellec)
- [mmoron](https://github.com/mmoron)

**Streamiz Kafka .Net** is a community project. We invite your participation through issues and pull requests! You can peruse the [contributing guidelines](CONTRIBUTING.md).

When adding or changing a service please add tests and documentations.

# Support

You can found support [here](https://discord.gg/J7Jtxum)