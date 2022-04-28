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

<img src="./resources/logo-kafka-stream-net.png" width="150">

----

Streamiz Kafka .NET is .NET stream processing library for Apache Kafka. 

```
KAFKA is a registered trademark of The Apache Software Foundation and
has been licensed for use by Streamiz. Streamiz has no
affiliation with and is not endorsed by The Apache Software Foundation.
```

It's allowed to develop .NET applications that transform input Kafka topics into output Kafka topics. 
It's supported .NET Standard 2.1. 

It's a rewriting inspired by [Kafka Streams](https://github.com/apache/kafka). Finally it will provide the same functionality as [Kafka Streams](https://github.com/apache/kafka).

This project is being written. Thanks for you contribution !

# ROADMAP

- 1.3.0 - Metrics
- 1.4.0 - Standby Replica, Processor API
- 1.5.0 - Interactive Queries

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
static async System.Threading.Tasks.Task Main(string[] args)
{ 
    var config = new StreamConfig<StringSerDes, StringSerDes>();
    config.ApplicationId = "test-app";
    config.BootstrapServers = "192.168.56.1:9092";
    
    StreamBuilder builder = new StreamBuilder();

    var kstream = builder.Stream<string, string>("stream");
    var ktable = builder.Table("table", InMemory<string, string>.As("table-store"));

    kstream.Join(ktable, (v, v1) => $"{v}-{v1}")
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

- [ ] Consumer Incremental Rebalance Protocol #KIP-429
- [ ] Supress Processor (.suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded())))
- [X] Repartition Processor [KAFKA-8611](https://issues.apache.org/jira/browse/KAFKA-8611) | [PR #7170](https://github.com/apache/kafka/pull/7170)
- [ ] Processor API
- [ ] Sample projects (Micro-services, console sample, topology implementation, etc ..) which use Streamiz package ([see](https://github.com/LGouellec/kafka-streams-dotnet-samples))
- [ ] Json SerDes which interact Confluent Schema Registry
- [ ] Optimizing Kafka Streams Topologies
- [ ] Standby Replica
- [ ] Interactive Queries
- [X] Metrics

# Contributing

Owners:

- [lgouellec](https://github.com/LGouellec)  [![Twitter Follow](https://img.shields.io/twitter/follow/LGouellec?style=social)](https://twitter.com/LGouellec)

Maintainers:

- [lgouellec](https://github.com/LGouellec)

**Streamiz Kafka .Net** is a community project. We invite your participation through issues and pull requests! You can peruse the [contributing guidelines](CONTRIBUTING.md).

When adding or changing a service please add tests and documentations.

# Support

You can found support [here](https://discord.gg/J7Jtxum)
