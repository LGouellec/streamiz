# .NET Stream Processing Library for Apache Kafka <sup>TM</sup> &middot; [![GitHub license](https://img.shields.io/badge/license-MIT-green.svg)](https://github.com/LGouellec/streamiz-kafka-net/blob/master/LICENSE) &middot; [![Join the chat at https://discord.gg/J7Jtxum](https://img.shields.io/discord/704268523169382421.svg?logoColor=white)](https://discord.gg/J7Jtxum) ![build](https://github.com/LGouellec/kafka-streams-dotnet/workflows/build/badge.svg?branch=master)

| Package  | Nuget version  | Downloads |
|---|---|---|
| Streamiz.Kafka.Net   | ![Nuget (with prereleases)](https://img.shields.io/nuget/vpre/Streamiz.Kafka.Net)  | ![Nuget](https://img.shields.io/nuget/dt/Streamiz.Kafka.Net) |
| Streamiz.Kafka.Net.SchemaRegistry.SerDes   | ![Nuget (with prereleases)](https://img.shields.io/nuget/vpre/Streamiz.Kafka.Net.SchemaRegistry.SerDes)  | ![Nuget](https://img.shields.io/nuget/dt/Streamiz.Kafka.Net.SchemaRegistry.SerDes) |
| Streamiz.Kafka.Net.SchemaRegistry.SerDes.Avro  | ![Nuget (with prereleases)](https://img.shields.io/nuget/vpre/Streamiz.Kafka.Net.SchemaRegistry.SerDes.Avro)  | ![Nuget](https://img.shields.io/nuget/dt/Streamiz.Kafka.Net.SchemaRegistry.SerDes.Avro) |
| Streamiz.Kafka.Net.SchemaRegistry.SerDes.Protobuf   | ![Nuget (with prereleases)](https://img.shields.io/nuget/vpre/Streamiz.Kafka.Net.SchemaRegistry.SerDes.Protobuf)  | ![Nuget](https://img.shields.io/nuget/dt/Streamiz.Kafka.Net.SchemaRegistry.SerDes.Protobuf) |
| Streamiz.Kafka.Net.SchemaRegistry.SerDes.Json   | ![Nuget (with prereleases)](https://img.shields.io/nuget/vpre/Streamiz.Kafka.Net.SchemaRegistry.SerDes.Json) | ![Nuget](https://img.shields.io/nuget/dt/Streamiz.Kafka.Net.SchemaRegistry.SerDes.Json) |
| Streamiz.Kafka.Net.Metrics.Prometheus   | ![Nuget (with prereleases)](https://img.shields.io/nuget/vpre/Streamiz.Kafka.Net.Metrics.Prometheus)  | ![Nuget](https://img.shields.io/nuget/dt/Streamiz.Kafka.Net.Metrics.Prometheus) |
| Streamiz.Kafka.Net.Metrics.OpenTelemetry   | ![Nuget (with prereleases)](https://img.shields.io/nuget/vpre/Streamiz.Kafka.Net.Metrics.OpenTelemetry)  | ![Nuget](https://img.shields.io/nuget/dt/Streamiz.Kafka.Net.Metrics.OpenTelemetry) |

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

# Try it with Gitpod

[![Open in Gitpod](https://gitpod.io/button/open-in-gitpod.svg)](https://gitpod.io/#https://github.com/LGouellec/kafka-streams-dotnet)

## Step 1

Waiting run task is complete. The task is consider complete some seconds after viewing this message `"🚀 Enjoy Streamiz the .NET Stream processing library for Apache Kafka (TM)"`

## Step 2

Switch to `producer` terminal and send sentences or word. The sample case is "Count the number words" similar to [here](https://developpaper.com/kafka-stream-word-count-instance/)

## Step 3

Switch to `consumer`terminal and check aggregation result


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
    config.BootstrapServers = "localhost:9092";
    
    StreamBuilder builder = new StreamBuilder();

    var kstream = builder.Stream<string, string>("stream");
    var ktable = builder.Table("table", InMemory.As<string, string>("table-store"));

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

# Compare Kafka Streams vs Streamiz

|                        **_Features_**                        | **Kafka Streams (JAVA) supported** | **Streamiz supported** |                 **Comment**                |
|:------------------------------------------------------------:|:----------------------------------:|:----------------------:|:------------------------------------------:|
| Stateless processors                                         |              X                     |         X              |                                            |
| RocksDb store                                                |              X                     |         X              |                                            |
| Standby replicas                                             |              X                     |                        |    No plan for now                         |
| InMemory store                                               |              X                     |         X              |                                            |
| Transformer, Processor API                                   |              X                     |         X              |                                            |
| Punctuate                                                    |              X                     |         X              |                                            |
| KStream-KStream Join                                         |              X                     |         X              |                                            |
| KTable-KTable Join                                           |              X                     |         X              |                                            |
| KTable-KTable FK Join                                        |              X                     |                        | Plan for future                            |
| KStream-KTable Join                                          |              X                     |         X              |                                            |
| KStream-GlobalKTable Join                                    |              X                     |         X              |                                            |
| KStream Async Processing (external call inside the topology) |                                    |         X              |  Not supported in Kafka Streams JAVA       |
| Hopping window                                               |              X                     |         X              |                                            |
| Tumbling window                                              |              X                     |         X              |                                            |
| Sliding window                                               |              X                     |                        | No plan for now                            |
| Session window                                               |              X                     |                        | No plan for now                            |
| Cache                                                        |              X                     |         X              | EA 1.6.0                                   |
| Suppress(..)                                                 |              X                     |                        | No plan for now                            |
| Interactive Queries                                          |              X                     |                        | No plan for now                            |
| State store batch restoring                                  |              X                     |                        | No plan for now                            |
| Exactly Once (v1 and v2)                                     |              X                     |         X              | EOS V1 supported, EOS V2 not supported yet |

# Contributing

Maintainers:

- [lgouellec](https://github.com/LGouellec)

**Streamiz Kafka .Net** is a community project. We invite your participation through issues and pull requests! You can peruse the [contributing guidelines](CONTRIBUTING.md).

When adding or changing a service please add tests and documentations.

# Support

You can found support [here](https://discord.gg/J7Jtxum)

# Star History

<a href="https://star-history.com/#LGouellec/kafka-streams-dotnet&Date">
 <picture>
   <source media="(prefers-color-scheme: dark)" srcset="https://api.star-history.com/svg?repos=LGouellec/kafka-streams-dotnet&type=Date&theme=dark" />
   <source media="(prefers-color-scheme: light)" srcset="https://api.star-history.com/svg?repos=LGouellec/kafka-streams-dotnet&type=Date" />
   <img alt="Star History Chart" src="https://api.star-history.com/svg?repos=LGouellec/kafka-streams-dotnet&type=Date" />
 </picture>
</a>
