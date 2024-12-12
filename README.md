> [!IMPORTANT]
We’re excited to hear from you and would love to get your feedback on the product. Your insights are invaluable and will help us shape the future of our product to better meet your needs. The [survey](https://docs.google.com/forms/d/1OVISLOQY0FLcvh9KhSEPZ5K2Lgqzl7NkciCyd6w9_kU/) will only take a few minutes, and your responses will be completely confidential.


# .NET Stream Processing Library for Apache Kafka <sup>TM</sup> &middot; [![GitHub license](https://img.shields.io/badge/license-MIT-green.svg)](https://github.com/LGouellec/streamiz/blob/master/LICENSE) &middot; [![Join the chat at https://discord.gg/J7Jtxum](https://img.shields.io/discord/704268523169382421.svg?logoColor=white)](https://discord.gg/J7Jtxum) ![build](https://github.com/LGouellec/streamiz/workflows/build/badge.svg?branch=master)

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

<img src="./resources/logo-kafka-stream-net.png" width="150">

----

Streamiz Kafka .NET is .NET stream processing library for Apache Kafka. 

```
KAFKA is a registered trademark of The Apache Software Foundation and
has been licensed for use by Streamiz. Streamiz has no
affiliation with and is not endorsed by The Apache Software Foundation.
```

# Try it with Gitpod

[![Open in Gitpod](https://gitpod.io/button/open-in-gitpod.svg)](https://gitpod.io/#https://github.com/LGouellec/streamiz)

## Step 1

Waiting run task is complete. The task is consider complete some seconds after viewing this message `"🚀 Enjoy Streamiz the .NET Stream processing library for Apache Kafka (TM)"`

## Step 2

Switch to `producer` terminal and send sentences or word. The sample case is "Count the number words" similar to [here](https://developpaper.com/kafka-stream-word-count-instance/)

## Step 3

Switch to `consumer`terminal and check aggregation result


# Documentation

Read the full documentation on https://lgouellec.github.io/streamiz/

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
| Suppress(..)                                                 |              X                     |           X             | Plan for 1.7.0                           |
| Interactive Queries                                          |              X                     |                        | No plan for now                            |
| State store batch restoring                                  |              X                     |                        | No plan for now                            |
| Exactly Once v2                                     |              X                     |         X              |  |

# Community Support

Feel free to reach out to our community support [here](https://discord.gg/J7Jtxum) anytime; we're here to help you with any questions or issues you may have! 
