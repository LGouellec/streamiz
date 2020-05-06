Welcome to <span style='color: #00A1F1'>Streamiz.Kafka.Net</span>
=============================================

// https://talk.jekyllrb.com/t/solved-github-page-build-failed-symlink-does-not-exist/1623

<img src="./assets/logo-kafka-stream-net.png" width="100">

**Streamiz.Kafka.Net** is .NET stream processing library for Apache Kafka (TM).

> The source code is available on [Github](https://github.com/LGouellec/kafka-streams-dotnet).

This framework is still in beta.

It's allowed to develop .NET applications that transform input Kafka topics into output Kafka topics. 
It's a rewriting inspired by [Kafka Streams](https://github.com/apache/kafka). 

Streamiz.Kafka.Net aims to provide the same functionality [Kafka Streams](https://github.com/apache/kafka). So you can found documentation here :
- [Confluent Kafka Streams](https://docs.confluent.io/current/streams/index.html)
- [Apache Kafka Streams](https://kafka.apache.org/documentation/streams/)


# Starting from scratch

Here is the easiest way to create a first stream, from scratch :

- Create a .Net Standard 2.1 compatible project, like .NET Core >= 3.0. For more information, please find help here [net-standard](https://docs.microsoft.com/fr-fr/dotnet/standard/net-standard#net-implementation-support)
- Add the nuget package Streamiz.Kafka.Net (not available for moment, soon)
- Please retrieve your kafka cluster connection information

Add this code
``` csharp
static void Main(string[] args)
{
    CancellationTokenSource source = new CancellationTokenSource();
    
    var config = new StreamConfig<StringSerDes, StringSerDes>();
    config.ApplicationId = "test-app";
    config.BootstrapServers = "192.168.56.1:9092";
    /** => PLEASE SET CREDENTIALS SETTINGS */
    config.SaslMechanism = SaslMechanism.Plain;
    config.SaslUsername = "admin";
    config.SaslPassword = "admin";
    config.SecurityProtocol = SecurityProtocol.SaslPlaintext;
    /** => PLEASE SET CREDENTIALS SETTINGS */
    
    StreamBuilder builder = new StreamBuilder();

    builder.Stream<string, string>("test")
        .FilterNot((k, v) => v.Contains("test"))
        .To("test-output");

    Topology t = builder.Build();
    KafkaStream stream = new KafkaStream(t, config);

    Console.CancelKeyPress += (o, e) => {
        source.Cancel();
        stream.Close();
    };

    stream.Start(source.Token);
}
```

You’re done !

# Need help

Feel free to ping me: [@LGouellec](https://twitter.com/LGouellec)