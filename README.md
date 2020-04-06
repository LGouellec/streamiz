# Kafka Stream .NET

Proof of Concept : Kafka Stream Implementation for .NET Application [WORK IN PROGRESS]

It's a rewriting inspired by [Kafka Streams](https://github.com/apache/kafka/tree/trunk/streams)

I need contribution ;)

# Stateless operator implemention

|Operator Name|Method|TODO|IMPLEMENTED|TESTED|DONE|
|---|---|---|---|---|---|
|Branch|KStream -> KStream[]|   | &#9745; |   |   |
|Filter|KStream -> KStream|   |&#9745;|   |   |
|Filter|KTable -> KTable|   |&#9745;|   |   |
|InverseFilter|KStream -> KStream|   |&#9745;|   |   |
|InverseFilter|KTable -> KTable|   |&#9745;|   |   |
|FlatMap|KStream → KStream|   |&#9745;|   |   |
|FlatMapValues|KStream → KStream|   |&#9745;|   |   |
|Foreach|KStream → void|   |&#9745;|   |   |
|GroupByKey|KStream → KGroupedStream|   |&#9745;|   |   |
|GroupBy|KStream → KGroupedStream|   |&#9745;|   |   |
|GroupBy|KTable → KGroupedTable|   |&#9745;|   |   |
|Map|KStream → KStream|   |&#9745;|   |   |
|MapValues|KStream → KStream|   |&#9745;|   |   |
|MapValues|KTable → KTable|   |&#9745;|   |   |
|Peek|KStream → KStream|   |&#9745;|   |   |
|Print|KStream → void|   |&#9745;|   |   |
|SelectKey|KStream → KStream|   |&#9745;|   |   |
|Table to Steam|KTable → KStream|   |&#9745;|   |   |

# Statefull operator implementation

TODO

# Priority feature for stateless beta version

- Adopt C# Syntax  [X]
- Configuration property [X]
- State thread + stream implementation [X]
- Naming Kafka Streams DSL Topologies [X]
- Logging [ ]
- [EOS](https://github.com/LGouellec/kafka-stream-net/issues/2) [ ]
- Unit test (TestTopologyDriver, ...) [ ]
- Test and validate all stateless processor [ ]

# TODO implementation

- Subtopology impl [ ]
- Task restoring [ ]
- Topology description [ ]
- State thread + task implementation [ ]
- Global state store [ ]
- Naming Kafka Streams DSL Topologies [ ]
- Processor API [ ]
- Repartition impl [ ]
- Logging [ ]
- Unit test (TestTopologyDriver, ...) [ ]
- [EOS](https://github.com/LGouellec/kafka-stream-net/issues/2) [ ]
- Configuration property [ ]
- Rocks DB state implementation [ ]
- Optimizing Kafka Streams Topologies  [ ]
- Interactive Queries [ ]
- Metrics [ ]
- Adopt C# Syntax  [ ]

Some documentations for help during implementation :
https://docs.confluent.io/current/streams/index.html
https://kafka.apache.org/20/documentation/streams/developer-guide/dsl-api.html#stateless-transformations

# Usage

Sample code
```
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

    builder.Table(
        "test-ktable",
        StreamOptions.Create(),
        InMemory<string, string>.As("test-ktable-store"));

    Topology t = builder.Build();
    KafkaStream stream = new KafkaStream(t, config);

    Console.CancelKeyPress += (o, e) => {
        source.Cancel();
        stream.Close();
    };

    stream.Start(source.Token);
}
```