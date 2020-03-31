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
- [EOS](https://github.com/LGouellec/kafka-stream-net/issues/2) [ ] : 
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
            StreamConfig config = new StreamConfig();
            config.ApplicationId = "test-app";
            config.Add("bootstrap.servers", "192.168.56.1:9092");
            config.Add("sasl.mechanism", "Plain");
            config.Add("sasl.username", "admin");
            config.Add("sasl.password", "admin");
            config.Add("security.protocol", "SaslPlaintext");
            config.NumStreamThreads = 1;

            StreamBuilder builder = new StreamBuilder();
            builder.stream("test").filterNot((k, v) => v.Contains("test")).to("test-output");
            builder.table("test-ktable", Consumed<string, string>.with(new StringSerDes(), new StringSerDes()), InMemory<string, string>.As("test-ktable-store"));

            Topology t = builder.build();
            KafkaStream stream = new KafkaStream(t, config);

            try
            {
                stream.Start();
                Console.ReadKey();
                stream.Stop();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message + ":" + e.StackTrace);
                stream.Kill();
            }
```