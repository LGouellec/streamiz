# kafka-stream-net
Kafka Stream Implementation for .NET Application [ WORK IN PROGRESS]
It's a POC for moment. 

It's a rewriting inspired by [Kafka Streams](https://github.com/apache/kafka/tree/trunk/streams)

I need contribution ;)

# Stateless operator implemention

|Operator Name|Method|TODO|IMPLEMENTED|TESTED|DONE|
|---|---|---|---|---|---|
|Branch|KStream -> KStream[]|   |[x]|   |   |
|Filter|KStream -> KStream|   |[x]|   |   |
|Filter|KTable -> KTable|[x]|   |   |   |
|InverseFilter|KStream -> KStream|   |[x]|   |   |
|InverseFilter|KTable -> KTable|[x]|   |   |   |
|FlatMap|KStream → KStream|   |[x]|   |   |
|FlatMapValues|KStream → KStream|[x]|   |   |   |
|Foreach|KStream → void|   |[x]|   |   |
|Foreach|KTable → void|[x]|   |   |   |
|GroupByKey|KStream → KGroupedStream|[x]|   |   |   |
|GroupBy|KStream → KGroupedStream|[x]|   |   |   |
|GroupBy|KTable → KGroupedTable|[x]|   |   |   |
|Map|KStream → KStream|   |[x]|   |   |
|MapValues|KStream → KStream|[x]|   |   |   |
|MapValues|KTable → KTable|[x]|   |   |   |
|Peek|KStream → KStream|   |[x]|   |   |
|Print|KStream → void|[x]|   |   |   |
|SelectKey|KStream → KStream|   |[x]|   |   |
|Table to Steam|KTable → KStream|[x]|   |   |   |

# Statefull operator implementation

TODO

# Logger

TODO

# Unit test

TODO 

# Usage

Sample code
```
            StreamConfig config = new StreamConfig();
            config.ApplicationId = "test-app";
            config.Add("bootstrap.servers", "192.168.56.1:9092");
            config.Add("sasl.mechanism", "SCRAM-SHA-512");
            config.Add("sasl.username", "admin");
            config.Add("sasl.password", "admin");
            config.Add("security.protocol", "SaslPlaintext");
            config.NumStreamThreads = 2;

            StreamBuilder builder = new StreamBuilder();
            builder.stream("test").filterNot((k, v) => v.Contains("test")).to("test-output");

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
