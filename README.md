# kafka-stream-net
Kafka Stream Implementation for .NET Application

Work in progress. It's a POC for moment. 
It's a rewriting inspired by [Kafka Streams](https://github.com/apache/kafka/tree/trunk/streams)
I need contribution ;)

Please see status_implementations.xlsx for status details implementations

Sample code
```
Configuration config = new Configuration();
config.ApplicationId = "test-app";
config.Add("bootstrap.servers", "192.168.56.1:9092");
config.Add("sasl.mechanism", "SCRAM-SHA-512");
config.Add("sasl.username", "admin");
config.Add("sasl.password", "admin");
config.Add("security.protocol", "SaslPlaintext");

StreamBuilder builder = new StreamBuilder();
builder.stream("test").filter((k, v) => v.Contains("toto")).to("test2");

Topology t = builder.build();
KafkaStream stream = new KafkaStream(t, config);
try
{
    stream.start();
    Console.ReadKey();
    stream.stop();
}catch(Exception e)
{
    Console.WriteLine(e.Message + ":" + e.StackTrace);
    stream.kill();
}
```
