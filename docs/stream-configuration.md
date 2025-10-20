# Configuring a Stream Application

Stream configuration options must be configured before using Streams. You can configure KafkaStream by specifying parameters in a ``` IStreamConfig``` instance.

``` IStreamConfig``` is an interface which defines all the parameters necessary for the proper functioning of a stream. You can therefore create your own implementation of your configuration.

You have a default implementation in Streamiz Kafka .Net package (aka ``` StreamConfig```)

## Required configuration parameters

### ApplicationId

(Required) The application ID. Each stream processing application must have a unique ID. The same ID must be given to all instances of the application. It is recommended to use only alphanumeric characters, . (dot), - (hyphen), and _ (underscore). Examples: "hello_world", "hello_world-v1.0.0"

This ID is used in the following places to isolate resources used by the application from others:

As the default Kafka consumer and producer client.id prefix
As the Kafka consumer group.id for coordination
As the name of the subdirectory in the state directory (cf. state.dir)
As the prefix of internal Kafka topic names
Tip:
When an application is updated, the application.id should be changed unless you want to reuse the existing data in internal topics and state stores. For example, you could embed the version information within application.id, as my-app-v1.0.0 and my-app-v1.0.2.

### BootstrapServers

(Required) The Kafka bootstrap servers. This is the same setting that is used by the underlying producer and consumer clients to connect to the Kafka cluster. Example: "kafka-broker1:9092,kafka-broker2:9092".

Tip:
Kafka Streams applications can only communicate with a single Kafka cluster specified by this config value. Future versions of Kafka Streams will support connecting to different Kafka clusters for reading input streams and writing output stream

## Optional configuration parameters

### Guarantee

The processing guarantee that should be used. Possible values are ```ProcessingGuarantee.AT_LEAST_ONCE``` (default) and ```ProcessingGuarantee.EXACTLY_ONCE```. Note that if exactly-once processing is enabled, the default for parameter commit.interval.ms changes to 100ms. Additionally, consumers are configured with isolation.level="read_committed" and producers are configured with retries=Int32.MAX_VALUE, enable.idempotence=true, and max.in.flight.requests.per.connection=5 per default. Note that by default exactly-once processing requires a cluster of at least three brokers what is the recommended setting for production. For development you can change this, by adjusting broker setting transaction.state.log.replication.factor and transaction.state.log.min.isr to the number of broker you want to use.

### DefaultTimestampExtractor

A timestamp extractor pulls a timestamp from an instance of ConsumeResult. Timestamps are used to control the progress of streams.

The default extractor is FailOnInvalidTimestamp. This extractor retrieves built-in timestamps that are automatically embedded into Kafka messages by the Kafka producer client since Kafka version 0.10. Depending on the setting of Kafka’s server-side log.message.timestamp.type broker and message.timestamp.type topic parameters, this extractor provides you with:

event-time processing semantics if log.message.timestamp.type is set to CreateTime aka “producer time” (which is the default). This represents the time when a Kafka producer sent the original message. If you use Kafka’s official producer client, the timestamp represents milliseconds since the epoch.
ingestion-time processing semantics if log.message.timestamp.type is set to LogAppendTime aka “broker time”. This represents the time when the Kafka broker received the original message, in milliseconds since the epoch.
The FailOnInvalidTimestamp extractor throws an exception if a record contains an invalid (i.e. negative) built-in timestamp, because Kafka Streams would not process this record but silently drop it. Invalid built-in timestamps can occur for various reasons: if for example, you consume a topic that is written to by pre-0.10 Kafka producer clients or by third-party producer clients that don’t support the new Kafka 0.10 message format yet; another situation where this may happen is after upgrading your Kafka cluster from 0.9 to 0.10, where all the data that was generated with 0.9 does not include the 0.10 message timestamps.

You can provide your own timestamp extractors, for instance to retrieve timestamps embedded in the payload of messages. If you cannot extract a valid timestamp, you can either throw an exception, return a negative timestamp, or estimate a timestamp. 

Returning a negative timestamp will result in data loss – the corresponding record will not be processed but silently dropped. If you want to estimate a new timestamp, you can use the value provided via previousTimestamp (i.e., a Kafka Streams timestamp estimation). Here is an example of a custom TimestampExtractor implementation:

``` csharp
using System;
using Confluent.Kafka;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Processors;

public class Foo
{
    public DateTime dateCreated { get; set; }
    public int Id { get; set; }

    public long GetTimestampInMillis() => dateCreated.GetMilliseconds();
}

public class MyExtractor : ITimestampExtractor
{
    public long Extract(ConsumeResult<object, object> record, long partitionTime)
    {
        // `Foo` is your own custom class, which we assume has a method that returns
        // the embedded timestamp (milliseconds since midnight, January 1, 1970 UTC).
        long timestamp = -1;
        Foo myPojo = (Foo)record.Message.Value;
        if (myPojo != null)
        {
            timestamp = myPojo.GetTimestampInMillis();
        }
        if (timestamp < 0)
        {
            // Invalid timestamp!  Attempt to estimate a new timestamp,
            // otherwise fall back to wall-clock time (processing-time).
            if (partitionTime >= 0)
            {
                return partitionTime;
            }
            else
            {
                return DateTime.Now.GetMilliseconds();
            }
        }
        else
            return timestamp;
    }
}
```

You would then define the custom timestamp extractor in your Streams configuration as follows:

``` csharp
    using Streamiz.Kafka.Net;

    var config = new StreamConfig();
    config.DefaultTimestampExtractor = new MyExtractor();
```

### DefaultKeySerDes

The default Serializer/Deserializer class for record keys. Serialization and deserialization in Kafka Streams happens whenever data needs to be materialized, for example:

Whenever data is read from or written to a Kafka topic (e.g., via the StreamsBuilder#Stream<K, V>() and IKStream<K, V>#To() methods).
Whenever data is read from or written to a state store.

Example :
``` csharp
    var config = new StreamConfig();
    config.DefaultKeySerDes = new StringSerDes();
```

Note : In Streamiz Kafka Net package, you have a stream configuration class with generic types parameters to set default (key and value) serdes

Example :
``` csharp
    // Set defaultkeyserdes to StringSerDes, and defaultvalueserdes to StringSerDes
    var config = new StreamConfig<StringSerDes, StringSerDes>();
```

### DefaultValueSerDes

The default Serializer/Deserializer class for record values. Serialization and deserialization in Kafka Streams happens whenever data needs to be materialized, for example:

Whenever data is read from or written to a Kafka topic (e.g., via the StreamsBuilder#Stream<K, V>() and IKStream<K, V>#To() methods).
Whenever data is read from or written to a state store.

Example :
``` csharp
    var config = new StreamConfig();
    config.DefaultValueSerDes = new StringSerDes();
```

Note : In Streamiz Kafka Net package, you have a stream configuration class with generic types parameters to set default (key and value) serdes

Example :
``` csharp
    // Set defaultkeyserdes to StringSerDes, and defaultvalueserdes to StringSerDes
    var config = new StreamConfig<StringSerDes, StringSerDes>();
```

### NumStreamThreads

This specifies the number of stream threads in an instance of the Kafka Streams application. The stream processing code runs in these thread.

### ClientId

An ID prefix string used for the client IDs of internal consumer, producer and restore-consumer, with pattern '<ClientId>-StreamThread-<threadSequenceNumber>-<consumer|producer|restore-consumer>' with 
- ClientId : property setted, if null or empty, CliendId is randomly generated wih 'ApplicationId-{GUID}'
- threadSequenceNumber : Thread number

### TransactionalId

Enables the transactional producer. The TransactionalId is used to identify the same transactional producer instance across process restarts.
Only use if Guarantee is setted to ```ProcessingGuarantee.EXACTLY_ONCE```.

### TransactionTimeout

Timeout used for transaction related operations. (Default : 10 seconds).
Only use if Guarantee is set to ```ProcessingGuarantee.EXACTLY_ONCE```.

### CommitIntervalMs

The frequency with which to save the position of the processor. (Note, if Guarantee is set to ```ProcessingGuarantee.EXACTLY_ONCE```, the default value is ```StreamConfig.EOS_DEFAULT_COMMIT_INTERVAL_MS```,
otherwise the default value is ```StreamConfig.DEFAULT_COMMIT_INTERVAL_MS```.

### PollMs

The amount of time in milliseconds to block waiting for input. (Default : 100)

### MaxPollRecords

The maximum number of records returned in consumption processing by thread. (Default: 500)

### MaxPollRestoringRecords

The maximum number of records processed for each restoration step. Only use when application needs to restore state store from changelog topics. (Default: 1000)

### MaxTaskIdleMs

Maximum amount of time a stream task will stay idle when not all of its partition buffers contain records, to avoid potential out-of-order record processing across multiple input streams. (Default: 0)

### MaxPollIntervalMs

Maximum allowed time between calls to consume messages for high-level consumers. If this interval is exceeded the consumer is considered failed and the group will rebalance in order to reassign the partitions to another consumer group member.

### BufferedRecordsPerPartition

[Deprecated] Maximum number of records to buffer per partition. (Default: Int32.MaxValue)
If this number is exceeded, the consumer pauses for this partition until stream instance process messages.

### InnerExceptionHandler

Inner exception handling function called during processing.

### DeserializationExceptionHandler

Deserialization exception handling function called when deserialization exception during kafka consumption is raise.

### ProductionExceptionHandler

Production exception handling function called when kafka produce exception is raise.

### Logger

This library uses Microsoft.Extensions.Logging.Abstractions for logging. To set logging for this library you need to set Logger property in your stream config instance.

#### Configuring log4net 

To configure log4net you need to install Microsoft.Extensions.Logging.Log4Net.AspNetCore nuget package in your project and add log4net.config.
``` bash
dotnet add package Microsoft.Extensions.Logging.Log4Net.AspNetCore
```

Configure your logger inside your streams config instance :
``` csharp
var config = new StreamsConfig();
// .... //
config.Logger = LoggerFactory.Create(builder => builder.AddLog4Net());
```

Add a log4net.config resources in your project :
``` xml
<log4net debug="true">
    <appender name="ConsoleAppender" type="log4net.Appender.ConsoleAppender">
        <param name="Threshold" value="DEBUG" />
        <layout type="log4net.Layout.PatternLayout">
            <param name="ConversionPattern" value="%d [%t] %-5p %c - %m%n" />
        </layout>
    </appender>
    <root>
        <level value="DEBUG" />
        <appender-ref ref="ConsoleAppender" />
    </root>
</log4net>
```

### FollowMetadata

Authorize your streams application to follow metadata (timestamp, topic, partition, offset and headers) during processing record. You can use ``` StreamizMetadata ``` to get these metadatas. (Default : false)

### StateDir

Directory location for state store. This path must be unique for each streams instance sharing the same underlying filesystem. (Default : ``` Path.Combine(Path.GetTempPath(), "streamiz-kafka-net") ```)

### ReplicationFactor

The replication factor for change log topics topics created by the stream processing application. Default is 1.

### WindowStoreChangelogAdditionalRetentionMs

Added to a windows maintainMs to ensure data is not deleted from the log prematurely. Allows for clock drift. 
Default is 1 day.

### OffsetCheckpointManager

Manager which track offset saved in local state store.

### RocksDbConfigHandler

A Rocks DB config handler function called just before openning a rocksdb state store.

### MetricsIntervalMs

Delay between two invocations of `MetricsReporter()`. The minimum and default value is 30 seconds.

### MetricsReporter

The reporter expose a list of sensors throw by a stream thread every `MetricsIntervalMs` This reporter has the responsibility to export sensors and metrics into another platform (default: empty).

Streamiz provide multiple reporters (see `Streamiz.Kafka.Net.Metrics.Prometheus` and `Streamiz.Kafka.Net.Metrics.OpenTelemetry`). You can easily implement your own reporter and publish your metrics inside one another system.

### ExposeLibrdKafkaStats

Boolean which indicate if librdkafka handle statistics should be exposed ot not (default: false).
**Only main consumer and producer will be concerned.**

### MetricsRecording

The highest recording level for metrics (default: INFO).

### StartTaskDelayMs

[Obsolete] Time wait before completing the start task of `KafkaStream` (default: 5000).

### BasicAuthUserInfo

Credentials for the schema registry

### BasicAuthCredentialsSource

Specifies the source, use 0 for UserInfo or 1 for SaslInherit.

### SchemaRegistryRequestTimeoutMs

Specifies the timeout for requests to Confluent Schema Registry. 
Default: 30000

### SchemaRegistryMaxCachedSchemas

Specifies the maximum number of schemas CachedSchemaRegistryClient should cache locally. 
Default: 1000

### SchemaRegistryUrl

A comma-separated list of URLs for schema registry instances that are used register or lookup schemas.

### AutoRegisterSchemas

Specifies whether or not the Avro serializer should attempt to auto-register unrecognized schemas with Confluent Schema Registry. 
Default: true

### SubjectNameStrategy

The subject name strategy to use for schema registration / lookup. 
Possible values: ``` Topic, Record, TopicRecord ```

### AllowAutoCreateTopic

Allow automatic topic creation on the broker when subscribing to or assigning non-existent topics. The broker must also be configured with `auto.create.topics.enable=true` for this configuration to take effect.

Note that enabling this setting in the StreamConfig can cause unexpected behavior when creating internal topics via Streaming abstractions. Kafka will auto-create the topics, with the default amount of partitions. This happens before the Streaming library can create the internal topics, causing an exception. This can lead to internal topics not having the expected amount of partitions.

## Kafka consumers and producer configuration parameters

You can specify parameters for the Kafka consumers, producers, and admin client that are used internally. The consumer, producer and admin client settings are defined by wrapper properties on ConsumerConfig, ProducerConfig and AdminConfig.

So, all consumer, producer and admin client settings are accessible directly from StreamConfig instance.

In this example, the Kafka consumer session timeout is configured to be 60000 milliseconds in the StreamConfig instance.
``` csharp
    var config = new StreamConfig();
    config.SessionTimeoutMs = 60000;
```

In case of the configuration is not wrapped in StreamConfig yet or because you want to override differently the configuration of the main consumer and the configuration of the restore consumer , you can directly add your configuration via the following methods.
``` csharp
    var config = new StreamConfig();
    // add key/value for the main consumer (prefix : "main.consumer."), similar to config.Add("main.consumer.fetch.min.bytes", 1000);
    config.Add(StreamConfig.MainConsumerPrefix("fetch.min.bytes"), 1000);
    // add key/value for the restore consumer (prefix : "restore.consumer."), similar to config.Add("restore.consumer.fetch.max.bytes", 1000000);
    config.Add(StreamConfig.RestoreConsumerPrefix("fetch.max.bytes"), 1000000);
    // add key/value for the global consumer (prefix : "global.consumer."), similar to config.Add("global.consumer.fetch.max.bytes", 1000000);
    config.Add(StreamConfig.GlobalConsumerPrefix("fetch.max.bytes"), 1000000);
    // add key/value for the producer (prefix : "producer."), similar to config.Add("producer.acks", Acks.All);
    config.Add(StreamConfig.ProducerPrefix("acks"), Acks.All);
```

## Sample configuration implementation

``` csharp
    var config = new StreamConfig<StringSerDes, StringSerDes>();
    config.ApplicationId = "test-app";
    config.BootstrapServers = "192.168.56.1:9092";
    config.SaslMechanism = SaslMechanism.Plain;
    config.SaslUsername = "admin";
    config.SaslPassword = "admin";
    config.SecurityProtocol = SecurityProtocol.SaslPlaintext;
    config.AutoOffsetReset = AutoOffsetReset.Earliest;
    config.NumStreamThreads = 1;
    config.SchemaRegistryUrl = "http://localhost:8081";
    config.BasicAuthUserInfo = "user:password";
    config.BasicAuthCredentialsSource = 0;
```
