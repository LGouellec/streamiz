# Test topology driver

Streamiz Kafka .Net provides a test-utils for testing your stream topology.

TopologyTestDriver that can be used pipe data through a Topology that is either assembled manually using the DSL using StreamsBuilder. The test driver simulates the library runtime that continuously fetches records from input topics and processes them by traversing the topology. 

You can use the test driver to verify that your specified processor topology computes the correct result with the manually piped in data records. The test driver captures the results records and allows to query its embedded state stores.

``` csharp
var config = new StreamConfig<StringSerDes, StringSerDes>();
config.ApplicationId = "test-test-driver-app";
    
StreamBuilder builder = new StreamBuilder();

builder.Stream<string, string>("test")
        .Filter((k, v) => v.Contains("test"))
        .To("test-output");

Topology t = builder.Build();
var driver = new TopologyTestDriver(t, config);
```

With the test driver you can create TestInputTopic<K, V> giving topic name and the corresponding serializers. TestInputTopic provides various methods to pipe new message values, keys and values, or list of KeyValue objects.

``` csharp
    var inputTopic = driver.CreateInputTopic<string, string>("test");
    inputTopic.PipeInput("test", "test-1234");
```

To verify the output, you can use TestOutputTopic<K, V> where you configure the topic and the corresponding deserializers during initialization. It offers helper methods to read only certain parts of the result records or the collection of records. For example, you can validate returned KeyValue with standard assertions if you only care about the key and value, but not the timestamp of the result record.

```csharp
    var outputTopic = driver.CreateOuputTopic<string, string>("test-output", TimeSpan.FromSeconds(5));
    var r = outputTopic.ReadKeyValue();
    Assert.IsNotNull(r);
    Assert.AreEqual("test", r.Message.Key);  
    Assert.AreEqual("test-1234", r.Message.Value);
```

Additionally, you can access state stores via the test driver before or after a test. Accessing stores before a test is useful to pre-populate a store with some initial values. After data was processed, expected updates to the store can be verified.

```csharp
var store = driver.GetKeyValueStore<string, string>("store-name");
```

Note, that you should always dispose the test driver at the end to make sure all resources are release properly.
TopologyTestDriver is disposable so you should 'using' keyword in your unit test.

## Example

Sample code :

``` csharp
var config = new StreamConfig<StringSerDes, StringSerDes>();
config.ApplicationId = "test-test-driver-app";

StreamBuilder builder = new StreamBuilder();

builder.Stream<string, string>("test")
        .Filter((k, v) => v.Contains("test"))
        .To("test-output");

builder.Table("test-ktable", InMemory.As<string, string>("test-store"));

Topology t = builder.Build();

using (var driver = new TopologyTestDriver(t, config))
{
    var inputTopic = driver.CreateInputTopic<string, string>("test");
    var inputTable = driver.CreateInputTopic<string, string>("test-ktable");
    var outputTopic = driver.CreateOuputTopic<string, string>("test-output", TimeSpan.FromSeconds(5));
    inputTopic.PipeInput("test", "test-1234");
    inputTable.PipeInput("key1", "value1");
    var r = outputTopic.ReadKeyValue();
    var store = driver.GetKeyValueStore<string, string>("test-store");
    var rbis = store.Get("key1");
    Assert.IsNotNull(r);
    Assert.AreEqual("test", r.Message.Key);  
    Assert.AreEqual("test-1234", r.Message.Value);
    Assert.IsNotNull(rbis);
    Assert.AreEqual("value1", rbis);
}
```

## Unit test with AVRO SerDes

SchemaAvroSerDes need to connect to Confluent Schema Registry to serialize/deserialize AVRO bean. 
So if you want write unit test with your avro beans, most of time you will not have one Confluent Schema Registry.
Since 1.1.4 release, you have a mock schema registry client implementation.

You have just to configuration SchemaRegistryUrl with string starts with "mock://[SCOPE_NAME]" :
``` csharp
config.SchemaRegistryUrl = "mock://test"; // scope here is test

// If you run multiple unit test in parallel, maybe you would reset mock schema registry after each test
// You can drop your scope like :
MockSchemaRegistry.DropScope("test");
```

Sample code :
``` csharp
var config = new StreamConfig<StringSerDes, SchemaAvroSerDes<Order>>();
config.ApplicationId = "test-mock-registry";
config.SchemaRegistryUrl = "mock://test";

StreamBuilder builder = new StreamBuilder();

builder.Stream<string, Order>("test")
        .Filter((k, v) => k.Contains("test"))
        .To("test-output");

Topology t = builder.Build();

using (var driver = new TopologyTestDriver(t, config))
{
    var inputTopic = driver.CreateInputTopic<string, Order>("test");
    var outputTopic = driver.CreateOuputTopic<string, Order>("test-output", TimeSpan.FromSeconds(5));
    inputTopic.PipeInput("test",
                    new Order
                    {
                        order_id = 12,
                        price = 150,
                        product_id = 1
                    });
    var r = outputTopic.ReadKeyValue();
    Assert.IsNotNull(r);
    Assert.AreEqual("test", r.Message.Key);
    Assert.AreEqual(12, r.Message.Value.order_id);
}
MockSchemaRegistry.DropScope("test");
```
## Testing partitioning

Records with the same key must land on the same partition for `GroupByKey`, `Aggregate`, `Join` or `Repartition` to be correct, and the inputs of a join must be co-partitioned. By default, every topic of the test driver has a single partition. Pass a `TestTopicPartitions` to test with several partitions:

``` csharp
var partitions = new TestTopicPartitions(defaultPartitions: 4) // input and output topics
    .WithPartitions("customers", 2);                          // per topic

using var driver = new TopologyTestDriver(topology, config, partitions);
```

- Records piped in an input topic are routed to a partition like a real Confluent.Kafka producer does, with the producer partitioner of the configuration (`StreamConfig.Partitioner`, `murmur2_random` by default, compatible with the Java producer). Set `TestRecord.Partition` to pipe a record to a given partition.
- Records read from an output topic expose their partition and their offset in that partition (`ConsumeResult.Partition`, `ConsumeResult.Offset`).
- Internal topics are sized like in a real application: a repartition topic gets the number of partitions of the source topics of the sub-topology writing to it, or `Repartitioned.WithNumberOfPartitions`.
- In `SYNC_TASK` mode, a single task per sub-topology processes all the partitions of its source topics, and `ProcessorContext.Partition` is the partition of the current record. In `ASYNC_CLUSTER_IN_MEMORY` mode, there is one task per partition.

The driver gives the partitioning of the topology:

| Method | Description |
|---|---|
| `GetPartitionCount(topic)` | Number of partitions of a topic (input, output or internal) |
| `GetPartition(topic, key, keySerdes)` | Partition where a producer writes a key, `-1` when it picks a random partition (null key) |
| `GetRepartitionTopics()` | Real names of the repartition topics |
| `GetCoPartitionedTopicGroups()` | Input topics of each join, which must be co-partitioned |
| `DescribeTopology()` | Machine-readable description of the topology, with internal topics resolved |

`PartitioningAssert` checks the partitioning, whatever your test framework. Each assertion throws a `PartitioningAssertionException` when it fails:

| Assertion | Checks |
|---|---|
| `KeysOnSinglePartition(records)` | All the records with the same key are on the same partition |
| `PartitionedByKey(driver, records)` | Each record is on the partition a producer would choose for its key |
| `CoPartitioned(driver)` | The input topics of each join have the same number of partitions (or the topics given) |
| `RepartitionTopicCount(driver, expected)` | The number of repartition topics, e.g. one after a key-changing operation followed by an aggregation |

``` csharp
var builder = new StreamBuilder();
builder.Stream<string, Order>("orders")
    .SelectKey((_, order, _) => order.CustomerId)
    .GroupByKey()
    .Count()
    .ToStream()
    .To("orders-by-customer");

var partitions = new TestTopicPartitions(4);
using var driver = new TopologyTestDriver(builder.Build(), config, partitions);
var input = driver.CreateInputTopic<string, Order>("orders");
var output = driver.CreateOutputTopic<string, long, StringSerDes, Int64SerDes>("orders-by-customer");

input.PipeInputs(orders);
var records = output.ReadKeyValueList().ToList();

PartitioningAssert.RepartitionTopicCount(driver, 1); // SelectKey + GroupByKey creates a repartition topic
PartitioningAssert.KeysOnSinglePartition(records);    // each customer is counted on a single partition
PartitioningAssert.PartitionedByKey(driver, records); // and written on the partition of its key
```

By default (`StreamConfig.DefaultPartitionerReshuffleEveryKey = true`), a sink computes the partition from the key of the record. When it is `false`, a sink keeps the partition of the input record: use `PartitionedByKey` to detect a key changed by `Map` or `SelectKey` without repartition.
