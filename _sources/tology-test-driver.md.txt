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

builder.Table("test-ktable", InMemory<string, string>.As("test-store"));

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