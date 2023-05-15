# Processor API

## Overview

The Processor API can be used to implement both stateless as well as stateful operations, where the latter is achieved through the use of state stores.

## Define a stream processor

A stream processor is a node in the processor topology that represents a single processing step. With the Processor API, you can define arbitrary stream processors that processes one received record at a time, and connect these processors with their associated state stores to compose the processor topology.

You can define a customized stream processor by implementing the `IProcessor<K, V>` interface or  the `ITransformer<Kin,Vin,Kout,Vout>` interface which provide the `Process()` API method. The `Process()` method is called on each of the received records.

The `Processor` interface also has an `Init()` method, which is called by the library during task construction phase. Processor instances should perform any required initialization in this method. The `Init()` method passes in a `ProcessorContext<K,V>` instance, which provides access to the metadata of the currently processed record, including its source Apache Kafka® topic and partition, its corresponding message offset, and further such information. You can also use this context instance to schedule a punctuation function (via `ProcessorContext<K,V>#Schedule()`), to forward a new record as a key-value pair to the downstream processors (via `ProcessorContext<K,V>#Forward()`), and to commit the current processing progress (via `ProcessorContext<K,V>#Commit()`). Any resources you set up in `Init()` can be cleaned up in the `Close()` method.

The `Transformer` interface takes two sets of generic parameters: Kin, Vin, Kout, and Vout. These define the input and output types that the processor implementation can handle. KIn and VIn define the key and value types that are passed to `Process()`. Likewise, KOut and VOut define the forwarded key and value types that `ProcessorContext#Forward()` accepts.

Both the `IProcessor<K,V>#Process()` and `ITransformer<Kin,Vin,Kout,Vout>#Process()` handle records in the form of the Record<K, V> data class. This class gives you access to the main components of a Kafka record: the key, value, timestamp, headers and topic/partition/offset. When forwarding records, you can use the static builder methods to create a new Record from scratch. 

In addition to handling incoming records by using `process()`, you can schedule periodic invocation, named “punctuation”, in your processor’s `Init()` method by calling `ProcessorContext<K,V>#Schedule()` and passing it a Punctuator. The `PunctuationType` determines what notion of time is used for the punctuation scheduling: either event-time or processing-time (by default, event-time is configured to represent event-time via `ITimestampExtractor`). When event-time is used, `Punctuate()` is triggered purely by data, because event-time is determined (and advanced forward) by the timestamps derived from the input data. When there is no new input data arriving, event-time is not advanced and `Punctuate()` is not called.

For example, if you schedule a Punctuator function every 10 seconds based on PunctuationType.STREAM_TIME and if you process a stream of 60 records with consecutive timestamps from 1 (first record) to 60 seconds (last record), then `Punctuate()` would be called 6 times. This happens regardless of the time required to actually process those records. `Punctuate()` would be called 6 times regardless of whether processing these 60 records takes a second, a minute, or an hour.

When processing-time (i.e. PunctuationType.PROCESSING_TIME) is used, `Punctuate()` is triggered purely by the wall-clock time. Reusing the example above, if the Punctuator function is scheduled based on PunctuationType.PROCESSING_TIME, and if these 60 records were processed within 20 seconds, `Punctuate()` is called 2 times (one time every 10 seconds). If these 60 records were processed within 5 seconds, then no `Punctuate()` is called at all. Note that you can schedule multiple Punctuator callbacks with different PunctuationType types within the same processor by calling `ProcessorContext<K,V>#Schedule()` multiple times inside `Init()` method.

## Accessing Processor Context

As we have mentioned above, a ProcessorContext controls the processing workflow such as scheduling a punctuation function, and committing the current processed state, etc. In fact, this object can also be used to access the metadata related with the application like applicationId, taskId, and stateDir located to store the task’s state, and also the current processed record’s metadata like topic, partition, offset, and timestamp.

The following example `Process()` function enriches the record differently based on the record context:

``` csharp

    public class EnrichTransformer : ITransformer<String, String, String, String>
    {
        public void Init(ProcessorContext<string, string> context)
        {
            // you can keep the processor context locally to schedule a punctuator,
            // commit explicitly or forward different message
        }

        public Record<string, string> Process(Record<string, string> record)
        {
            switch (record.TopicPartitionOffset.Topic)
            {
                case "alerts":
                    return Record<string, string>.Create(record.Key, DecorateWithHighPriority(record.Value));
                case "notifications":
                    return Record<string, string>.Create(record.Key, DecorateWithMediumPriority(record.Value));
                default:
                    return Record<string, string>.Create(record.Key, DecorateWithLowPriority(record.Value));
            }
        }

        public void Close() { }
    }
```

```
Record context metadata: The metadata of the currently processing record may not always be available. For example, if the current processing record is not piped from any source topic, but is generated from a punctuation function, then its metadata field topic will be empty, and partition offset will be a sentinel value (-1 in this case), while its timestamp field will be the triggering time of the punctuation function that generated this record.
```

## State stores

To implement a stateful `Processor` or `Transformer`, you must provide one or more state stores to the processor or transformer (stateless processors or transformers do not need state stores). State stores can be used to remember recently received input records, to track rolling aggregates, to de-duplicate input records, and more.

The available state store types in Streamiz have fault tolerance enabled by default.

### Defining and creating a State Store

You can either use one of the available store types or implement your own custom store type. It’s common practice to leverage an existing store type via the `Stores` factory.

Note that, when using Streamiz, you normally don’t create or instantiate state stores directly in your code. Rather, you define state stores indirectly by creating a so-called `StoreBuilder`. This builder is used by Streamiz as a factory to instantiate the actual state stores locally in application instances when and where needed.

The following store types are available out of the box.

| **_Store Type_** | **Storage Engine** | **Fault-tolerant?** | **Description** |
|:---:|:---:|---|:---:|
| Persistent `IKeyValueStore<K, V>` | RocksDB | Yes (enabled by default) | - The recommended store type for most use cases.<br>- Stores its data on local disk.<br>- Storage capacity: managed local state can be larger than the memory (heap space) of an application instance, but must fit into the available local disk space.<br><br>Available store variants:<br>- [time window key-value store](https://github.com/LGouellec/kafka-streams-dotnet/blob/develop/core/State/IWindowStore.cs)<br>- [timestamped key-value store](https://github.com/LGouellec/kafka-streams-dotnet/blob/develop/core/State/ITimestampedKeyValueStore.cs)<br>- [timestamped window key-value store](https://github.com/LGouellec/kafka-streams-dotnet/blob/develop/core/State/ITimestampedWindowStore.cs)<br><br>Example : <br> // Creating a persistent key-value store:<br>// here, we create a `IKeyValueStore<string, long>` named "persistent-counts".<br><br>StoreBuilder countStoreBuilder = Stores.KeyValueStoreBuilder<string, long> (Stores.PersistentKeyValueStore("persistent-counts"),<br>new StringSerDes(),<br>new Int64SerDes());<br> |
| In-memory `IKeyValueStore<K,V>` | -- | Yes (enabled by default) | - Stores its data in memory.<br>- Storage capacity: managed local state must fit into memory of an application instance.<br>- Useful when application instances run in an environment where local disk space is either not available or local disk space is wiped in-between app instance restarts.<br><br>Available store variants:<br>- [time window key-value store](https://github.com/LGouellec/kafka-streams-dotnet/blob/develop/core/State/IWindowStore.cs)<br>- [timestamped key-value store](https://github.com/LGouellec/kafka-streams-dotnet/blob/develop/core/State/ITimestampedKeyValueStore.cs)<br>- [timestamped window key-value store](https://github.com/LGouellec/kafka-streams-dotnet/blob/develop/core/State/ITimestampedWindowStore.cs)<br><br>Example : <br>// Creating an in-memory key-value store:<br>// here, we create a `IKeyValueStore<string, long>` named "inmemory-count".<br><br>StoreBuilder countStoreBuilder = Stores.KeyValueStoreBuilder<string, long> (Stores.InMemoryKeyValueStore("inmemory-count"),<br>new StringSerDes(),<br>new Int64SerDes());<br> |

### Timestamped State Stores

IKTables always store timestamps by default. A timestamped state store improves stream processing semantics and enables management of out-of-order data in source IKTables, detects out-of-order joins and aggregations.

You can query timestamped state stores with or without a timestamp.

### Fault-tolerant State Stores

To make state stores fault-tolerant and to allow for state store migration without data loss, a state store can be continuously backed up to a Kafka topic behind the scenes. For example, to migrate a stateful stream task from one machine to another when elastically adding or removing capacity from your application. This topic is sometimes referred to as the state store’s associated changelog topic, or its changelog. For example, if you experience machine failure, the state store and the application’s state can be fully restored from its changelog. You can enable or disable this backup feature for a state store.

By default, persistent key-value stores are fault-tolerant. They are backed by a compacted changelog topic. The purpose of compacting this topic is to prevent the topic from growing indefinitely, to reduce the storage consumed in the associated Kafka cluster, and to minimize recovery time if a state store needs to be restored from its changelog topic.

Similarly, persistent window stores are fault-tolerant. They are backed by a topic that uses both compaction and deletion. Because of the structure of the message keys that are being sent to the changelog topics, this combination of deletion and compaction is required for the changelog topics of window stores. For window stores, the message keys are composite keys that include the “normal” key and window timestamps. For these types of composite keys it would not be sufficient to only enable compaction to prevent a changelog topic from growing out of bounds. With deletion enabled, old windows that have expired will be cleaned up by Kafka’s log cleaner as the log segments expire. The default retention setting is [Materialized<K, V, S>#WithRetention()](https://github.com/LGouellec/kafka-streams-dotnet/blob/b700b698bb45dee5f51a49876d64c59b17432399/core/Table/Materialized.cs#LL321C45-L321C45) + 1 day. You can override this setting by specifying [IStreamConfig.WindowStoreChangelogAdditionalRetentionMs](https://github.com/LGouellec/kafka-streams-dotnet/blob/b700b698bb45dee5f51a49876d64c59b17432399/core/StreamConfig.cs#LL275C14-L275C55) in the StreamConfig.

When you open an `IEnumerator` from a state store you must call `Dispose()` on the enumerator when you are done working with it to reclaim resources; or you can use the enumerator from within an using statement. If you do not dispose an enumerator, you may encounter an OOM error.

### Enable or Disable Fault Tolerance of State Stores (Store Changelogs)

You can enable or disable fault tolerance for a state store by enabling or disabling the change logging of the store through `WithLoggingEnabled()` and `WithLoggingDisabled()`. You can also fine-tune the associated topic’s configuration if needed.

Example for disabling fault-tolerance:

```csharp
var countStoreSupplier = Stores.KeyValueStoreBuilder(
  Stores.PersistentKeyValueStore("counts"),
    new StringSerDes(),
    new Int64SerDes())
  .WithLoggingDisabled(); // disable backing up the store to a changelog topic
```

Here is an example for enabling fault tolerance, with additional changelog-topic configuration: You can add any log config from kafka.log.LogConfig. Unrecognized configs will be ignored.

```csharp
IDictionary<string, string> changelogConfig = new Dictionary<string, string>();
// override min.insync.replicas
changelogConfig.Add("min.insync.replicas", "1");

var countStoreSupplier = Stores.KeyValueStoreBuilder(
  Stores.PersistentKeyValueStore("counts"),
    new StringSerDes(),
    new Int64SerDes())
  .WithLoggingEnabled(changelogConfig); // enable changelogging, with custom changelog settings
```

### Implementing Custom State Stores

You can use the built-in state store types or implement your own. The primary interface to implement for the store is `Streamiz.Kafka.Net.Processors.IStateStore`. Streamiz also has a few extended interfaces such as `IKeyValueStore` and so on.

You also need to provide a “factory” for the store by implementing the `Streamiz.Kafka.Net.State.IStoreBuilder` interface, which Streamiz uses to create instances of your store.

### Connecting Processors and State Stores

You can easily create stateful `Processor` or `Transformer` using the `ProcessorBuilder` or `TransformerBuilder`.

Example : 

``` csharp
private class MyStatefullProcessor : IProcessor<string, string>
{
    private IKeyValueStore<string, string> store;
            
    public void Init(ProcessorContext<string, string> context)
    {
        store = (IKeyValueStore<string, string>)context.GetStateStore("my-store");
    }

    public void Process(Record<string, string> record)
    {
        store.Put(record.Key, record.Value);
    }

    public void Close()
    { }
}

// .... 
var builder = new StreamBuilder();
            
builder.Stream<string, string>("inputTopic")
       .Process(ProcessorBuilder
                    .New<string, string>()
                    .Processor<MyStatefullProcessor>()
                    .StateStore(State.Stores.KeyValueStoreBuilder(
                            State.Stores.InMemoryKeyValueStore("my-store"),
                            new StringSerDes(),
                            new StringSerDes()))
                    .Build());
```