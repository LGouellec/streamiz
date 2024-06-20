# State stores

⚠️ <span style="color:red">**Some things you need to know**</span> ⚠️

- From 1.4.0 release, the default state store is a RocksDb state store. (Before 1.4.0, the default state store was a in memory state store.)
- RocksDb state store is available from 1.2.0 release.
- By default, a state store is tracked by a changelog topic from 1.2.0 release. (If you don't need, you have to make it explicit).

## Basics

A stateful processor may use one or more state stores. Each task that contains a stateful processor has exclusive access to the state stores in the processor. That means a topology with two state stores and five input partitions will lead to five tasks, and each task will own two state stores resulting in 10 state stores in total for your Kafka Streams application.

State stores in Streamiz are layered in four ways :
- The outermost layer collects metrics about the operations on the state store and serializes and deserializes the records that are written to and read from the state store.
- The next layer caches the records. If a record exists in the cache with the same key as a new record, the cache overwrites the existing record with the new record; otherwise, the cache adds a new entry for the new record. The cache is the primary serving area for lookups. If a lookup can’t find a record with a given key in the cache, it is forwarded to the next layer. If this lookup returns an entry, the entry is added to the cache. If the cache exceeds its configured size during a write, the cache evicts the records that have been least recently used and sends new and overwritten records downstream. The caching layer decreases downstream traffic because no updates are sent downstream unless the cache evicts records or is flushed. The cache’s size is configurable. If it is set to zero, the cache is disabled. (`EARLY ACCESS ONLY`)
- The changelogging layer sends each record updated in the state store to a topic in Kafka—the state’s changelog topic. The changelog topic is a compacted topic that replicates the data in the local state. Changelogging is needed for fault tolerance, as we will explain below.
- The innermost layer updates and reads the local state store.

![state-store](./assets/state-store.png)

## In Memory key/value store

As his name, this is an inmemory key value state store which is supplied by InMemoryKeyValueBytesStoreSupplier.
You have an child materialized class to help you to use it.

Usefull with statefull operation like Count, Aggregate, Reduce but also to materialized IKTable<K, V> or IGlobalKTable<K, V>

Example :
``` csharp

builder.Table("test-ktable", InMemory.As<string, string>("test-store"));

builder
        .Stream<string, string>("topic")
        .GroupBy((k, v) => k.ToUpper())
        .Aggregate(
            () => 0L,
            (k, v, agg) => agg + 1,
            InMemory.As<string, long>("agg-store").WithValueSerdes<Int64SerDes>()
        );
```

**Be carefull, this state store is not persistent ! So after each application restart, you loose the state of your state store.**

## In Memory window store

As his name, this is an inmemory windows state store which is supplied by InMemoryWindowStoreSupplier.
You have an child materialized class to help you to use it.

Usefull with windowing statefull operation like Count, Aggregate, Reduce in ITimeWindowedKStream<K, V>

Example :
``` csharp
builder
        .Stream<string, string>("topic")
        .GroupByKey()
        .WindowedBy(TumblingWindowOptions.Of(2000))
        .Aggregate(
            () => 0,
            (k, v, agg) => Math.Max(v.Length, agg),
            InMemoryWindows.As<string, int>("store").WithValueSerdes<Int32SerDes>()
        )
        .ToStream()
        .To<StringTimeWindowedSerDes, Int32SerDes>("output");
```

**Be carefull, this state store is not persistent ! So after each application restart, you loose the state of your state store.**

## RocksDb key/value store

As his name, this is a rocksdb key value state store which is supplied by RocksDbKeyValueBytesStoreSupplier. 

The state store is persisted on disk at `{config.StateDir}/{config.ApplicationId}/{taskId}/rocksdb/{store.Name}`.

You have an child materialized class to help you to use it.

Usefull with statefull operation like Count, Aggregate, Reduce but also to materialized IKTable<K, V> or IGlobalKTable<K, V>

Example :
``` csharp

builder.Table("test-ktable", RocksDb.As<string, string>("test-store"));

 builder
    .Stream<string, string>("topic")
    .GroupBy((k, v) => k.ToUpper())
    .Aggregate(
        () => 0L,
        (k, v, agg) => agg + 1,
        RocksDb.As<string, long>("agg-store").WithValueSerdes<Int64SerDes>()
    );
```

## RocksDb window store

As his name, this is a rocksdb windows state store which is supplied by RocksDbWindowBytesStoreSupplier.

This state store save data in 3 segments, each segment is a rocksdb properly and contains certain time range. This for optimization purposes and for retention periods (window size + grace + window-additional-retention).

You have an child materialized class to help you to use it.

Usefull with windowing statefull operation like Count, Aggregate, Reduce in ITimeWindowedKStream<K, V>

Example :
``` csharp
builder
        .Stream<string, string>("topic")
        .GroupByKey()
        .WindowedBy(TumblingWindowOptions.Of(2000))
        .Aggregate(
            () => 0,
            (k, v, agg) => Math.Max(v.Length, agg),
            RocksDbWindows.As<string, int>("store").WithValueSerdes<Int32SerDes>()
        )
        .ToStream()
        .To<StringTimeWindowedSerDes, Int32SerDes>("output");
```

## Caching

**This feature is available in `EARLY ACCESS` only.**

Streamiz offers robust capabilities for stream processing applications, including efficient data caching mechanisms. Caching optimizes performance by minimizing redundant computations, reducing latency, and enhancing overall throughput within stream processing pipelines.

**Purpose of Caching**

Caching in Streamiz serves several key purposes:
- **Reduction of Redundant Computations**: Stores intermediate results to avoid recomputing data unnecessarily.
- **Performance Optimization**: Minimizes latency by reducing the need for repeated expensive computations or data fetches.
- **Fault Tolerance**: Ensures resilience by maintaining readily accessible intermediate results for recovery during failures.

**Types of Caching**

Streamiz supports one primary type of caching:
1. **State Store Cache**: Enhances processing efficiency by caching state store entries, reducing the overhead of state fetches and updates.

### Configuring Caching
To optimize caching behavior, Streamiz offers configurable parameters:
- **Cache Size**: Defines the maximum size of the cache to balance performance and memory usage. To be fair, this cache size is the maximum cache size per state store. 
- **Enable caching** : By default, caching is disabled for all stateful processors in Streamiz. **This feature will be enabled by default soon.** You can enable the caching feature with `InMemory.WithCachingEnabled()`, `InMemoryWindows.WithCachingEnabled()`, `RocksDb.WithCachingEnabled()` or `RocksDbWindows.WithCachingEnabled()`

``` csharp
// Enable caching for a rocksdb window store
RocksDbWindows.As<string, long>("count-window-store")
        .WithKeySerdes(new StringSerDes())
        .WithValueSerdes(new Int64SerDes())
        .WithCachingEnabled();

// Enable caching for an in-memory window store
InMemoryWindows.As<string, long>("count-window-store")
        .WithKeySerdes(new StringSerDes())
        .WithValueSerdes(new Int64SerDes())
        .WithCachingEnabled();

// Enable caching for an rocksdb key/value store
RocksDb.As<string, long>("count-store")
        .WithKeySerdes(new StringSerDes())
        .WithValueSerdes(new Int64SerDes())
        .WithCachingEnabled();

// Enable caching for an in-memory key/value store
InMemory.As<string, long>("count-store")
        .WithKeySerdes(new StringSerDes())
        .WithValueSerdes(new Int64SerDes())
        .WithCachingEnabled();
```