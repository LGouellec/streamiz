# State stores

⚠️ <span style="color:red">**Some things you need to know**</span> ⚠️

- From 1.4.0 release, the default state store is a RocksDb state store. (Before 1.4.0, the default state store was a in memory state store.)
- RocksDb state store is available from 1.2.0 release.
- By default, a state store is tracked by a changelog topic from 1.2.0 release. (If you don't need, you have to make it explicit).

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