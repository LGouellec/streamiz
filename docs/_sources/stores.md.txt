# State stores

⚠️ <span style="color:red">**Only no persistent store are supported !**</span> => RocksDB implementation will arrive soon. ⚠️

**No changelog topics are created with In Memory store for moment !**

## In Memory

As his name, this is an inmemory key value state store which is supplied by InMemoryKeyValueBytesStoreSupplier.
You have an child materialized class to help you to use it.

It can be use in statefull operation like Count, Aggregate, Reduce but also to materialized IKTable<K, V> or IGlobalKTable<K, V>

Example :
``` csharp

builder.Table("test-ktable", InMemory<string, string>.As("test-store"));

builder
        .Stream<string, string>("topic")
        .GroupBy((k, v) => k.ToUpper())
        .Aggregate(
            () => 0L,
            (k, v, agg) => agg + 1,
            InMemory<string, long>.As("agg-store").WithValueSerdes<Int64SerDes>()
        );
```

**Be carefull, this state store is not persistent ! So after each application restart, you loose the state of your state store.**

## In Memory Windows

As his name, this is an inmemory windows state store which is supplied by InMemoryWindowStoreSupplier.
You have an child materialized class to help you to use it.

It can be use in windowing statefull operation like Count, Aggregate, Reduce in ITimeWindowedKStream<K, V>

Example :
``` csharp
builder
        .Stream<string, string>("topic")
        .GroupByKey()
        .WindowedBy(TumblingWindowOptions.Of(2000))
        .Aggregate(
            () => 0,
            (k, v, agg) => Math.Max(v.Length, agg),
            InMemoryWindows<string, int>.As("store").WithValueSerdes<Int32SerDes>()
        )
        .ToStream()
        .To<StringTimeWindowedSerDes, Int32SerDes>("output");
```

**Be carefull, this state store is not persistent ! So after each application restart, you loose the state of your state store.**