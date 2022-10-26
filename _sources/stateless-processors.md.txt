# Stateless processors

Stateless transformations do not require state for processing and they do not require a state store associated with the stream processor. Kafka 0.11.0 and later allows you to materialize the result from a stateless IKTable transformation. This allows the result to be queried through interactive queries. To materialize a IKTable, each of the below stateless operations can be augmented with an optional queryableStoreName argument.

## Branch

Branch (or split) a IKStream based on the supplied predicates into one or more IKStream instances. (details)

Predicates are evaluated in order. A record is placed to one and only one output stream on the first match: if the n-th predicate evaluates to true, the record is placed to n-th stream. If no predicate matches, the the record is dropped.

Branching is useful, for example, to route records to different downstream topics.

- IKStream -> IKStream[]

``` csharp
IKStream<string, string> stream = ....;
var branches = stream.Branch(
                (k,v) => k.StartsWith("A"),
                (k,v) => k.StartsWith("B"),
                (k,v) => k.StartsWith("C"),
                (k,v) => true); // DLQ pattern
// branches[0] contains all records whose keys start with "A"
// branches[1] contains all records whose keys start with "B"
// branches[2] contains all records whose keys start with "C"
// branches[3] contains other records
```

## Filter

Evaluates a boolean function for each element and retains those for which the function returns true.

- IKStream -> IKStream
- IKTable -> IKTable

``` csharp
IKStream<string, string> stream = ....;
IKTable<string, string> table =  ...;

// A filter that selects  only value which contains 'test' string constant
stream.Filter((k, v) => v.Contains("test"))
table.Filter((k, v) => v.Contains("test"))
``` 

## InverseFilter

Evaluates a boolean function for each element and drops those for which the function returns true.

- IKStream -> IKStream
- IKTable -> IKTable

``` csharp
IKStream<string, string> stream = ....;
IKTable<string, string> table =  ...;

// A inverse filter that selects value which contains not 'test' string constant
stream.FilterNot((k, v) => v.Contains("test"))
table.FilterNot((k, v) => v.Contains("test"))
```

## FlatMap

Takes one record and produces zero, one, or more records. You can modify the record keys and values, including their types. 

- IKStream → IKStream

``` csharp
IKStream<string, string> stream = ....;

// Here, we generate two output records for each input record.
// We also change the key and value types.
// Example: ("KEY1", "Hello") -> ("HELLO", 100), ("HELLO", 900)
stream
    .FlatMap((k, v) =>
    {
        List<KeyValuePair<string, long>> results = new List<KeyValuePair<string, long>>();
        results.Add(KeyValuePair.Create(v.ToUpper(), 100L));
        results.Add(KeyValuePair.Create(v.ToUpper(), 900L));
        return results;
    })
```

## FlatMapValues

Takes one record and produces zero, one, or more records, while retaining the key of the original record. You can modify the record values and the value type.

flatMapValues is preferable to flatMap because it will not cause data re-partitioning. However, you cannot modify the key or key type like flatMap does.

- IKStream → IKStream

``` csharp
IKStream<string, string> stream = ....;

// Split a word into characters.
stream.FlatMapValues((k,v) => v.ToCharArray())
```

## Foreach

Terminal operation. Performs a stateless action on each record. 

You would use foreach to cause side effects based on the input data (similar to peek) and then stop further processing of the input data (unlike peek, which is not a terminal operation).

Note on processing guarantees: Any side effects of an action (such as writing to external systems) are not trackable by Kafka, which means they will typically not benefit from Kafka’s processing guarantees.

- IKStream → void

``` csharp
IKStream<string, string> stream = ....;

// Print the contents of the IKStream to the local console
stream.Foreach((k,v) => Console.WriteLine($"{k} {v}"))
```

## GroupByKey

Groups the records by the existing key.

Grouping is a prerequisite for aggregating a stream or a table and ensures that data is properly partitioned (“keyed”) for subsequent operations.

When to set explicit SerDes: Variants of GroupByKey exist to override the configured default SerDes of your application, which you must do if the key and/or value types of the resulting IKGroupedStream do not match the configured default SerDes.

- IKStream → IKGroupedStream

## GroupBy

Groups the records by a new key, which may be of a different key type. When grouping a table, you may also specify a new value and value type. groupBy is a shorthand for SelectKey(...).GroupByKey().

Grouping is a prerequisite for aggregating a stream or a table and ensures that data is properly partitioned (“keyed”) for subsequent operations.

When to set explicit SerDes: Variants of GroupBy exist to override the configured default SerDes of your application, which you must do if the key and/or value types of the resulting IKGroupedStream or IKGroupedTable do not match the configured default SerDes.

- IKStream → IKGroupedStream
- IKTable → IKGroupedTable

## Map

Takes one record and produces one record. You can modify the record key and value, including their types.

- IKStream → IKStream

``` csharp
IKStream<string, string> stream = ....;

// We create a new record keyvalue, with the value to key and key to value
stream.Map((k,v) => KeyValuePair.Create(v.ToUpper(), k.ToUpper()))
```

## MapValues

Takes one record and produces one record, while retaining the key of the original record. You can modify the record value and the value type.

MapValues is preferable to map because it will not cause data re-partitioning. However, it does not allow you to modify the key or key type like map does.

- IKStream → IKStream
- IKTable → IKTable

``` csharp
IKStream<string, string> stream = ....;
IKTable<string, string> table = ...;

// New value type => Int32 which is the length of string value
stream.MapValues((k,v) => v.Length)
table.MapValues((k,v) => v.Length)
```

## Peek

Performs a stateless action on each record, and returns an unchanged stream.

You would use peek to cause side effects based on the input data (similar to foreach) and continue processing the input data (unlike foreach, which is a terminal operation). peek returns the input stream as-is; if you need to modify the input stream, use map or mapValues instead.

Peek is helpful for use cases such as logging or tracking metrics or for debugging and troubleshooting.

Note on processing guarantees: Any side effects of an action (such as writing to external systems) are not trackable by Kafka, which means they will typically not benefit from Kafka’s processing guarantees.

- IKStream → IKStream

``` csharp
IKStream<string, string> stream = ....;

stream.Peek((k,v) => Console.WriteLine($"{k} {v}"))
```

## Print

Terminal operation. Prints the records to Sys Out.

Calling Print() is the same as calling Foreach((key, value) => Console.WriteLine($"{k} {v}"))

Print is mainly for debugging/testing purposes, and it will try to flush on each record print. Hence it should not be used for production usage if performance requirements are concerned.

- IKStream → void

``` csharp
IKStream<string, string> stream = ....

// New value type => Int32 which is the lenght of string value
stream.Print(Printed<string, string>.ToOut())
```

## SelectKey

Assigns a new key – possibly of a new key type – to each record.

Calling SelectKey(...) is the same as calling Map((key, value) => ...)

Marks the stream for data re-partitioning: Applying a grouping or a join after selectKey will result in re-partitioning of the records.

- IKStream → IKStream

``` csharp
IKStream<string, string> stream = ....;

// Derive a new record key from the record's value.
stream.SelectKey((k,v) => v.Length)
```

## Table to Steam

Get the changelog stream of this table.

- IKTable → IKStream

``` csharp
IKTable<string, string> table = ....;
// Also, a variant of `ToStream` exists that allows you
// to select a new key for the resulting stream.
IKStream<string, string> = table.ToStream();
```

## Repartition

Manually trigger repartitioning of the stream with desired number of partitions.

Generated topic is treated as internal topic, as a result data will be purged automatically as any other internal repartition topic. In addition, you can specify the desired number of partitions, which allows to easily scale in/out downstream sub-topologies.

``` csharp
IKStream<string, string> stream = ....;
IKStream<string, string> repartitionedStream = stream.Repartition(Repartitioned<string, string>.NumberOfPartitions(10));
```

## MapAsync

Takes one record and produces one record. You can modify the record key and value, including their types.This operation is asynchronous and will create a request/response pattern. This asynchronous processing will be release by a dedicated external thread and implement a retry behavior.

Use cases : Enrichment data from HTTP Api, Database SQL or Nosql, etc ...

- IKStream → IKStream

``` csharp
IKStream<string, string> stream = ....;

// We create a new record keyvalue, with the upper value to key and value
stream.MapAsync(
        async (record, context) =>
            await Task.FromResult(new KeyValuePair<string, string>(record.Value.ToUpper(), record.Value)),
        RetryPolicy.NewBuilder().NumberOfRetry(10).Build());
```

## MapValuesAsync

Takes one record and produces one record, while retaining the key of the original record. You can modify the record value and the value type.

This operation is asynchronous and will create a request/response pattern. This asynchronous processing will be release by a dedicated external thread and implement a retry behavior.

Use cases : Enrichment data from HTTP Api, Database SQL or Nosql, etc ...

- IKStream → IKStream

``` csharp
IKStream<string, string> stream = ....;

// New value type => Int32 which is the length of string value
stream.MapValuesAsync(
        async (record, context) =>
            await Task.FromResult(record.Value.Length),
        RetryPolicy.NewBuilder().NumberOfRetry(10).Build());
```

## FlatMapAsync

Takes one record and produces zero, one, or more records. You can modify the record keys and values, including their types. 

This operation is asynchronous and will create a request/response pattern. This asynchronous processing will be release by a dedicated external thread and implement a retry behavior.

Use cases : Enrichment data from HTTP Api, Database SQL or Nosql, etc ...

- IKStream → IKStream

``` csharp
IKStream<string, string> stream = ....;

// Here, we generate two output records for each input record.
// We also change the key and value types.
// Example: ("KEY1", "co") -> ("KEY1", c), ("KEY1", o)
stream
   .FlatMapAsync(
        async (record, context) =>
            await Task.FromResult(record.Value.ToCharArray().Select(c => new KeyValuePair<string,char>(record.Key, c))),
        RetryPolicy.NewBuilder().NumberOfRetry(10).Build());
```

## FlatMapValuesAsync

Takes one record and produces zero, one, or more records, while retaining the key of the original record. You can modify the record values and the value type.

This operation is asynchronous and will create a request/response pattern. This asynchronous processing will be release by a dedicated external thread and implement a retry behavior.

Use cases : Enrichment data from HTTP Api, Database SQL or Nosql, etc ...

- IKStream → IKStream

``` csharp
IKStream<string, string> stream = ....;

// Split a word into characters.
stream..FlatMapValuesAsync<char>(
            async (record, context) =>
                await Task.FromResult(record.Value.ToCharArray()),
            RetryPolicy.NewBuilder().NumberOfRetry(10).Build());
```

## ForeachAsync

Perform an asynchronous action on each record of a stream. Note that this is a terminal operation that returns void. This operation is asynchronous and will create a request/response pattern. This asynchronous processing will be release by a dedicated external thread and implement a retry behavior.

Use cases : Push data asynchronously into a sink system like Database, HTTP Api, JMS Broker, etc ..

- IKStream → void

``` csharp
IKStream<string, string> stream = ....;

// try to insert new items into a mongoDb collection
stream..ForeachAsync(
                    async (record, _) =>
                    {
                        await database
                            .GetCollection<Person>("adress")
                            .InsertOneAsync(new Person()
                            {
                                name = record.Key,
                                address = new Address()
                                {
                                    city = record.Value
                                }
                            });
                    },
                    RetryPolicy
                        .NewBuilder()
                        .NumberOfRetry(10)
                        .RetryBackOffMs(100)
                        .RetriableException<Exception>()
                        .RetryBehavior(EndRetryBehavior.SKIP)
                        .Build());
```

## Process

**Terminal operation.** Applies a Processor to each record. `Process(...)` allows you to leverage the Processor API from the DSL.

**Be carefull, if you want interact with an external system.** Please use `ForeachAsync` instead.

- IKStream → void

``` csharp
var builder = new StreamBuilder();
            
builder.Stream<string, string>("topic")
    .Process(ProcessorBuilder
        .New<string, string>()
        .Processor((record) =>
        {
           // what you want ...
        })
        .Build());
```

## Transform

Applies a Transformer to each record. `Transform(..)` allows you to leverage the Processor API from the DSL.

Each input record is transformed into zero or one record (similar to the stateless `Map`). The Transformer must return null for zero output. You can modify the record’s key and value, including their types.

**Marks the stream for data re-partitioning:** Applying a grouping or a join after transform will result in re-partitioning of the records. If possible use `TransformValues(...)` instead, which will not cause data re-partitioning.

**Be carefull, if you want interact with an external system.** Please use `MapAsync(...), MapValuesAsync(...), FlatMapAsync(...) or FlatMapValuesAsync(...)` instead.

- IKStream → IKStream

``` csharp
var builder = new StreamBuilder();
            
builder.Stream<string, string>("topic")
    .Transform(TransformerBuilder
        .New<string, string, string, string>()
        .Transformer((record) => 
                Record<string, string>.Create(record.Key.ToUpper(), record.Value.ToUpper()))
        .Build())
    .To("topic-output");
```

## TransformValues

Applies a Transformer to each record, while retaining the key of the original record (even if you change the key into the output `Record`). `TransformValues(..)` allows you to leverage the Processor API from the DSL.

Each input record is transformed into zero or one output record (similar to the stateless `MapValues`). The Transformer must return null for zero output. You can modify the record’s  value, including his type.

`TransformValues(...)` is preferable to `Transform(...)` because it will not cause data re-partitioning.

- IKStream → IKStream

``` csharp
var builder = new StreamBuilder();
            
builder.Stream<string, string>("topic")
                .TransformValues(TransformerBuilder
                    .New<string, string, string, string>()
                    .Transformer((record) => Record<string, string>.Create(record.Value.ToUpper()))
                    .Build())
        .To("topic-output");
```