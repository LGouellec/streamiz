# Statefull processors

Stateful transformations depend on state for processing inputs and producing outputs and require a state store associated with the stream processor. For example, in aggregating operations, a windowing state store is used to collect the latest aggregation results per window. In join operations, a windowing state store is used to collect all of the records received so far within the defined window boundary.

IMPLEMENTATION WORK IN PROGRESS

**To follow the progress, you can star [the Github project](https://github.com/LGouellec/kafka-streams-dotnet) and watch it !** 

|Operator Name|Method|TODO|IMPLEMENTED|TESTED|DOCUMENTED|
|---|---|---|---|---|---|
|Aggregate|KGroupedStream -> KTable|   |   |   |&#9745;|
|Aggregate|KGroupedTable -> KTable|   |   |   |&#9745;|
|Aggregate(windowed)|KGroupedStream -> KTable|   |   |   |&#9745;|
|Count|KGroupedStream -> KTable|   |   |   |&#9745;|
|Count|KGroupedTable -> KTable|   |   |   |&#9745;|
|Count(windowed)|KGroupedStream → KStream|   |   |   |&#9745;|
|Reduce|KGroupedStream → KTable|   |   |   |&#9745;|
|Reduce|KGroupedTable → KTable|   |   |   |&#9745;|
|Reduce(windowed)|KGroupedStream → KTable|   |   |   |&#9745;|
|InnerJoin(windowed)|(KStream,KStream) → KStream|&#9745;|   |   |   |
|LeftJoin(windowed)|(KStream,KStream) → KStream|&#9745;|   |   |   |
|OuterJoin(windowed)|(KStream,KStream) → KStream|&#9745;|   |   |   |
|InnerJoin|(KTable,KTable) → KTable|&#9745;|   |   |   |
|LeftJoin|(KTable,KTable) → KTable|&#9745;|   |   |   |
|OuterJoin|(KTable,KTable) → KTable|&#9745;|   |   |   |
|InnerJoin|(KStream,KTable) → KStream|&#9745;|   |   |   |
|LeftJoin|(KStream,KTable) → KStream|&#9745;|   |   |   |
|InnerJoin|(KStream,GlobalKTable) → KStream|   |&#9745;|   |   |
|LeftJoin|(KStream,GlobalKTable) → KStream|&#9745;|   |   |   |

## Count

**Rolling aggregation.** Counts the number of records by the grouped key. (see IKGroupedStream for details)

Several variants of count exist.

- IKGroupedStream  → IKTable
- IKGroupedTable  -> IKTable

``` csharp
var groupedStream = builder
                        .Stream<string, string>("test")
                        .GroupBy((k, v) => k.ToUpper());

// Counting a IKGroupedStream
var table = groupedStream.Count(InMemory<string, long>.As("count-store"));

var table2 = builder
                .Table<string, string>("topic")
                .GroupBy((k, v) => KeyValuePair.Create(k.ToCharArray()[0], v))
                .Count(InMemory<char, long>.As("count-store2").WithKeySerdes(new CharSerDes()));
```

Detailed behavior for IKGroupedStream:
- Input records with null keys or values are ignored.

Detailed behavior for IKGroupedTable:
- Input records with null keys are ignored. Records with null values are not ignored but interpreted as “tombstones” for the corresponding key, which indicate the deletion of the key from the table.

## Count (Windowed)

** Windowed aggregation.** Counts the number of records, per window, by the grouped key. (ITimeWindowedKStream details)

The windowed count turns a ITimeWindowedKStream<K, V> into a windowed IKTable<Windowed<K>, V>.

``` csharp
var groupedStream = builder
                        .Stream<string, string>("topic")
                        .GroupByKey();

var countStream = groupedStream
                    .WindowedBy(TumblingWindowOptions.Of(2000))
                    .Count(m);
```

Detailed behavior:
- Input records with null keys or values are ignored.

## Aggregate

**Rolling aggregation.** Aggregates the values of (non-windowed) records by the grouped key. Aggregating is a generalization of reduce and allows, for example, the aggregate value to have a different type than the input values. (see IKGroupedStream for details)

When aggregating a grouped stream, you must provide an initializer (e.g., aggValue = 0) and an “adder” aggregator (e.g., aggValue + curValue).

Several variants of aggregate exist.

- KGroupedStream → KTable
- IKGroupedTable  -> IKTable

``` csharp
var groupedStream = builder
                        .Stream<string, string>("test")
                        .GroupBy((k, v) => k.ToUpper());

var table = groupedStream.Aggregate(() => 0L, (k,v,agg) => agg+ 1, InMemory<string, long>.As("agg-store").WithValueSerdes<Int64SerDes>());

var table2 = builder
                .Table<string, string>("topic")
                .GroupBy((k, v) => KeyValuePair.Create(k.ToUpper(), v))
                .Aggregate(
                    () => new Dictionary<char, int>(),
                    (k, v, old) =>
                    {
                        var caracs = v.ToCharArray();
                        foreach (var c in caracs)
                        {
                            if (old.ContainsKey(c))
                                ++old[c];
                            else
                                old.Add(c, 1);
                        }
                        return old;
                    },
                    (k, v, old) => old,
                    InMemory<string, Dictionary<char, int>>.As("agg-store2").WithValueSerdes<DictionarySerDes>()
                );
```

Detailed behavior of IKGroupedStream:
- Input records with null keys are ignored.
- When a record key is received for the first time, the initializer is called (and called before the adder).
- Whenever a record with a non-null value is received, the adder is called.

Detailed behavior of IKGroupedTable:
- Input records with null keys are ignored.
- When a record key is received for the first time, the initializer is called (and called before the adder and subtractor). Note that, in contrast to IKGroupedStream, over time the initializer may be called more than once for a key as a result of having received input tombstone records for that key (see below).
- When the first non-null value is received for a key (e.g., INSERT), then only the adder is called.
- When subsequent non-null values are received for a key (e.g., UPDATE), then (1) the subtractor is called with the old value as stored in the table and (2) the adder is called with the new value of the input record that was just received. The order of execution for the subtractor and adder is not defined.
- When a tombstone record – i.e. a record with a null value – is received for a key (e.g., DELETE), then only the subtractor is called. Note that, whenever the subtractor returns a null value itself, then the corresponding key is removed from the resulting IKTable. If that happens, any next input record for that key will trigger the initializer again.

## Aggregate (Windowed)

**Windowed aggregation.** Aggregates the values of records, per window, by the grouped key. Aggregating is a generalization of reduce and allows, for example, the aggregate value to have a different type than the input values. (ITimeWindowedKStream details)

You must provide an initializer (e.g., aggValue = 0), “adder” aggregator (e.g., aggValue + curValue), and a window. When windowing based on sessions, you must additionally provide a “session merger” aggregator (e.g., mergedAggValue = leftAggValue + rightAggValue).

The windowed aggregate turns a ITimeWindowedKStream<K, V> into a windowed KTable<Windowed<K>, V>.

```csharp

var groupedStream = builder
                        .Stream<string, string>("topic")
                        .GroupByKey();

var aggStream = groupedStream
                    .WindowedBy(TumblingWindowOptions.Of(2000))
                    .Aggregate(
                        () => 0,
                        (k, v, agg) => Math.Max(v.Length, agg),
                        m);
```

Detailed behavior:
- The windowed aggregate behaves similar to the rolling aggregate described above. The additional twist is that the behavior applies per window.
- Input records with null keys are ignored in general.
- When a record key is received for the first time for a given window, the initializer is called (and called before the adder).
- Whenever a record with a non-null value is received for a given window, the adder is called.

## Reduce

**Rolling aggregation.** Combines the values of (non-windowed) records by the grouped key. The current record value is combined with the last reduced value, and a new reduced value is returned. The result value type cannot be changed, unlike aggregate. (see IKGroupedStream for details)

When reducing a grouped stream, you must provide an “adder” reducer (e.g., aggValue + curValue).

Several variants of reduce exist.

- IKGroupedStream  → IKTable
- IKGroupedTable  -> IKTable

``` csharp
var groupedStream = builder
                        .Stream<string, string>("test")
                        .MapValues(v => v.Length)
                        .GroupBy((k, v) => k.ToUpper());

// Reduce a IKGroupedStream
var table = groupedStream.Reduce((agg, new) => agg + new, InMemory<string, int>.As("reduce-store").WithValueSerdes<Int32SerDes>());

var table2 = builder
                .Table<string, string>("topic")
                .MapValues(v => v.Length)
                .GroupBy((k, v) => KeyValuePair.Create(k.ToUpper(), v))
                .Reduce(
                    (v1, v2) => Math.Max(v1, v2),
                    (v1, v2) => Math.Max(v1, v2),
                    InMemory<string, int>.As("reduce-store2").WithValueSerdes<Int32SerDes>());
```

Detailed behavior for IKGroupedStream:
- Input records with null keys are ignored in general.
- When a record key is received for the first time, then the value of that record is used as the initial aggregate value.
- Whenever a record with a non-null value is received, the adder is called.

Detailed behavior for IKGroupedTable:
- Input records with null keys are ignored in general.
- When a record key is received for the first time, then the value of that record is used as the initial aggregate value. Note that, in contrast to IKGroupedStream, over time this initialization step may happen more than once for a key as a result of having received input tombstone records for that key (see below).
- When the first non-null value is received for a key (e.g., INSERT), then only the adder is called.
- When subsequent non-null values are received for a key (e.g., UPDATE), then (1) the subtractor is called with the old value as stored in the table and (2) the adder is called with the new value of the input record that was just received. The order of execution for the subtractor and adder is not defined.
- When a tombstone record – i.e. a record with a null value – is received for a key (e.g., DELETE), then only the subtractor is called. Note that, whenever the subtractor returns a null value itself, then the corresponding key is removed from the resulting IKTable. If that happens, any next input record for that key will re-initialize its aggregate value.

## Reduce (Windowed)

**Windowed aggregation.** Combines the values of records, per window, by the grouped key. The current record value is combined with the last reduced value, and a new reduced value is returned. Records with null key or value are ignored. The result value type cannot be changed, unlike aggregate. (ITimeWindowedKStream details)

The windowed reduce turns a turns a ITimeWindowedKStream<K, V> into a windowed KTable<Windowed<K>, V>.

```csharp
var groupedStream = builder
                        .Stream<string, string>("topic")
                        .GroupByKey();

var reduceStream = groupedStream
                        .WindowedBy(TumblingWindowOptions.Of(2000))
                        .Reduce((v1, v2) => v1.Length > v2.Length ? v1 : v2);
```

Detailed behavior:
- The windowed reduce behaves similar to the rolling reduce described above. The additional twist is that the behavior applies per window.
- Input records with null keys are ignored in general.
- When a record key is received for the first time for a given window, then the value of that record is used as the initial aggregate value.
- Whenever a record with a non-null value is received for a given window, the reducer is called.