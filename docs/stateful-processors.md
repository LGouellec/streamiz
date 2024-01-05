# Stateful processors

Stateful transformations depend on state for processing inputs and producing outputs and require a state store associated with the stream processor. For example, in aggregating operations, a windowing state store is used to collect the latest aggregation results per window. In join operations, a windowing state store is used to collect all of the records received so far within the defined window boundary.

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

- KGroupedStream → IKTable
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

The windowed aggregate turns a ITimeWindowedKStream<K, V> into a windowed IKTable<Windowed<K>, V>.

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

The windowed reduce turns a turns a ITimeWindowedKStream<K, V> into a windowed IKTable<Windowed<K>, V>.

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

## IKStream-IKStream Join

IKStream-IKStream joins are always windowed joins, because otherwise the size of the internal state store used to perform the join – e.g., a sliding window or “buffer” – would grow indefinitely. For stream-stream joins it’s important to highlight that a new input record on one side will produce a join output for each matching record on the other side, and there can be multiple such matching records in a given join window.

### Inner Join (Windowed)

- (IKStream, IKStream) → IKStream

Performs an INNER JOIN of this stream with another stream. Even though this operation is windowed, the joined stream will be of type IKStream<K, V> rather than IKStream<Windowed<K>, V>.

Data must be co-partitioned: The input data for both sides must be co-partitioned.

Causes data re-partitioning of a stream if and only if the stream was marked for re-partitioning (if both are marked, both are re-partitioned).

```csharp
var stream1 = builder.Stream<string, string>("topic1");
var stream2 = builder.Stream<string, string>("topic2");

stream1.Join(stream2, 
                    (v1, v2) => $"{v1}-{v2}",
                    JoinWindowOptions.Of(TimeSpan.FromMinutes(1)))
        .To("output-join");
```

Detailed behavior:
- The join is key-based, i.e. with the join predicate leftRecord.key == rightRecord.key, and window-based, i.e. two input records are joined if and only if their timestamps are “close” to each other as defined by the user-supplied JoinWindows, i.e. the window defines an additional join predicate over the record timestamps.
- The join will be triggered under the conditions listed below whenever new input is received. When it is triggered, the user-supplied ValueJoiner will be called to produce join output records.
- Input records with a null key or a null value are ignored and do not trigger the join. See the semantics overview at the bottom of this section for a detailed description.

### Left Join (Windowed)

- (IKStream, IKStream) → IKStream

Performs a LEFT JOIN of this stream with another stream. Even though this operation is windowed, the joined stream will be of type IKStream<K, V> rather than IKStream<Windowed<K>, V>.

Data must be co-partitioned: The input data for both sides must be co-partitioned.

Causes data re-partitioning of a stream if and only if the stream was marked for re-partitioning (if both are marked, both are re-partitioned).

```csharp
var stream1 = builder.Stream<string, string>("topic1");
var stream2 = builder.Stream<string, string>("topic2");

stream1.LeftJoin(stream2, 
                    (v1, v2) => $"{v1}-{v2}",
                    JoinWindowOptions.Of(TimeSpan.FromMinutes(1)))
        .To("output-join");
```

Detailed behavior:
- The join is key-based, i.e. with the join predicate leftRecord.key == rightRecord.key, and window-based, i.e. two input records are joined if and only if their timestamps are “close” to each other as defined by the user-supplied JoinWindows, i.e. the window defines an additional join predicate over the record timestamps.
- The join will be triggered under the conditions listed below whenever new input is received. When it is triggered, the user-supplied ValueJoiner will be called to produce join output records.
- Input records with a null key or a null value are ignored and do not trigger the join. For each input record on the left side that does not have any match on the right side, the joiner will be called with joiner(leftRecord.value, null); this explains the row with timestamp=3 in the table below, which lists [A, null] in the LEFT JOIN column.

### Outer Join (Windowed)

- (IKStream, IKStream) → IKStream

Performs a OUTER JOIN of this stream with another stream. Even though this operation is windowed, the joined stream will be of type IKStream<K, V> rather than IKStream<Windowed<K>, V>.

Data must be co-partitioned: The input data for both sides must be co-partitioned.

Causes data re-partitioning of a stream if and only if the stream was marked for re-partitioning (if both are marked, both are re-partitioned).

```csharp
var stream1 = builder.Stream<string, string>("topic1");
var stream2 = builder.Stream<string, string>("topic2");

stream1.OuterJoin(stream2, 
                    (v1, v2) => $"{v1}-{v2}",
                    JoinWindowOptions.Of(TimeSpan.FromMinutes(1)))
        .To("output-join");
```

Detailed behavior:
- The join is key-based, i.e. with the join predicate leftRecord.key == rightRecord.key, and window-based, i.e. two input records are joined if and only if their timestamps are “close” to each other as defined by the user-supplied JoinWindows, i.e. the window defines an additional join predicate over the record timestamps.
- The join will be triggered under the conditions listed below whenever new input is received. When it is triggered, the user-supplied ValueJoiner will be called to produce join output records.
- Input records with a null key or a null value are ignored and do not trigger the join. For each input record on one side that does not have any match on the other side, the joiner will be called with joiner(leftRecord.value, null) or joiner(null, rightRecord.value), respectively; this explains the row with timestamp=3 in the table below, which lists [A, null] in the OUTER JOIN column (unlike LEFT JOIN, [null, x] is possible, too, but no such example is shown in the table).

## IKStream-IKTable Join

IKStream-IKTable joins are always non-windowed joins. They allow you to perform table lookups against a IKTable (changelog stream) upon receiving a new record from the IKStream (record stream). An example use case would be to enrich a stream of user activities (IKStream) with the latest user profile information (IKTable).

### Inner Join

- (IKStream, IKTable) → IKStream

Performs an INNER JOIN of this stream with the table, effectively doing a table lookup.

Data must be co-partitioned: The input data for both sides must be co-partitioned.

Causes data re-partitioning of the stream if and only if the stream was marked for re-partitioning.

```csharp
var stream1 = builder.Stream<string, string>("topic1");
var table = builder.Table<string, string>("topic2", InMemory<string, string>.As("table-store"));

stream1.Join(table,(v1, v2) => $"{v1}-{v2}")
        .To("output-join");
```

Detailed behavior:
- The join is key-based, i.e. with the join predicate leftRecord.key == rightRecord.key.
- The join will be triggered under the conditions listed below whenever new input is received. When it is triggered, the user-supplied joiner will be called to produce join output records.
- Only input records for the left side (stream) trigger the join. Input records for the right side (table) update only the internal right-side join state.
Input records for the stream with a null key or a null value are ignored and do not trigger the join.
Input records for the table with a null value are interpreted as tombstones for the corresponding key, which indicate the deletion of the key from the table. Tombstones do not trigger the join.

### Left Join

- (IKStream, IKTable) → IKStream

Performs an LEFT JOIN of this stream with the table, effectively doing a table lookup.

Data must be co-partitioned: The input data for both sides must be co-partitioned.

Causes data re-partitioning of the stream if and only if the stream was marked for re-partitioning.b

```csharp
var stream1 = builder.Stream<string, string>("topic1");
var table = builder.Table<string, string>("topic2", InMemory<string, string>.As("table-store"));

stream1.LeftJoin(table,(v1, v2) => $"{v1}-{v2}")
        .To("output-join");
```

Detailed behavior:
- The join is key-based, i.e. with the join predicate leftRecord.key == rightRecord.key.
- The join will be triggered under the conditions listed below whenever new input is received. When it is triggered, the user-supplied joiner will be called to produce join output records.
- Only input records for the left side (stream) trigger the join. Input records for the right side (table) update only the internal right-side join state.
Input records for the stream with a null key or a null value are ignored and do not trigger the join.
Input records for the table with a null value are interpreted as tombstones for the corresponding key, which indicate the deletion of the key from the table. Tombstones do not trigger the join.
For each input record on the left side that does not have any match on the right side, the ValueJoiner will be called with joiner(leftRecord.value, null); this explains the row with timestamp=3 in the table below, which lists [A, null] in the LEFT JOIN column.

## IKStream-IGlobalKTable Join

IKStream-IGlobalKTable joins are always non-windowed joins. They allow you to perform table lookups against a IGlobalKTable (entire changelog stream) upon receiving a new record from the IKStream (record stream). An example use case would be “star queries” or “star joins”, where you would enrich a stream of user activities (IKStream) with the latest user profile information (IGlobalKTable) and further context information (further GlobalKTables). However, because GlobalKTables have no notion of time, a IKStream-IGlobalKTable join is not a temporal join, and there is no event-time synchronization between updates to a IGlobalKTable and processing of IKStream records.

At a high-level, IKStream-IGlobalKTable joins are very similar to IKStream-IKTable joins. However, global tables provide you with much more flexibility at the some expense when compared to partitioned tables:

- They do not require data co-partitioning.
- They allow for efficient “star joins”; i.e., joining a large-scale “facts” stream against “dimension” tables
- They allow for joining against foreign keys; i.e., you can lookup data in the table not just by the keys of records in the stream, but also by data in the record values.
- They make many use cases feasible where you must work on heavily skewed data and thus suffer from hot partitions.
- They are often more efficient than their partitioned IKTable counterpart when you need to perform multiple joins in succession.

### Inner Join

- (IKStream, IGlobalKTable) → IKStream

Performs an INNER JOIN of this stream with the global table, effectively doing a table lookup. (details)

The IGlobalKTable is fully bootstrapped upon (re)start of a KafkaStreams instance, which means the table is fully populated with all the data in the underlying topic that is available at the time of the startup. The actual data processing begins only once the bootstrapping has completed.

```csharp
var stream1 = builder.Stream<string, string>("topic1");
var table = builder.GlobalTable<string, string>("topic2", InMemory<string, string>.As("table-store"));

stream1.Join(table, 
                (k,v) => k.ToUpper(), 
                (v1, v2) => $"{v1}-{v2}")
        .To("output-join");
```

Detailed behavior:
- The join is indirectly key-based, i.e. with the join predicate keymapper(leftRecord.key, leftRecord.value) == rightRecord.key.
- The join will be triggered under the conditions listed below whenever new input is received. When it is triggered, the user-supplied ValueJoiner will be called to produce join output records.
- Only input records for the left side (stream) trigger the join. Input records for the right side (table) update only the internal right-side join state.
Input records for the stream with a null key or a null value are ignored and do not trigger the join.
Input records for the table with a null value are interpreted as tombstones, which indicate the deletion of a record key from the table. Tombstones do not trigger the join.

### Left Join

- (IKStream, IGlobalKTable) → IKStream

Performs an LEFT JOIN of this stream with the global table, effectively doing a table lookup. (details)

The IGlobalKTable is fully bootstrapped upon (re)start of a KafkaStreams instance, which means the table is fully populated with all the data in the underlying topic that is available at the time of the startup. The actual data processing begins only once the bootstrapping has completed.

```csharp
var stream1 = builder.Stream<string, string>("topic1");
var table = builder.GlobalTable<string, string>("topic2", InMemory<string, string>.As("table-store"));

stream1.LeftJoin(table, 
                (k,v) => k.ToUpper(), 
                (v1, v2) => $"{v1}-{v2}")
        .To("output-join");
```

Detailed behavior:
- The join is indirectly key-based, i.e. with the join predicate keymapper(leftRecord.key, leftRecord.value) == rightRecord.key.
- The join will be triggered under the conditions listed below whenever new input is received. When it is triggered, the user-supplied ValueJoiner will be called to produce join output records.
- Only input records for the left side (stream) trigger the join. Input records for the right side (table) update only the internal right-side join state.
Input records for the stream with a null key or a null value are ignored and do not trigger the join.
Input records for the table with a null value are interpreted as tombstones, which indicate the deletion of a record key from the table. Tombstones do not trigger the join.
For each input record on the left side that does not have any match on the right side, the joiner will be called with joiner(leftRecord.value, null)

## IKTable-IKTable Equi-Join

IKTable-IKTable equi-joins are always non-windowed joins. They are designed to be consistent with their counterparts in relational databases. The changelog streams of both IKTables are materialized into local state stores to represent the latest snapshot of their table duals. The join result is a new IKTable that represents the changelog stream of the join operation.

### Inner Join

- (IKTable, IKTable) → IKTable

Performs an INNER JOIN of this table with another table. The result is an ever-updating KTable that represents the "current" result of the join.

Data must be co-partitioned: The input data for both sides must be co-partitioned.

```csharp
var table1 = builder.Table<string, string>("topic2", InMemory<string, string>.As("table1-store"));
var table2 = builder.Table<string, string>("topic2", InMemory<string, string>.As("table2-store"));

var tableJoin = table1.Join(table2,(v1, v2) => $"{v1}-{v2}");
```

Detailed behavior:
- The join is key-based, i.e. with the join predicate leftRecord.key == rightRecord.key.
- The join will be triggered under the conditions listed below whenever new input is received. When it is triggered, the user-supplied value joiner will be called to produce join output records.
- Input records with a null key are ignored and do not trigger the join.
- Input records with a null value are interpreted as tombstones for the corresponding key, which indicate the deletion of the key from the table. Tombstones do not trigger the join. When an input tombstone is received, then an output tombstone is forwarded directly to the join result IKTable if required (i.e. only if the corresponding key actually exists already in the join result IKTable).

### Left Join

- (IKTable, IKTable) → IKTable

Performs a LEFT JOIN of this table with another table.

Data must be co-partitioned: The input data for both sides must be co-partitioned.

``` csharp
var table1 = builder.Table<string, string>("topic2", InMemory<string, string>.As("table1-store"));
var table2 = builder.Table<string, string>("topic2", InMemory<string, string>.As("table2-store"));

var tableJoin = table1.LeftJoin(table2,(v1, v2) => $"{v1}-{v2}");
```

Detailed behavior:
- The join is key-based, i.e. with the join predicate leftRecord.key == rightRecord.key.
- The join will be triggered under the conditions listed below whenever new input is received. When it is triggered, the user-supplied value joiner will be called to produce join output records.
- Input records with a null key are ignored and do not trigger the join.
- Input records with a null value are interpreted as tombstones for the corresponding key, which indicate the deletion of the key from the table. Tombstones do not trigger the join. When an input tombstone is received, then an output tombstone is forwarded directly to the join result IKTable if required (i.e. only if the corresponding key actually exists already in the join result IKTable).
- For each input record on the left side that does not have any match on the right side, the ValueJoiner will be called with (leftRecord.value, null); this explains the row with timestamp=3 in the table below, which lists [A, null] in the LEFT JOIN column.

### Outer Join

- (IKTable, IKTable) → IKTable

Performs a OUTER JOIN of this table with another table.

Data must be co-partitioned: The input data for both sides must be co-partitioned.

``` csharp
var table1 = builder.Table<string, string>("topic2", InMemory<string, string>.As("table1-store"));
var table2 = builder.Table<string, string>("topic2", InMemory<string, string>.As("table2-store"));

var tableJoin = table1.OuterJoin(table2,(v1, v2) => $"{v1}-{v2}");
```

Detailed behavior:
- The join is key-based, i.e. with the join predicate leftRecord.key == rightRecord.key.
- The join will be triggered under the conditions listed below whenever new input is received. When it is triggered, the user-supplied ValueJoiner will be called to produce join output records.
- Input records with a null key are ignored and do not trigger the join.
- Input records with a null value are interpreted as tombstones for the corresponding key, which indicate the deletion of the key from the table. Tombstones do not trigger the join. When an input tombstone is received, then an output tombstone is forwarded directly to the join result IKTable if required (i.e. only if the corresponding key actually exists already in the join result IKTable).
- For each input record on one side that does not have any match on the other side, the ValueJoiner will be called with (leftRecord.value, null) or (null, rightRecord.value), respectively; this explains the rows with timestamp=3 and timestamp=7 in the table below, which list [A, null] and [null, b], respectively, in the OUTER JOIN column.