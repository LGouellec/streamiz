# Statefull processors

Stateful transformations depend on state for processing inputs and producing outputs and require a state store associated with the stream processor. For example, in aggregating operations, a windowing state store is used to collect the latest aggregation results per window. In join operations, a windowing state store is used to collect all of the records received so far within the defined window boundary.

IMPLEMENTATION WORK IN PROGRESS

**To follow the progress, you can star [the Github project](https://github.com/LGouellec/kafka-streams-dotnet) and watch it !** 

|Operator Name|Method|TODO|IMPLEMENTED|TESTED|DOCUMENTED|
|---|---|---|---|---|---|
|Aggregate|KGroupedStream -> KTable|   |   |   |&#9745;|
|Aggregate|KGroupedTable -> KTable|&#9745;|   |   |   |
|Aggregate(windowed)|KGroupedStream -> KTable|&#9745;|   |   |   |
|Count|KGroupedStream -> KTable|   |   |   |&#9745;|
|Count|KGroupedTable -> KTable|&#9745;|   |   |   |
|Count(windowed)|KGroupedStream → KStream|&#9745;|   |   |   |
|Reduce|KGroupedStream → KTable|&#9745;|   |   |   |
|Reduce|KGroupedTable → KTable|&#9745;|   |   |   |
|Reduce(windowed)|KGroupedStream → KTable|&#9745;|   |   |   |
|InnerJoin(windowed)|(KStream,KStream) → KStream|&#9745;|   |   |   |
|LeftJoin(windowed)|(KStream,KStream) → KStream|&#9745;|   |   |   |
|OuterJoin(windowed)|(KStream,KStream) → KStream|&#9745;|   |   |   |
|InnerJoin(windowed)|(KTable,KTable) → KTable|&#9745;|   |   |   |
|LeftJoin(windowed)|(KTable,KTable) → KTable|&#9745;|   |   |   |
|OuterJoin(windowed)|(KTable,KTable) → KTable|&#9745;|   |   |   |
|InnerJoin(windowed)|(KStream,KTable) → KStream|&#9745;|   |   |   |
|LeftJoin(windowed)|(KStream,KTable) → KStream|&#9745;|   |   |   |
|InnerJoin(windowed)|(KStream,GlobalKTable) → KStream|&#9745;|   |   |   |
|LeftJoin(windowed)|(KStream,GlobalKTable) → KStream|&#9745;|   |   |   |

## Count

**Rolling aggregation.** Counts the number of records by the grouped key. (see IKGroupedStream for details)

Several variants of count exist.

- IKGroupedStream  → IKTable
- IKGroupedTable  -> IKTable (soon)

``` csharp
var groupedStream = builder
                        .Stream<string, string>("test")
                        .GroupBy((k, v) => k.ToUpper());

// Counting a IKGroupedStream
var table = groupedStream.Count(InMemory<string, long>.As("count-store"));
```

Detailed behavior for IKGroupedStream:
- Input records with null keys or values are ignored.

## Aggregate

**Rolling aggregation.** Aggregates the values of (non-windowed) records by the grouped key. Aggregating is a generalization of reduce and allows, for example, the aggregate value to have a different type than the input values. (see IKGroupedStream for details)

When aggregating a grouped stream, you must provide an initializer (e.g., aggValue = 0) and an “adder” aggregator (e.g., aggValue + curValue).

Several variants of aggregate exist.

- KGroupedStream → KTable

``` csharp
var groupedStream = builder
                        .Stream<string, string>("test")
                        .GroupBy((k, v) => k.ToUpper());

// Counting a IKGroupedStream
var table = groupedStream.Aggregate(() => 0L, (k,v,agg) => agg+ 1, InMemory<string, long>.As("agg-store").WithValueSerdes<Int64SerDes>());
```