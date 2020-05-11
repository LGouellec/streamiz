# Statefull processors


IMPLEMENTATION WORK IN PROGRESS

**To follow the progress, you can star [the Github project](https://github.com/LGouellec/kafka-streams-dotnet) and watch it !** 

|Operator Name|Method|TODO|IMPLEMENTED|TESTED|DOCUMENTED|
|---|---|---|---|---|---|
|Aggregate|KGroupedStream -> KTable|&#9745;|   |   |   |
|Aggregate|KGroupedTable -> KTable|&#9745;|   |   |   |
|Aggregate(windowed)|KGroupedStream -> KTable|&#9745;|   |   |   |
|Count|KGroupedStream -> KTable|&#9745;|   |   |   |
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