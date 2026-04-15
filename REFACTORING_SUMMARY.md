# Per-Processor Parallel Processing Refactoring Summary

## Date
2026-04-10

## Objective
Refactor the parallel processing architecture to apply `ParallelProcessingConfig` at the **ProcessingStrategy level** (per request topic) instead of at the **Processor level** (Task-based parallelism).

## Rationale
The initial implementation used Task/SemaphoreSlim within `AbstractAsyncProcessor` to achieve per-processor parallelism. However, this created a **duplication** with the existing `ProcessingStrategy` infrastructure which already handles parallel processing at the ExternalStreamThread level.

The correct architecture is:
- Each async processor with `ParallelProcessingConfig` → creates a unique **request topic**
- `ExternalStreamThread` consumes **all** request topics
- Each request topic → gets its own **ProcessingStrategy** based on its config
- `ProcessingStrategy` handles the parallelism (PerPartition, PerKey, Unordered, Sequential)
- `AsyncProcessor` remains **sequential** (no Task/SemaphoreSlim)

## Architecture

### Before (Wrong)
```
Input Record
  ↓
ExternalStreamThread (single ProcessingStrategy for all topics)
  ↓
AsyncProcessor (Task-based parallelism with SemaphoreSlim)
```

### After (Correct)
```
Input Record
  ↓
ExternalStreamThread.Run()
  ↓
Route to strategiesByTopic[record.Topic]
  ↓
ProcessingStrategy (PerPartition/PerKey/Unordered/Sequential)
  ↓
AsyncProcessor (sequential processing)
```

## Changes Made

### 1. InternalTopologyBuilder
**File**: `core/Processors/Internal/InternalTopologyBuilder.cs`

**Added**:
- `Dictionary<string, ParallelProcessingConfig> requestTopicParallelConfigs` - Maps request topics to their configs
- `SetParallelConfigForRequestTopic(string, ParallelProcessingConfig)` - Stores config for a topic
- `GetParallelConfigForRequestTopic(string)` - Retrieves config for a topic

### 2. AsyncNode
**File**: `core/Stream/Internal/Graph/Nodes/AsyncNode.cs`

**Added**:
- `using Streamiz.Kafka.Net.Processors;` - Import for ParallelProcessingConfig
- `ParallelProcessingConfig` parameter to both constructors
- Property `ParallelConfig` in inner classes `AsyncNodeRequest` and `AsyncNodeRequestVoid`
- Call to `builder.SetParallelConfigForRequestTopic()` in `WriteToTopology()`

### 3. KStream
**File**: `core/Stream/Internal/KStream.cs`

**Modified**:
- `AsyncProcess<K1, V1>()` - Added `ParallelProcessingConfig` parameter
- All async methods (MapAsync, MapValuesAsync, FlatMapAsync, FlatMapValuesAsync, ForeachAsync) now pass `parallelProcessingConfig` to `AsyncProcess()` and `AsyncNode` constructor

### 4. ExternalStreamThread
**File**: `core/Processors/ExternalStreamThread.cs`

**Major Refactoring**:

**Replaced**:
- `IProcessingStrategy processingStrategy` → `Dictionary<string, IProcessingStrategy> strategiesByTopic`

**Modified `Start()`**:
```csharp
// Create processing strategies per topic
strategiesByTopic = new Dictionary<string, IProcessingStrategy>();
foreach (var requestTopic in externalSourceTopics)
{
    var parallelConfig = internalTopologyBuilder.GetParallelConfigForRequestTopic(requestTopic);
    var strategy = CreateProcessingStrategy(requestTopic, parallelConfig);
    strategiesByTopic[requestTopic] = strategy;
    
    // Start the strategy...
}
```

**Modified `Run()` loop**:
```csharp
if (result != null)
{
    // Route to the correct strategy based on topic
    if (strategiesByTopic.TryGetValue(result.Topic, out var strategy))
    {
        strategy.SubmitAsync(result).GetAwaiter().GetResult();
        
        // Per-topic backpressure management
        if (isAtCapacity)
        {
            consumer.Pause(topic-specific partitions);
        }
    }
}
```

**Modified `CreateProcessingStrategy()`**:
```csharp
private IProcessingStrategy CreateProcessingStrategy(string topic, ParallelProcessingConfig perProcessorConfig)
{
    // Use per-processor config if provided, otherwise fall back to global config
    var config = perProcessorConfig ?? configuration.ExternalProcessingConfig;
    
    // Log config source
    var configSource = perProcessorConfig != null ? "per-processor" : "global";
    
    switch (config?.Mode ?? ParallelProcessingMode.SEQUENTIAL)
    {
        case ParallelProcessingMode.SEQUENTIAL:
            log.LogInformation($"{logPrefix}Topic [{topic}]: Using SEQUENTIAL processing strategy ({configSource} config)");
            // ...
    }
}
```

**Modified `CommitOffsets()`**:
```csharp
// Flush all processing strategies
foreach (var strategy in strategiesByTopic.Values)
{
    strategy.Flush();
}

// Get committable offsets from all strategies
var offsets = strategiesByTopic.Values
    .SelectMany(s => s.GetCommittableOffsets())
    .ToList();
```

**Modified `CompleteShutdown()` and `HandleInnerException()`**:
- Close/dispose all strategies in the dictionary
- Recreate all strategies on exception handling

**Modified `RecordParallelProcessingMetrics()`**:
- Aggregate metrics from all strategies
- Sum up in-flight records, queue depth, and workers across all topics

### 5. AbstractAsyncProcessor
**File**: `core/Processors/AbstractAsyncProcessor.cs`

**Removed** (Complete cleanup):
- `ParallelProcessingConfig parallelProcessingConfig` field
- `bool useParallelProcessing` field
- `SemaphoreSlim concurrencySemaphore` field
- `ConcurrentQueue<Task> activeTasks` field
- `CancellationTokenSource closeCts` field
- `ProcessWithParallelism()` method
- `ProcessAsyncWithRetry()` method
- `CleanupCompletedTasks()` method
- Parallel processing logic in `Init()` and `Close()`

**Simplified constructor**:
```csharp
protected AbstractAsyncProcessor(RetryPolicy policy)
{
    Policy = policy;
}
```

**Simplified `Process()`**:
```csharp
public override void Process(K key, V value)
{
    // Direct call to ProcessSequential (renamed from previous implementation)
    // No more branching between parallel/sequential
}
```

### 6. Async Processor Classes
**Files**:
- `core/Processors/KStreamMapAsyncProcessor.cs`
- `core/Processors/KStreamFlatMapAsyncProcessor.cs`
- `core/Processors/KStreamForeachAsyncProcessor.cs`

**Removed**: `ParallelProcessingConfig` parameter from constructors
**Updated**: Base constructor calls to pass only `RetryPolicy`

### 7. Processor Supplier Classes
**Files**:
- `core/Stream/Internal/Graph/KStreamMapAsync.cs`
- `core/Stream/Internal/Graph/KStreamMapValuesAsync.cs`
- `core/Stream/Internal/Graph/KStreamFlatMapAsync.cs`
- `core/Stream/Internal/Graph/KStreamFlatMapValuesAsync.cs`
- `core/Stream/Internal/Graph/KStreamForeachAsync.cs`

**Removed**: 
- `ParallelProcessingConfig parallelProcessingConfig` field
- `ParallelProcessingConfig` parameter from constructors
- Passing `parallelProcessingConfig` to processor constructors

### 8. Sample Fix
**File**: `launcher/sample-stream/Program.cs`

**Fixed**: Commented out incomplete `.MapValuesAsync()` call that was causing build error

## API Surface (Unchanged)

The public API remains **exactly the same**:

```csharp
builder.Stream<string, Order>("orders")
    .MapValuesAsync(
        async (r, c) => await db.GetCustomer(r.Value.CustomerId),
        parallelProcessingConfig: ParallelProcessingConfig.Unordered(16))
    .MapValuesAsync(
        async (r, c) => await api.ValidateAddress(r.Value.Address),
        parallelProcessingConfig: ParallelProcessingConfig.Unordered(4))
    .To("validated-orders");
```

Users still pass `ParallelProcessingConfig` to async methods. The difference is **internal**:
- Before: Config was used in `AbstractAsyncProcessor` for Task-based parallelism
- After: Config is stored per-topic and used to create a `ProcessingStrategy`

## Benefits of New Architecture

1. **No Duplication**: Uses existing `ProcessingStrategy` infrastructure instead of reimplementing parallelism
2. **Consistency**: All parallel processing (global and per-processor) uses the same strategies
3. **Better Resource Management**: Strategies manage thread pools, not individual processors
4. **Per-Topic Backpressure**: Can pause/resume specific topics based on their strategy capacity
5. **Metrics Aggregation**: Can aggregate metrics across all topics/strategies
6. **Ordering Support**: Could potentially support PerPartition/PerKey modes per processor (future work)

## Current Limitations

Per-processor parallel processing currently only supports **bounded concurrency** (Unordered mode):
- `ParallelProcessingConfig.Unordered(maxConcurrency: N)`

Sequential mode is the default:
- `parallelProcessingConfig: null` or `ParallelProcessingConfig.Sequential()`

PerPartition and PerKey modes are technically possible but not documented/tested for per-processor use.

## Files Modified

### Core (11 files)
1. `core/Processors/Internal/InternalTopologyBuilder.cs`
2. `core/Stream/Internal/Graph/Nodes/AsyncNode.cs`
3. `core/Stream/Internal/KStream.cs`
4. `core/Processors/ExternalStreamThread.cs`
5. `core/Processors/AbstractAsyncProcessor.cs`
6. `core/Processors/KStreamMapAsyncProcessor.cs`
7. `core/Processors/KStreamFlatMapAsyncProcessor.cs`
8. `core/Processors/KStreamForeachAsyncProcessor.cs`
9. `core/Stream/Internal/Graph/KStreamMapAsync.cs`
10. `core/Stream/Internal/Graph/KStreamMapValuesAsync.cs`
11. `core/Stream/Internal/Graph/KStreamFlatMapAsync.cs`
12. `core/Stream/Internal/Graph/KStreamFlatMapValuesAsync.cs`
13. `core/Stream/Internal/Graph/KStreamForeachAsync.cs`

### Launcher (1 file)
1. `launcher/sample-stream/Program.cs` - Fixed incomplete code

## Next Steps

1. ✅ Core refactoring complete
2. ⏳ Update tests (PerProcessorParallelTests needs adaptation)
3. ⏳ Update documentation:
   - `docs/parallel-processing.md` - Explain per-processor uses ProcessingStrategy
   - `docs/async-processing.md` - Update architecture explanation
4. ⏳ Test with real Kafka setup
5. ⏳ Update CLAUDE.md with new architecture

## Testing Status

**Build**: ✅ Compiles (after fixing sample)
**Tests**: ⏳ Need to be updated for new architecture

The existing tests in `PerProcessorParallelTests.cs` were designed for the Task-based approach. They need to be adapted or rewritten to test the new strategy-based architecture.

## Breaking Changes

**None** - The API is identical, only the internal implementation changed.

## Performance Implications

**Expected**: Similar or better performance
- Strategies use optimized thread pools
- Less overhead from Task/SemaphoreSlim management in processor
- Better resource sharing across processors with similar configs

**To Verify**: Run benchmarks comparing old vs new implementation

## Code Quality

**Reduced Complexity**:
- Removed ~150 lines of parallel processing code from `AbstractAsyncProcessor`
- Centralized parallel processing logic in strategies
- Clearer separation of concerns

**Improved Maintainability**:
- Single source of truth for parallel processing behavior
- Easier to add new processing modes
- Consistent behavior between global and per-processor configs
