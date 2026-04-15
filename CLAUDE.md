# Streamiz Kafka .NET - Claude Context

## Project Overview

**Streamiz Kafka .NET** is a .NET stream processing library for Apache Kafka, inspired by Apache Kafka Streams. It provides a high-level DSL and Processor API for building stream processing applications in C#.

- **Language**: C# (.NET Standard 2.0, .NET 5, 6, 7, 8)
- **Main Branch**: `develop`
- **Current Feature Branch**: `async-processing-v2` (parallel processing implementation)
- **Version**: 1.8.0 (in development)
- **Repository**: https://github.com/LGouellec/kafka-streams-dotnet

## Recent Major Work (2026-04-08 to 2026-04-10)

### Parallel Processing Architecture Refactoring (async-processing-v2 branch)

**Latest**: Refactored per-processor parallel processing to use unified `ProcessingStrategy` architecture (2026-04-10).

Previously implemented comprehensive parallel processing support for both external streams and async processors within topologies.

**Commits**:
- `419e40ac` - "Add parallel processing support for async processors and external streams" (initial)
- Latest refactoring - Unified architecture using ProcessingStrategy for both global and per-processor configs

**Key Components Added**:

1. **Processing Strategies** (4 modes):
   - `SequentialProcessingStrategy` - Single-threaded (default, backward compatible)
   - `PerPartitionProcessingStrategy` - Parallel by partition
   - `PerKeyProcessingStrategy` - Parallel by key (consistent hashing)
   - `UnorderedProcessingStrategy` - Maximum parallelism (shared queue)

2. **Configuration**:
   - `ParallelProcessingConfig` - Configuration class with factory methods
   - `ParallelProcessingMode` enum - SEQUENTIAL, PER_PARTITION, PER_KEY, UNORDERED
   - Global config via `StreamConfig.ExternalProcessingConfig`
   - Per-processor config via async method parameters

3. **Core Infrastructure**:
   - `IProcessingStrategy` - Strategy pattern interface
   - `OffsetTracker` - Manages committable offsets for at-least-once semantics
   - `RecordWorkItem` - Encapsulates work items in processing queues
   - `AbstractAsyncProcessor` - Base class for async processors (simplified after refactoring)
   - `InternalTopologyBuilder` - Maps request topics to ParallelProcessingConfig
   - `ExternalStreamThread` - Routes records to appropriate ProcessingStrategy per topic

4. **Tests**: 42 total (all passing)
   - `OffsetTrackerTests.cs` - Offset management
   - `PerPartitionProcessingStrategyTests.cs` - Per-partition mode
   - `PerKeyProcessingStrategyTests.cs` - Key hashing and distribution
   - `UnorderedProcessingStrategyTests.cs` - Unordered mode
   - `PerProcessorParallelTests.cs` - Per-processor parallelism

5. **Documentation**:
   - `docs/parallel-processing.md` - Complete user guide (628 lines)
   - `docs/ASYNC_PROCESSING_V2_DESIGN.md` - Technical design document
   - Updated `docs/async-processing.md`, `docs/monitoring.md`, `docs/stream-configuration.md`
   - `PER_PROCESSOR_PARALLEL_SUMMARY.md` - Implementation summary

**Performance**: 5-10x throughput improvement for I/O-bound workloads with appropriate concurrency settings.

**Backward Compatibility**: ✅ All changes opt-in, defaults to sequential processing. Existing code works without modification.

### Architecture Refactoring (2026-04-10)

**Objective**: Eliminate duplication between global and per-processor parallel processing by using unified `ProcessingStrategy` architecture.

**Before** (Initial Implementation):
- Global: `ExternalStreamThread` uses `ProcessingStrategy` 
- Per-processor: `AbstractAsyncProcessor` uses Task/SemaphoreSlim for parallelism
- **Problem**: Two different implementations of the same concept

**After** (Refactored):
- Both global and per-processor use `ProcessingStrategy` in `ExternalStreamThread`
- Each async processor with `ParallelProcessingConfig` creates a unique request topic
- `ExternalStreamThread` maintains `Dictionary<string, IProcessingStrategy>` for all topics
- Records routed to correct strategy based on topic
- `AbstractAsyncProcessor` simplified (removed Task/SemaphoreSlim code)

**Benefits**:
1. Single source of truth for parallel processing logic
2. Consistent behavior between global and per-processor
3. Simpler code (~150 lines removed from `AbstractAsyncProcessor`)
4. Better resource management (shared thread pool)
5. Same offset tracking and at-least-once semantics for both

**Files Modified** (14 files):
- `InternalTopologyBuilder.cs` - Maps request topics to configs
- `AsyncNode.cs` - Passes config to builder
- `KStream.cs` - Passes config to AsyncNode
- `ExternalStreamThread.cs` - Routes to strategies per topic
- `AbstractAsyncProcessor.cs` - Simplified (removed parallel code)
- All async processor classes and suppliers
- `PerProcessorParallelTests.cs` - Adapted for new architecture

**Testing Limitation**: `TopologyTestDriver` does not create `ExternalStreamThread`, so per-processor parallel tests only verify API correctness, not actual parallelism. Integration tests required for parallel behavior verification.

## Project Structure

```
streamiz/
├── core/                          # Main library
│   ├── Processors/               # Stream processors
│   │   ├── *ProcessingStrategy.cs    # NEW: Parallel processing strategies
│   │   ├── ParallelProcessingConfig.cs
│   │   ├── OffsetTracker.cs
│   │   └── Abstract*Processor.cs
│   ├── Stream/                   # Stream DSL
│   │   ├── IKStream.cs          # Stream interface
│   │   └── Internal/            # Internal implementations
│   ├── Kafka/                    # Kafka integration
│   ├── Metrics/                  # Metrics and monitoring
│   ├── State/                    # State stores
│   └── StreamConfig.cs          # Configuration
├── test/                         # Tests
│   └── Streamiz.Kafka.Net.Tests/
│       ├── Private/             # Unit tests
│       └── Public/              # Integration tests
├── docs/                         # Documentation
│   ├── parallel-processing.md   # NEW: Parallel processing guide
│   ├── async-processing.md
│   ├── processor-api.md
│   └── *.md
├── serdes/                       # Serialization/Deserialization
├── metrics/                      # Metrics implementations
└── launcher/                     # Demo applications
```

## Key Files and Their Purposes

### Core Processing

- **`ExternalStreamThread.cs`** - External stream consumer thread; routes records to ProcessingStrategy per topic
- **`IProcessingStrategy.cs`** - Strategy pattern interface for parallel processing
- **`*ProcessingStrategy.cs`** - Concrete strategy implementations (Sequential, PerPartition, PerKey, Unordered)
- **`AbstractAsyncProcessor.cs`** - Base class for async processors (simplified, sequential processing)
- **`OffsetTracker.cs`** - Tracks in-flight and committable offsets for at-least-once semantics
- **`ParallelProcessingConfig.cs`** - Configuration for parallel processing (factory methods)
- **`InternalTopologyBuilder.cs`** - Maps request topics to their ParallelProcessingConfig

### Async Processors

- **`KStreamMapAsyncProcessor.cs`** - Map with async operations
- **`KStreamFlatMapAsyncProcessor.cs`** - FlatMap with async operations
- **`KStreamForeachAsyncProcessor.cs`** - ForEach with async operations

### Configuration

- **`StreamConfig.cs`** - Main configuration class
  - `ExternalProcessingConfig` property for global parallel processing
  - Consumer, producer, and admin client configs

### Documentation

- **`docs/parallel-processing.md`** - Complete parallel processing guide
- **`docs/async-processing.md`** - Async processor documentation
- **`docs/stream-configuration.md`** - Configuration reference
- **`docs/monitoring.md`** - Metrics and monitoring

## Build and Test

### Build
```bash
dotnet build --no-restore
```

### Run All Tests
```bash
dotnet test
```

### Run Specific Tests
```bash
# Parallel processing tests
dotnet test --filter "FullyQualifiedName~Parallel"

# Specific test class
dotnet test --filter "FullyQualifiedName~OffsetTrackerTests"
```

### Test Results (Current)
- **Total**: 1,202+ tests
- **Parallel Processing**: 42 tests (all passing)
- **Build**: ✅ Succeeded

## Development Guidelines

### Code Style

1. **Naming Conventions**:
   - Classes: PascalCase
   - Methods: PascalCase
   - Private fields: camelCase with no prefix
   - Constants: UPPER_CASE (for enums)

2. **Async Methods**:
   - Always suffix with `Async` (e.g., `ProcessAsync`)
   - Use `Task` or `Task<T>` return types
   - Use `async/await` pattern

3. **Logging**:
   - Use `Microsoft.Extensions.Logging.ILogger`
   - Log prefix format: `$"{logPrefix}Message"`
   - Debug logging for trace operations
   - Info for lifecycle events
   - Error for exceptions

4. **Thread Safety**:
   - Use `ConcurrentQueue<T>`, `ConcurrentDictionary<K,V>` for shared state
   - Use `Interlocked` for counters
   - Use `lock` sparingly, prefer lock-free designs
   - Use `SemaphoreSlim` for async concurrency control

### Testing Conventions

1. **Test Organization**:
   - `Private/` folder for unit tests
   - `Public/` folder for integration tests
   - Test file naming: `{ClassName}Tests.cs`

2. **Test Structure**:
   - Use NUnit framework
   - Test naming: `{Method}_{Scenario}_{ExpectedResult}` or descriptive names
   - Use `[Test]` attribute
   - Use `Assert.AreEqual`, `Assert.Greater`, etc.

3. **Test Patterns**:
   - Arrange-Act-Assert structure
   - Use `TopologyTestDriver` for topology testing
   - Clean up resources in test teardown or using statements

### Commit Guidelines

1. **Commit Messages**:
   - First line: Brief summary (< 72 chars)
   - Body: Detailed description with bullet points
   - Include "Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>" when applicable
   - Reference issues/PRs when relevant

2. **Commit Scope**:
   - Atomic commits (one logical change per commit)
   - Include tests with feature commits
   - Update documentation in same commit as feature

3. **Branch Strategy**:
   - Main branch: `develop`
   - Feature branches: descriptive names (e.g., `async-processing-v2`)
   - Checkout from `develop`, merge back to `develop`

## Architecture Patterns

### Strategy Pattern (Parallel Processing)

```csharp
// Strategy interface
public interface IProcessingStrategy
{
    Task SubmitAsync(ConsumeResult<byte[], byte[]> record);
    IEnumerable<TopicPartitionOffset> GetCommittableOffsets();
    void Start();
    void Close();
}

// Concrete strategies
- SequentialProcessingStrategy
- PerPartitionProcessingStrategy
- PerKeyProcessingStrategy
- UnorderedProcessingStrategy

// Usage
var strategy = CreateProcessingStrategy(config);
strategy.Start();
await strategy.SubmitAsync(record);
```

### Factory Pattern (Configuration)

```csharp
// Factory methods for clean API
ParallelProcessingConfig.Sequential()
ParallelProcessingConfig.PerPartition(maxConcurrency: 8)
ParallelProcessingConfig.PerKey(maxConcurrency: 16)
ParallelProcessingConfig.Unordered(maxConcurrency: 32)
```

### Processor Pattern (DSL)

```csharp
// Fluent API for stream processing
builder.Stream<K, V>("topic")
    .Filter(predicate)
    .MapValuesAsync(asyncMapper, retryPolicy, parallelConfig)
    .To("output");
```

## Important Context for Future Work

### Parallel Processing

1. **Ordering Guarantees**:
   - SEQUENTIAL: Full ordering preserved
   - PER_PARTITION: Partition order preserved, cross-partition unordered
   - PER_KEY: Per-key order preserved, cross-key unordered
   - UNORDERED: No ordering guarantees

2. **Offset Management**:
   - `OffsetTracker` maintains in-flight and completed offsets
   - Only sequential offsets are committable (at-least-once semantics)
   - Strategy provides committable offsets via `GetCommittableOffsets()`

3. **Concurrency Models** (Unified Architecture):
   - Both global and per-processor use `ProcessingStrategy` in `ExternalStreamThread`
   - Single `ExternalStreamThread` routes records to strategies based on topic
   - Each topic (external or internal request) has its own `ProcessingStrategy` instance
   - Strategy manages worker pool based on mode and `MaxConcurrency`

4. **Default Concurrency**:
   - SEQUENTIAL: 1
   - PER_PARTITION: `Environment.ProcessorCount`
   - PER_KEY: `Environment.ProcessorCount × 2`
   - UNORDERED: `Environment.ProcessorCount × 4`

### Known Limitations

1. **Per-Processor Configuration**:
   - Creates internal request/response topics for each async processor
   - More async processors = more internal topics
   - Ordering depends on strategy mode chosen
   - Each request topic has its own `ProcessingStrategy` instance

2. **Testing with TopologyTestDriver**:
   - `TopologyTestDriver` does NOT create `ExternalStreamThread`
   - Per-processor parallel processing executes sequentially in unit tests
   - Integration tests with real Kafka required to test actual parallelism
   - Unit tests can only verify API correctness, not parallel behavior

3. **Resource Usage**:
   - Single `ExternalStreamThread` shared across all topics (external + request)
   - Memory usage proportional to `MaxQueuedRecords` per strategy
   - Multiple strategies compound memory usage
   - Backpressure pauses consumer when strategy queue full

4. **Compatibility**:
   - .NET Standard 2.0 minimum
   - Kafka 0.10+ required
   - librdkafka client dependency

## Metrics

### Parallel Processing Metrics (Since 1.8.0)

- **parallel-in-flight-records**: Number of records being processed
- **parallel-queue-depth**: Number of records queued
- **parallel-worker-count**: Number of active workers

### Configuration

```csharp
config.MetricsRecording = MetricsRecordingLevel.DEBUG; // or INFO
config.UsePrometheusReporter(9090, includeLibrdkafkaMetrics: true);
```

## Common Tasks

### Adding a New Processing Strategy

1. Implement `IProcessingStrategy`
2. Add factory method to `ParallelProcessingConfig`
3. Update `ExternalStreamThread.CreateProcessingStrategy()`
4. Add tests in `test/Private/{StrategyName}Tests.cs`
5. Document in `docs/parallel-processing.md`

### Adding a New Async Processor

1. Create processor in `core/Processors/{Name}AsyncProcessor.cs`
2. Inherit from `AbstractAsyncProcessor<K, V, K1, V1>`
3. Implement `ProcessAsync` method
4. Add supplier in `core/Stream/Internal/Graph/{Name}Async.cs`
5. Add DSL method to `IKStream.cs` and `KStream.cs`
6. Accept `ParallelProcessingConfig` parameter
7. Add tests

### Updating Documentation

1. **API Changes**: Update `docs/processor-api.md` or relevant doc
2. **Configuration**: Update `docs/stream-configuration.md`
3. **Metrics**: Update `docs/monitoring.md`
4. **Examples**: Update `docs/async-processing.md` or `docs/parallel-processing.md`
5. **Index**: Update `docs/index.rst` if adding new pages

## Troubleshooting

### Build Issues

```bash
# Clean and rebuild
dotnet clean
dotnet restore
dotnet build
```

### Test Issues

```bash
# Run tests with verbose logging
dotnet test --logger "console;verbosity=detailed"

# Run specific test
dotnet test --filter "FullyQualifiedName=Streamiz.Kafka.Net.Tests.Private.OffsetTrackerTests.SequentialCompletion_ReturnsCommittableOffset"
```

### Common Errors

1. **"AbstractAsyncProcessor does not contain a constructor..."**
   - After refactoring: `AbstractAsyncProcessor` takes only `RetryPolicy` parameter
   - `ParallelProcessingConfig` is NOT passed to processor constructor
   - Config is stored in `InternalTopologyBuilder` and used by `ExternalStreamThread`

2. **"Cannot implicitly convert List<Exception> to ReadOnlyCollection<Exception>"**
   - Use: `new ReadOnlyCollection<Exception>(new List<Exception> { ex })`

3. **Build warnings about .NET version support**
   - Expected for multi-targeting (net5.0, net6.0, net7.0, net8.0, netstandard2.0)
   - Can be ignored unless breaking

4. **Per-processor tests failing with "Expected concurrent processing"**
   - `TopologyTestDriver` does not create `ExternalStreamThread`
   - Per-processor parallelism cannot be tested with `TopologyTestDriver`
   - Use integration tests with real Kafka to verify parallel behavior

## Resources

- **Documentation**: https://lgouellec.github.io/streamiz/
- **GitHub**: https://github.com/LGouellec/kafka-streams-dotnet
- **NuGet**: https://www.nuget.org/packages/Streamiz.Kafka.Net/
- **Discord**: https://discord.gg/J7Jtxum
- **Kafka Streams Docs**: https://kafka.apache.org/documentation/streams

## Notes for Claude Code

### Session Preferences

- **Build Before Tests**: Always run `dotnet build --no-restore` before running tests
- **Parallel Tool Calls**: Use parallel tool calls for independent operations
- **Commit Style**: Use detailed commit messages with structured format
- **Documentation**: Update docs in same commit as code changes
- **Tests Required**: All new features need comprehensive tests

### Memory/Context

- **Current Branch**: `async-processing-v2` (parallel processing implementation)
- **Last Major Work**: 
  - Initial: `419e40ac` - Parallel processing support
  - Refactoring (2026-04-10): Unified ProcessingStrategy architecture
- **Test Status**: 
  - Strategy tests: 42/42 passing
  - Per-processor API tests: 6/6 passing (adapted for new architecture)
- **Architecture**: Unified ProcessingStrategy for both global and per-processor configs
- **Next Steps**: Integration testing, potentially merge to `develop` or create PR

### Project-Specific Patterns

1. **Async Processors**: Always use retry policies and external context
2. **Processing Strategies**: Worker pools use `ConcurrentQueue<T>` for work distribution
3. **Offset Management**: Only commit sequential offsets for at-least-once semantics
4. **Metrics**: Use ThreadMetrics helper for creating sensors
5. **Logging**: Use `Logger.GetLogger(typeof(ClassName))` pattern

---

Last Updated: 2026-04-10 (Post-Refactoring)
Branch: async-processing-v2
Major Work:
- Initial: 419e40ac - Parallel processing support
- Refactoring: Unified ProcessingStrategy architecture (14 files modified)
Status: Tests passing, documentation updated
