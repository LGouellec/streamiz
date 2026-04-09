# Async Processing V2 - Parallel Consumer Design

## 1. Overview and Goals

### Current State
- **Sequential Processing**: ExternalStreamThread processes one record at a time
- **Limited Parallelism**: No concurrent processing within a single consumer
- **Simple Retry**: Buffering failed records, reprocessing them later
- **Pause/Resume**: Basic backpressure using consumer pause/resume

### Goals
- **Configurable Parallelism**: Multiple concurrent workers processing records
- **Ordering Guarantees**: Three strategies (per-partition, per-key, unordered)
- **Better Throughput**: Leverage multi-core systems for I/O-bound external processing
- **Flexible Configuration**: Easy to tune based on workload characteristics
- **Backward Compatible**: Existing code continues to work with minimal changes

## 2. Proposed Architecture

### 2.1 Three Processing Strategies

#### Strategy 1: PER_PARTITION
- **Ordering**: Maintains partition order
- **Parallelism**: Different partitions processed concurrently
- **Use Case**: When partition order matters but cross-partition order doesn't
- **Workers**: One queue per partition, workers pick from any partition queue

#### Strategy 2: PER_KEY
- **Ordering**: Maintains per-key order
- **Parallelism**: Different keys processed concurrently
- **Use Case**: When key order matters (e.g., user sessions, entity updates)
- **Workers**: Key-based work distribution, same key always goes to same worker

#### Strategy 3: UNORDERED
- **Ordering**: No guarantees
- **Parallelism**: Maximum - any worker can process any record
- **Use Case**: When order doesn't matter, maximum throughput needed
- **Workers**: Shared queue, all workers compete

### 2.2 Architecture Layers

```
┌─────────────────────────────────────────────────────────────┐
│              ExternalStreamThread (Main Loop)                │
│  - Poll records from Kafka                                   │
│  - Dispatch to ParallelProcessingCoordinator                 │
│  - Manage offset commits                                     │
└──────────────────────────┬──────────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────────┐
│         ParallelProcessingCoordinator                        │
│  - Implements IProcessingStrategy                            │
│  - Routes records based on strategy                          │
│  - Manages worker pool                                       │
│  - Tracks in-flight records                                  │
│  - Determines committable offsets                            │
└──────────────────────────┬──────────────────────────────────┘
                           │
        ┌──────────────────┼──────────────────┐
        │                  │                  │
┌───────▼──────┐  ┌────────▼──────┐  ┌───────▼──────┐
│ Per-Partition│  │   Per-Key     │  │  Unordered   │
│  Strategy    │  │   Strategy    │  │   Strategy   │
└───────┬──────┘  └────────┬──────┘  └───────┬──────┘
        │                  │                  │
        └──────────────────┼──────────────────┘
                           │
        ┌──────────────────┴──────────────────┐
        │                                     │
┌───────▼──────┐                     ┌────────▼──────┐
│   Worker 1   │  ...                │   Worker N    │
│ (Task-based) │                     │ (Task-based)  │
└───────┬──────┘                     └────────┬──────┘
        │                                     │
        └──────────────────┬──────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────────┐
│       ExternalProcessorTopologyExecutor                      │
│  - Process record through topology                           │
│  - Handle retries                                            │
│  - Track completion                                          │
└─────────────────────────────────────────────────────────────┘
```

## 3. Core Components

### 3.1 New Components to Create

#### 3.1.1 `ParallelProcessingMode` (Enum)
```csharp
public enum ParallelProcessingMode
{
    SEQUENTIAL,      // Current behavior (default for backward compatibility)
    PER_PARTITION,   // Parallel by partition
    PER_KEY,         // Parallel by key
    UNORDERED        // Maximum parallelism
}
```

#### 3.1.2 `ParallelProcessingConfig`
```csharp
public class ParallelProcessingConfig
{
    public ParallelProcessingMode Mode { get; set; } = ParallelProcessingMode.SEQUENTIAL;
    public int MaxConcurrency { get; set; } = Environment.ProcessorCount;
    public int MaxQueuedRecords { get; set; } = 10000;
    public TimeSpan MaxWaitForCompletion { get; set; } = TimeSpan.FromSeconds(30);
}
```

#### 3.1.3 `IProcessingStrategy` (Interface)
```csharp
internal interface IProcessingStrategy
{
    Task SubmitAsync(ConsumeResult<byte[], byte[]> record);
    IEnumerable<TopicPartitionOffset> GetCommittableOffsets();
    void Flush();
    void Close();
    int InFlightCount { get; }
}
```

#### 3.1.4 `ParallelProcessingCoordinator`
- Factory to create appropriate strategy
- Manages lifecycle
- Provides metrics

#### 3.1.5 Strategy Implementations
- `SequentialProcessingStrategy` (current behavior)
- `PerPartitionProcessingStrategy`
- `PerKeyProcessingStrategy`
- `UnorderedProcessingStrategy`

#### 3.1.6 `OffsetTracker`
```csharp
internal class OffsetTracker
{
    // Track in-flight offsets
    void RecordDispatched(TopicPartitionOffset tpo);
    
    // Mark offset as completed
    void RecordCompleted(TopicPartitionOffset tpo);
    
    // Get highest sequential completed offset per partition
    IEnumerable<TopicPartitionOffset> GetCommittableOffsets();
}
```

#### 3.1.7 `RecordWorkItem`
```csharp
internal class RecordWorkItem
{
    public ConsumeResult<byte[], byte[]> Record { get; set; }
    public TaskCompletionSource<ProcessingResult> CompletionSource { get; set; }
    public DateTime DispatchedAt { get; set; }
    public int RetryCount { get; set; }
}
```

### 3.2 Components to Modify

#### 3.2.1 `ExternalStreamThread`
- Remove direct processing logic
- Delegate to `ParallelProcessingCoordinator`
- Simplify to: poll → dispatch → commit cycle
- Track backpressure from coordinator

#### 3.2.2 `ExternalProcessorTopologyExecutor`
- Make async-friendly (return Task)
- Remove internal buffering (handled by coordinator)
- Focus on processing logic only
- Report completion status

#### 3.2.3 `IStreamConfig`
- Add `ParallelProcessingConfig` property
- Default to SEQUENTIAL mode

## 4. Implementation Phases

### Phase 1: Foundation (Week 1)
**Goal**: Set up infrastructure without changing behavior

- [ ] Create `ParallelProcessingMode` enum
- [ ] Create `ParallelProcessingConfig` class
- [ ] Add configuration to `IStreamConfig`
- [ ] Create `IProcessingStrategy` interface
- [ ] Create `OffsetTracker` class with tests
- [ ] Create `RecordWorkItem` class
- [ ] Create `SequentialProcessingStrategy` (wraps current behavior)

**Deliverable**: Configuration in place, no behavior change

### Phase 2: Per-Partition Strategy (Week 2)
**Goal**: First parallel strategy working end-to-end

- [ ] Create `PerPartitionProcessingStrategy`
  - [ ] Partition-based queue management
  - [ ] Worker pool (Task-based)
  - [ ] Work distribution logic
- [ ] Modify `ExternalStreamThread` to use strategy pattern
- [ ] Modify `ExternalProcessorTopologyExecutor` to be async
- [ ] Add metrics for parallelism
- [ ] Integration tests
- [ ] Performance benchmarks

**Deliverable**: Per-partition parallelism working

### Phase 3: Per-Key Strategy (Week 3)
**Goal**: Key-based ordering with parallelism

- [ ] Create `PerKeyProcessingStrategy`
  - [ ] Key extraction from record
  - [ ] Consistent hashing for work distribution
  - [ ] Per-worker key ordering
- [ ] Handle null keys (fallback to partition-based)
- [ ] Tests for key ordering guarantees
- [ ] Performance benchmarks

**Deliverable**: Per-key parallelism working

### Phase 4: Unordered Strategy (Week 4)
**Goal**: Maximum parallelism

- [ ] Create `UnorderedProcessingStrategy`
  - [ ] Single shared queue
  - [ ] Worker competition
  - [ ] Simplified offset tracking
- [ ] Optimize for maximum throughput
- [ ] Tests and benchmarks

**Deliverable**: All three strategies complete

### Phase 5: Polish & Production Ready (Week 5)
**Goal**: Production-ready with observability

- [ ] Comprehensive error handling
- [ ] Graceful shutdown with in-flight completion
- [ ] Detailed metrics
  - Worker utilization
  - Queue depths
  - Processing latency percentiles
  - In-flight record count
- [ ] Documentation
- [ ] Migration guide
- [ ] Examples

**Deliverable**: Production-ready release

## 5. Key Design Decisions

### 5.1 Worker Model: Tasks vs Threads
**Decision**: Use TPL (Task Parallel Library) with async/await

**Rationale**:
- External processing is typically I/O-bound (HTTP, DB calls)
- Tasks scale better than threads
- Better integration with modern .NET async patterns
- Lower overhead for high concurrency

### 5.2 Offset Commit Strategy
**Decision**: Track in-flight, commit only sequential completed

**Example** (Per-Partition):
```
Partition 0:
  Offset 10 - Completed ✓
  Offset 11 - Completed ✓
  Offset 12 - In-flight ...
  Offset 13 - Completed ✓
  Offset 14 - In-flight ...
  
  → Committable: 11 (can't commit 13 because 12 not done)
```

**Rationale**:
- Prevents record loss on restart
- Maintains at-least-once semantics
- May cause some reprocessing on crash (acceptable trade-off)

### 5.3 Backpressure Mechanism
**Decision**: Multi-level backpressure

1. **Queue-level**: Max queued records per strategy
2. **Consumer-level**: Pause partitions when queue full
3. **Global-level**: Stop polling when total in-flight > threshold

**Rationale**:
- Prevents OOM
- Maintains responsiveness
- Allows graceful degradation

### 5.4 Worker Pool Sizing
**Decision**: Configurable with smart defaults

- Default: `Environment.ProcessorCount` (CPU cores)
- Recommendation: 2-4x CPU cores for I/O-bound
- Max: Configurable limit (prevent runaway)

### 5.5 Ordering Guarantees
**Decision**: Clear contract per strategy

| Strategy       | Partition Order | Key Order | Cross-Key Order |
|----------------|----------------|-----------|-----------------|
| SEQUENTIAL     | ✓              | ✓         | ✓               |
| PER_PARTITION  | ✓              | ✓         | ✗               |
| PER_KEY        | ✗              | ✓         | ✗               |
| UNORDERED      | ✗              | ✗         | ✗               |

## 6. Offset Management Strategy

### 6.1 OffsetTracker Design

```csharp
internal class OffsetTracker
{
    // TopicPartition → SortedSet of (offset, completed)
    private ConcurrentDictionary<TopicPartition, OffsetState> _partitionStates;
    
    class OffsetState
    {
        public SortedSet<long> InFlight { get; }
        public long HighestCompleted { get; set; }
        public object Lock { get; }
    }
    
    public void RecordDispatched(TopicPartitionOffset tpo)
    {
        var state = GetOrCreateState(tpo.TopicPartition);
        lock(state.Lock)
        {
            state.InFlight.Add(tpo.Offset.Value);
        }
    }
    
    public void RecordCompleted(TopicPartitionOffset tpo)
    {
        var state = GetOrCreateState(tpo.TopicPartition);
        lock(state.Lock)
        {
            state.InFlight.Remove(tpo.Offset.Value);
            
            // Find highest sequential completed
            long expectedOffset = state.HighestCompleted + 1;
            while(!state.InFlight.Contains(expectedOffset))
            {
                state.HighestCompleted = expectedOffset;
                expectedOffset++;
            }
        }
    }
    
    public IEnumerable<TopicPartitionOffset> GetCommittableOffsets()
    {
        foreach(var kv in _partitionStates)
        {
            lock(kv.Value.Lock)
            {
                if(kv.Value.HighestCompleted >= 0)
                {
                    yield return new TopicPartitionOffset(
                        kv.Key, 
                        kv.Value.HighestCompleted + 1  // Kafka commits next offset
                    );
                }
            }
        }
    }
}
```

### 6.2 Commit Flow

1. **Poll**: Consumer polls records
2. **Dispatch**: Coordinator dispatches to workers, calls `OffsetTracker.RecordDispatched`
3. **Process**: Worker processes async
4. **Complete**: Worker completes, calls `OffsetTracker.RecordCompleted`
5. **Commit**: Periodically, get committable offsets and commit to Kafka

## 7. Error Handling & Retries

### 7.1 Retry Levels

#### Level 1: Worker-Level Retry (Existing)
- Current `RetryPolicy` in `ExternalProcessorTopologyExecutor`
- Handles transient failures
- Exponential backoff

#### Level 2: Coordinator-Level Retry (New)
- When worker-level exhausted but retry behavior is BUFFERED
- Re-enqueue to appropriate queue
- Track retry count per record

#### Level 3: Dead Letter Queue (Future)
- After all retries exhausted
- Optional DLQ topic
- Include error metadata

### 7.2 Error Handling by Strategy

**PER_PARTITION**:
- Failed record blocks partition
- Other partitions continue
- Eventual retry or DLQ

**PER_KEY**:
- Failed record blocks key
- Other keys continue
- May cause offset commit delays

**UNORDERED**:
- Failed record doesn't block others
- Maximum resilience
- May delay offset commits

## 8. Metrics & Observability

### 8.1 New Metrics

**Coordinator Metrics**:
- `parallel.in-flight-records` (Gauge): Current in-flight count
- `parallel.queue-depth` (Gauge): Records waiting in queues
- `parallel.worker-utilization` (Gauge): Active workers / total workers
- `parallel.dispatch-rate` (Rate): Records dispatched per second
- `parallel.completion-rate` (Rate): Records completed per second
- `parallel.offset-lag` (Gauge): Latest polled - latest committed per partition

**Strategy-Specific**:
- `parallel.partition.{id}.queue-depth` (Per-partition)
- `parallel.key.distribution` (Per-key histogram)

**Worker Metrics**:
- `parallel.worker.{id}.processing-time` (Histogram)
- `parallel.worker.{id}.idle-time` (Counter)

### 8.2 Health Checks

- All workers responsive (not hung)
- In-flight count not growing unbounded
- Offset lag not increasing continuously
- No partition stuck for > threshold time

## 9. Testing Strategy

### 9.1 Unit Tests

- `OffsetTracker` sequential completion tracking
- Each strategy's routing logic
- Worker pool management
- Backpressure mechanisms

### 9.2 Integration Tests

**Ordering Tests**:
- Verify partition order (PER_PARTITION)
- Verify key order (PER_KEY)
- Verify no unexpected ordering (UNORDERED)

**Failure Tests**:
- Worker crash during processing
- Rebalance during in-flight processing
- Retry exhaustion scenarios

**Performance Tests**:
- Baseline: SEQUENTIAL vs PER_PARTITION throughput
- Scalability: 1, 2, 4, 8, 16 workers
- Latency: p50, p95, p99 under load

### 9.3 Chaos Tests

- Random worker failures
- Random processing delays
- Network partitions during commit
- Consumer rebalances

## 10. Migration Path

### 10.1 Backward Compatibility

**Default Behavior**: `ParallelProcessingMode.SEQUENTIAL`
- Existing applications unchanged
- No breaking changes
- Same performance characteristics

### 10.2 Opt-In Migration

```csharp
// Step 1: Enable per-partition (safest)
config.ParallelProcessing = new ParallelProcessingConfig 
{
    Mode = ParallelProcessingMode.PER_PARTITION,
    MaxConcurrency = 4
};

// Step 2: Monitor metrics, tune concurrency

// Step 3: Consider per-key or unordered if ordering not critical
```

### 10.3 Rollback Strategy

- Configuration-based: Just change config back
- No data migration needed
- Offset commits remain compatible

## 11. Configuration Examples

### Example 1: Conservative (Low Risk)
```csharp
config.ParallelProcessing = new ParallelProcessingConfig
{
    Mode = ParallelProcessingMode.PER_PARTITION,
    MaxConcurrency = 2,  // Start small
    MaxQueuedRecords = 1000
};
```

### Example 2: Balanced (Most Use Cases)
```csharp
config.ParallelProcessing = new ParallelProcessingConfig
{
    Mode = ParallelProcessingMode.PER_KEY,
    MaxConcurrency = Environment.ProcessorCount * 2,
    MaxQueuedRecords = 10000
};
```

### Example 3: Maximum Throughput (Order Not Critical)
```csharp
config.ParallelProcessing = new ParallelProcessingConfig
{
    Mode = ParallelProcessingMode.UNORDERED,
    MaxConcurrency = 50,
    MaxQueuedRecords = 50000
};
```

## 12. Open Questions / Future Work

### 12.1 Dynamic Worker Scaling
- Auto-scale workers based on queue depth?
- Adaptive concurrency based on latency?

### 12.2 Priority Processing
- Process certain keys/partitions with higher priority?
- SLA-based scheduling?

### 12.3 Exactly-Once Semantics
- Current design maintains at-least-once
- Can we support exactly-once with parallel processing?
- Would require idempotency tracking

### 12.4 Cross-Partition Batching
- Batch commits across partitions?
- Trade-off: latency vs efficiency

## 13. Success Criteria

### 13.1 Functional
- ✓ All three strategies implemented and tested
- ✓ Ordering guarantees verified
- ✓ No offset loss or data duplication (beyond expected reprocessing)
- ✓ Graceful shutdown with in-flight completion

### 13.2 Performance
- ✓ Per-partition: 2-4x throughput improvement (I/O-bound workloads)
- ✓ Per-key: 3-6x throughput improvement
- ✓ Unordered: 5-10x throughput improvement
- ✓ Latency: p99 < 2x sequential mode

### 13.3 Production Ready
- ✓ Comprehensive metrics
- ✓ Documentation complete
- ✓ Migration guide published
- ✓ Example applications
- ✓ Performance benchmarks published

---

**Next Steps**:
1. Review and approve design
2. Create GitHub issues for each phase
3. Begin Phase 1 implementation
4. Set up benchmarking infrastructure
