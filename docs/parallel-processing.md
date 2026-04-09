# Parallel Processing for External Streams

## Overview

Since version `1.8.0`, Streamiz Kafka .NET supports parallel processing for external streams, enabling significant throughput improvements for I/O-bound workloads. This feature is inspired by Confluent's Parallel Consumer and provides four configurable processing strategies with different ordering guarantees and performance characteristics.

External streams are typically used when you need to process Kafka records that involve external I/O operations (database calls, HTTP requests, etc.) outside of the standard Kafka Streams topology.

## Processing Strategies

Streamiz provides four processing modes that offer different trade-offs between ordering guarantees and throughput:

| Strategy | Partition Order | Key Order | Cross-Key Order | Default Concurrency | Use Case |
|----------|----------------|-----------|-----------------|---------------------|----------|
| **SEQUENTIAL** | ✓ | ✓ | ✓ | 1 | Strict ordering required, backward compatibility |
| **PER_PARTITION** | ✓ | ✓ | ✗ | CPU cores | Partition-level ordering matters |
| **PER_KEY** | ✗ | ✓ | ✗ | CPU cores × 2 | Key-level ordering matters (e.g., user sessions) |
| **UNORDERED** | ✗ | ✗ | ✗ | CPU cores × 4 | Maximum throughput, order doesn't matter |

### Sequential Mode

The default mode that maintains full backward compatibility. Processes one record at a time in strict order.

**Ordering Guarantees:**
- ✓ Partition order preserved
- ✓ Per-key order preserved
- ✓ Cross-key order preserved

**Performance:**
- Baseline throughput (1x)
- Best for strictly ordered processing

**Example Use Cases:**
- Financial transactions where order is critical
- Legacy systems requiring sequential processing
- Scenarios where data dependencies exist across keys

### Per-Partition Mode

Processes different partitions in parallel while maintaining ordering within each partition.

**Ordering Guarantees:**
- ✓ Partition order preserved
- ✓ Per-key order preserved (keys within same partition)
- ✗ Cross-key order not guaranteed

**Performance:**
- 2-4x throughput for I/O-bound workloads
- Each worker processes one partition at a time

**Example Use Cases:**
- Processing logs from different servers (each server = partition)
- Multi-tenant systems where each tenant is on a separate partition
- Time-series data partitioned by sensor/device

**How It Works:**
- Records from different partitions are distributed to available workers
- Workers process records from their assigned partitions in FIFO order
- Same partition always goes to the same worker (while processing that batch)

### Per-Key Mode

Processes records with different keys in parallel while maintaining ordering for each individual key.

**Ordering Guarantees:**
- ✗ Partition order not guaranteed
- ✓ Per-key order preserved
- ✗ Cross-key order not guaranteed

**Performance:**
- 3-6x throughput for I/O-bound workloads
- Uses consistent hashing to route keys to workers

**Example Use Cases:**
- User session processing (events per user must be ordered)
- Entity updates (operations on same entity must be ordered)
- Shopping cart operations (per-user cart modifications)

**How It Works:**
- Uses consistent hashing algorithm to assign keys to workers
- Same key always goes to same worker (ensuring key ordering)
- Different keys may be processed in parallel by different workers
- Null or empty keys fall back to partition-based routing

### Unordered Mode

Maximum parallelism with no ordering guarantees. All workers compete for work from a shared queue.

**Ordering Guarantees:**
- ✗ Partition order not guaranteed
- ✗ Per-key order not guaranteed
- ✗ Cross-key order not guaranteed

**Performance:**
- 5-10x throughput for I/O-bound workloads
- Maximum possible concurrency

**Example Use Cases:**
- Independent analytics events
- Metrics collection
- Logging and monitoring data
- Stateless enrichment operations

**How It Works:**
- Single shared queue for all records
- Workers compete for the next available work item
- First available worker processes the next record
- No ordering maintained whatsoever

## Configuration

Streamiz supports parallel processing at two levels:
1. **Global configuration** - For external streams (via `ExternalProcessingConfig`)
2. **Per-processor configuration** - For individual async processors (via method parameters)

### Global Configuration (External Streams)

Configure parallel processing for external streams through the `ExternalProcessingConfig` property in your `StreamConfig`:

```csharp
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.Processors;

var config = new StreamConfig<StringSerDes, StringSerDes>();
config.ApplicationId = "my-parallel-app";
config.BootstrapServers = "localhost:9092";

// Configure parallel processing strategy
config.ExternalProcessingConfig = ParallelProcessingConfig.PerKey(maxConcurrency: 16);
```

### Factory Methods

Use the convenient factory methods to create configurations for each strategy:

```csharp
// Sequential (default) - 1 worker
config.ExternalProcessingConfig = ParallelProcessingConfig.Sequential();

// Per-Partition - default: CPU core count workers
config.ExternalProcessingConfig = ParallelProcessingConfig.PerPartition();

// Per-Partition - custom concurrency
config.ExternalProcessingConfig = ParallelProcessingConfig.PerPartition(maxConcurrency: 8);

// Per-Key - default: CPU cores × 2 workers
config.ExternalProcessingConfig = ParallelProcessingConfig.PerKey();

// Per-Key - custom concurrency
config.ExternalProcessingConfig = ParallelProcessingConfig.PerKey(maxConcurrency: 16);

// Unordered - default: CPU cores × 4 workers
config.ExternalProcessingConfig = ParallelProcessingConfig.Unordered();

// Unordered - custom concurrency
config.ExternalProcessingConfig = ParallelProcessingConfig.Unordered(maxConcurrency: 32);
```

### Advanced Configuration

For fine-grained control, you can configure all properties manually:

```csharp
config.ExternalProcessingConfig = new ParallelProcessingConfig
{
    Mode = ParallelProcessingMode.PER_KEY,
    MaxConcurrency = 16,                    // Number of worker threads
    MaxQueuedRecords = 10000,              // Queue capacity (triggers backpressure)
    MaxWaitForCompletion = TimeSpan.FromSeconds(30)  // Graceful shutdown timeout
};
```

#### Configuration Properties

| Property | Description | Default |
|----------|-------------|---------|
| `Mode` | Processing strategy (SEQUENTIAL, PER_PARTITION, PER_KEY, UNORDERED) | SEQUENTIAL |
| `MaxConcurrency` | Number of worker threads | Strategy-dependent |
| `MaxQueuedRecords` | Maximum records in queue before backpressure | 10,000 |
| `MaxWaitForCompletion` | Timeout when shutting down | 30 seconds |

## Performance Characteristics

### Expected Throughput

Performance improvements depend on workload characteristics:

| Strategy | I/O-Bound Workloads | CPU-Bound Workloads |
|----------|---------------------|---------------------|
| SEQUENTIAL | 1x (baseline) | 1x (baseline) |
| PER_PARTITION | 2-4x | 1.5-2x |
| PER_KEY | 3-6x | 2-3x |
| UNORDERED | 5-10x | 3-5x |

**Note:** These are approximate multipliers. Actual performance depends on:
- I/O latency and throughput
- CPU availability
- Key distribution (for PER_KEY mode)
- Partition count and distribution

### Choosing Concurrency Levels

**Default Values:**
- SEQUENTIAL: 1 worker (no parallelism)
- PER_PARTITION: `Environment.ProcessorCount` workers
- PER_KEY: `Environment.ProcessorCount × 2` workers
- UNORDERED: `Environment.ProcessorCount × 4` workers

**Guidelines:**
- **I/O-bound workloads**: Higher concurrency (2-4× CPU cores) typically helps
- **CPU-bound workloads**: Concurrency near CPU core count is usually optimal
- **Mixed workloads**: Start with default, tune based on monitoring
- **Resource constraints**: Reduce concurrency if experiencing memory pressure

### Backpressure

When the queue reaches `MaxQueuedRecords`, the system triggers backpressure:
- Kafka consumer pauses polling for new records
- Workers continue processing queued records
- Polling resumes when queue drops below capacity

This prevents out-of-memory errors and maintains system stability.

## Per-Processor Parallel Processing

**Since version 1.8.0**, you can configure parallel processing for individual async processors within your topology. This provides fine-grained control over concurrency for specific async operations.

### When to Use Per-Processor Configuration

Use per-processor configuration when:
- You have async operations **within** your topology (not external streams)
- Different processors have different I/O characteristics
- You want to tune concurrency for specific operations
- Order doesn't matter for that specific processor

### Configuration Example

```csharp
var builder = new StreamBuilder();

builder.Stream<string, User>("users")
    // High concurrency for database lookups
    .MapValuesAsync(
        async (record, ctx) =>
        {
            return await userDatabase.EnrichAsync(record.Value);
        },
        retryPolicy: RetryPolicy.NewBuilder()
            .NumberOfRetry(3)
            .RetriableException<IOException>()
            .Build(),
        parallelProcessingConfig: ParallelProcessingConfig.Unordered(maxConcurrency: 16))
    // Lower concurrency for external API calls (rate limited)
    .MapValuesAsync(
        async (record, ctx) =>
        {
            return await externalApi.FetchDataAsync(record.Value.Id);
        },
        retryPolicy: RetryPolicy.NewBuilder()
            .NumberOfRetry(5)
            .RetryBackOffMs(1000)
            .RetriableException<HttpRequestException>()
            .Build(),
        parallelProcessingConfig: ParallelProcessingConfig.Unordered(maxConcurrency: 4))
    .To("enriched-users");

Topology topology = builder.Build();
var stream = new KafkaStream(topology, config);
await stream.StartAsync();
```

### How It Works

Per-processor parallel processing uses bounded Task-based concurrency:

1. **Semaphore Control**: Uses `SemaphoreSlim` to limit concurrent operations to `MaxConcurrency`
2. **Fire-and-Forget**: Each record spawns an async Task that processes independently  
3. **Retry Logic**: Each Task has its own retry loop based on the `RetryPolicy`
4. **Graceful Shutdown**: `Close()` waits for all active tasks to complete (with timeout)

**Architecture:**
```
Record arrives → Wait for available slot → Spawn async Task → Process + Retry → Forward result
                 (Semaphore blocks if at max concurrency)
```

### Differences from Global Configuration

| Feature | Global (External Streams) | Per-Processor |
|---------|--------------------------|---------------|
| **Implementation** | Dedicated consumer thread + worker pool | Task-based parallelism |
| **Offset Management** | Tracks offsets, commits sequentially | No offset tracking (fires and forgets) |
| **Ordering** | Can maintain partition/key order based on mode | Records may complete out of order |
| **Resource Model** | Separate thread per external stream | Shared thread pool |
| **Best For** | External stream consumers | Inline async operations in topology |

### Best Practices

**1. Choose Appropriate Concurrency**
```csharp
// I/O-bound operations: Higher concurrency
parallelProcessingConfig: ParallelProcessingConfig.Unordered(maxConcurrency: 20)

// CPU-bound operations: Lower concurrency (near CPU count)
parallelProcessingConfig: ParallelProcessingConfig.Unordered(maxConcurrency: 4)

// Rate-limited APIs: Match rate limit
parallelProcessingConfig: ParallelProcessingConfig.Unordered(maxConcurrency: 10)
```

**2. Configure Retry Policies**
```csharp
retryPolicy: RetryPolicy.NewBuilder()
    .NumberOfRetry(3)
    .RetryBackOffMs(100)
    .RetriableException<IOException>()
    .RetriableException<TimeoutException>()
    .Build()
```

**3. Monitor Resource Usage**
- Watch thread pool saturation
- Monitor memory usage (each queued task consumes memory)
- Check external system load

**4. Handle Errors Gracefully**
- Use retry policies for transient errors
- Log non-retriable errors
- Consider dead letter queues for failed records

### Limitations

1. **No Ordering Guarantees**: Records may complete out of order
   - Even records with the same key may be processed concurrently
   - Results may be forwarded in different order than arrival

2. **No Offset Coordination**: Unlike external streams, per-processor parallelism doesn't track offsets
   - At-least-once semantics relies on Kafka consumer commits
   - Failed records are logged but not retried after stream restart

3. **Shared Resources**: All processors share the application's thread pool
   - Very high concurrency across multiple processors may cause contention
   - Monitor thread pool metrics

4. **Memory Overhead**: Each concurrent operation holds state in memory
   - Consider `MaxQueuedRecords` equivalent is the number of concurrent tasks
   - High concurrency = high memory usage

### When NOT to Use

Don't use per-processor parallelism if:
- **Ordering is critical**: Use external streams with appropriate mode instead
- **Large message volumes**: External streams with dedicated threads scale better
- **Need offset management**: External streams provide proper offset tracking

## Complete Example

### Use Case: User Event Processing with External API

Process user events that require enrichment from an external API, maintaining per-user ordering:

```csharp
using System;
using System.Net.Http;
using System.Threading.Tasks;
using Streamiz.Kafka.Net;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.SerDes;

class Program
{
    private static readonly HttpClient httpClient = new HttpClient();

    static async Task Main(string[] args)
    {
        // Configure with per-key parallel processing
        var config = new StreamConfig<StringSerDes, StringSerDes>
        {
            ApplicationId = "user-event-processor",
            BootstrapServers = "localhost:9092",
            AutoOffsetReset = Confluent.Kafka.AutoOffsetReset.Earliest,
            
            // Enable per-key parallel processing
            // Ensures events for each user are processed in order
            ExternalProcessingConfig = ParallelProcessingConfig.PerKey(
                maxConcurrency: 16  // 16 parallel workers
            )
        };

        var builder = new StreamBuilder();

        // Create external stream for user events
        var externalStream = builder.CreateExternalStream(
            "user-events",
            async (record) =>
            {
                // Enrich with external API call (simulated)
                var userId = record.Message.Key;
                var eventData = record.Message.Value;
                
                // This is I/O-bound - parallel processing helps!
                var userProfile = await FetchUserProfile(userId);
                
                // Process the enriched event
                var enrichedEvent = $"{eventData}|{userProfile}";
                
                Console.WriteLine($"[Worker {Task.CurrentId}] Processed user {userId}: {enrichedEvent}");
                
                return enrichedEvent;
            });

        // Build and start the topology
        Topology topology = builder.Build();
        var stream = new KafkaStream(topology, config);

        Console.CancelKeyPress += (o, e) => stream.Dispose();
        await stream.StartAsync();
    }

    static async Task<string> FetchUserProfile(string userId)
    {
        // Simulate external API call
        await Task.Delay(100);  // Simulated I/O latency
        return $"Profile-{userId}";
    }
}
```

### Example: Analytics Events (Unordered)

For independent analytics events where order doesn't matter:

```csharp
var config = new StreamConfig<StringSerDes, StringSerDes>
{
    ApplicationId = "analytics-processor",
    BootstrapServers = "localhost:9092",
    
    // Maximum parallelism for analytics
    ExternalProcessingConfig = ParallelProcessingConfig.Unordered(
        maxConcurrency: 32  // High concurrency for maximum throughput
    )
};

var externalStream = builder.CreateExternalStream(
    "analytics-events",
    async (record) =>
    {
        // Process independent analytics event
        await StoreAnalyticsEvent(record.Message.Value);
        return record.Message.Value;
    });
```

## Migration Guide

### Upgrading from Sequential Processing

The default behavior remains sequential, so existing code continues to work without changes:

```csharp
// Before (implicit sequential)
var config = new StreamConfig<StringSerDes, StringSerDes>();
config.ApplicationId = "my-app";
// ... external stream processing works as before

// After (explicit sequential - same behavior)
var config = new StreamConfig<StringSerDes, StringSerDes>();
config.ApplicationId = "my-app";
config.ExternalProcessingConfig = ParallelProcessingConfig.Sequential();
// ... identical behavior
```

### Enabling Parallel Processing

To enable parallel processing, simply configure the desired strategy:

```csharp
// Add this line to your existing configuration
config.ExternalProcessingConfig = ParallelProcessingConfig.PerKey();

// Everything else remains the same
```

### Testing Parallel Processing

When testing parallel processing:

1. **Start conservative:** Begin with lower concurrency and increase gradually
2. **Monitor metrics:** Track queue depth, in-flight records, and throughput
3. **Verify ordering:** Ensure your chosen strategy's ordering guarantees meet requirements
4. **Load test:** Test under realistic load before production deployment

## Monitoring

Parallel processing exposes additional metrics for monitoring and tuning. See the [Monitoring documentation](monitoring.md#parallel-processing-metrics) for details.

Key metrics to monitor:

- `parallel-in-flight-records`: Number of records currently being processed
- `parallel-queue-depth`: Number of records queued for processing
- `parallel-worker-count`: Number of active worker threads

## Best Practices

### Choosing the Right Strategy

1. **Start with requirements:**
   - Need strict ordering? → SEQUENTIAL
   - Need per-user/per-entity ordering? → PER_KEY
   - Need per-partition ordering? → PER_PARTITION
   - Order doesn't matter? → UNORDERED

2. **Consider your workload:**
   - Heavy I/O (DB, HTTP, etc.)? → Higher concurrency helps
   - CPU-intensive? → Concurrency near CPU count
   - Mixed? → Start with strategy defaults

3. **Test and measure:**
   - Monitor throughput and latency
   - Adjust concurrency based on metrics
   - Watch for resource constraints (CPU, memory, connections)

### Concurrency Tuning

- **Too low:** Underutilized resources, lower throughput
- **Too high:** Resource contention, diminishing returns, potential instability
- **Just right:** High throughput without resource saturation

Monitor queue depth:
- Consistently full? → Consider increasing concurrency
- Consistently empty? → May be over-provisioned
- Fluctuating? → Probably well-tuned

### Error Handling

Parallel processing maintains at-least-once semantics:
- Failed records are marked complete to avoid blocking
- Records will be reprocessed on restart
- Implement idempotent processing where possible

### Resource Management

- Each worker is a separate task using thread pool threads
- Configure `MaxQueuedRecords` based on available memory
- Consider external system limits (DB connection pools, API rate limits)

## Limitations

1. **Offset Management:**
   - Only sequential offsets can be committed
   - Out-of-order completion may delay commits
   - This is by design for at-least-once semantics

2. **Memory Usage:**
   - Queue size (`MaxQueuedRecords`) affects memory consumption
   - Each queued record holds a `ConsumeResult` in memory
   - Backpressure prevents unbounded growth

3. **External Stream Only:**
   - Parallel processing applies only to external streams
   - Regular Kafka Streams DSL operations are not affected
   - Standard topology maintains existing behavior

## Troubleshooting

### Low Throughput

**Symptoms:** Expected performance improvement not achieved

**Possible Causes:**
- Concurrency too low
- Queue size too small (frequent backpressure)
- External system is the bottleneck
- CPU-bound workload with excessive concurrency

**Solutions:**
- Monitor metrics to identify bottleneck
- Increase concurrency if workers are idle
- Increase queue size if backpressure is frequent
- Reduce concurrency for CPU-bound workloads

### High Memory Usage

**Symptoms:** Increasing memory consumption

**Possible Causes:**
- `MaxQueuedRecords` too high
- Large message sizes
- Slow external operations

**Solutions:**
- Reduce `MaxQueuedRecords`
- Increase concurrency to drain queue faster
- Optimize external operations

### Ordering Issues

**Symptoms:** Records processed out of expected order

**Possible Causes:**
- Wrong strategy for ordering requirements
- Misunderstanding of ordering guarantees

**Solutions:**
- Review ordering guarantee table
- Use more restrictive strategy if needed
- For PER_KEY: verify key distribution

### Shutdown Delays

**Symptoms:** Application takes long time to shut down

**Possible Causes:**
- Many in-flight records
- `MaxWaitForCompletion` too long
- External operations hanging

**Solutions:**
- Adjust `MaxWaitForCompletion` timeout
- Ensure external operations have timeouts
- Monitor in-flight count during shutdown

## See Also

- [Stream Configuration](stream-configuration.md#externalprocessingconfig) - Configuration reference
- [Monitoring](monitoring.md#parallel-processing-metrics) - Metrics and observability
- [External Stream Processing](processor-api.md) - External stream API basics
