# Monitoring Stream Applications

## Metrics

The Streamiz library reports a variety of metrics through his own registry. You have to specify a <a href="stream-configuration.html#optional-configuration-parameters" rel="stylesheet">metric reporter</a> (see `IStreamConfig.MetricsReporter`) to export all metrics to a different layer (Prometheus, Elastic, ...). This reporter will be triggered every `IStreamConfig.MetricsIntervalMs` (minimium and default : 30 seconds).

Streamiz provide an implementation for Prometheus. See `Streamiz.Kafka.Net.Metrics.Prometheus` package. 
You can use extension method to specify prometheus port and some other settings.

``` csharp
var config = new StreamConfig<StringSerDes, StringSerDes>();
config.ApplicationId = "test-app2";
config.BootstrapServers = "localhost:9092";
config.MetricsRecording = MetricsRecordingLevel.DEBUG;
// Run a http endpoint to 9090 and expose metrics to prometheus format
// Expose librdkafka metrics too !
config.UsePrometheusReporter(9090, true);
```

### Accessing Metrics

The entire metrics registry of a KafkaStream instance can be accessed read-only through the method `KafkaStream.Metrics()`. The metrics registry will contain all the available metrics listed below.

### Configuring Metrics Granularity

By default Streamiz has metrics with two recording levels: INFO and DEBUG. The `DEBUG` level records most metrics, while the `INFO` level records only some of them. Use the `IStreamConfig.MetricsRecording` configuration option to specify which metrics you want collected, see <a href="stream-configuration.html#metricsrecording" rel="stylesheet">configuration parameters</a>.

### Application metrics

All the following metrics have a recording level of `INFO`.

**Sensor : stream-metrics.sensor.app-info, Type : stream-metrics**

| Metric name      | Description | Tags          |
| :---             |    :----:   |          ---: |
| <span style="color: #06389C; font-weight: bold;">version</span>        | The version of the Streamiz client       | application_id,version  |
| <span style="color: #06389C; font-weight: bold;">application_id</span>        | The application ID of the Streamiz client        | application_id      |
| <span style="color: #06389C; font-weight: bold;">topology_description</span>        | The description of the topology executed in the Streamiz client        | application_id,topology_description      |
| <span style="color: #06389C; font-weight: bold;">state</span>        | The state of the Streamiz client (1 = running, 0 = stopped)        | application_id      |
| <span style="color: #06389C; font-weight: bold;">stream_threads</span>        | The number of stream threads that are running or participating in rebalance        | application_id      |

### Thread metrics

All the following metrics have a recording level of `INFO`.

**Sensor : internal.[THREAD_ID].sensor.thread-start-time, Type : stream-thread-metrics**

| Metric name      | Description | Tags          |
| :---             |    :----:   |          ---: |
| <span style="color: #06389C; font-weight: bold;">thread-start-time</span>        | The time that the thread was started       | thread_id  |

**Sensor : internal.[THREAD_ID].sensor.task-created, Type : stream-thread-metrics**

| Metric name      | Description | Tags          |
| :---             |    :----:   |          ---: |
| <span style="color: #06389C; font-weight: bold;">task-created-total</span>        | The total number of newly created tasks        | thread_id  |
| <span style="color: #06389C; font-weight: bold;">task-created-rate</span>        | The average per-second number of newly created tasks       | thread_id  |

**Sensor : internal.[THREAD_ID].sensor.task-closed, Type : stream-thread-metrics**

| Metric name      | Description | Tags          |
| :---             |    :----:   |          ---: |
| <span style="color: #06389C; font-weight: bold;">task-closed-total</span>        | The total number of closed tasks        | thread_id  |
| <span style="color: #06389C; font-weight: bold;">task-closed-rate</span>        | The average per-second number of closed tasks       | thread_id  |

**Sensor : internal.[THREAD_ID].sensor.commit, Type : stream-thread-metrics**

| Metric name      | Description | Tags          |
| :---             |    :----:   |          ---: |
| <span style="color: #06389C; font-weight: bold;">commit-total</span>        | The total number of calls to commit        | thread_id  |
| <span style="color: #06389C; font-weight: bold;">commit-rate</span>        | The average per-second number of calls to commit       | thread_id  |
| <span style="color: #06389C; font-weight: bold;">commit-latency-avg</span>        | The average commit latency       | thread_id  |
| <span style="color: #06389C; font-weight: bold;">commit-latency-max</span>        | The maximum commit latency       | thread_id  |

**Sensor : internal.[THREAD_ID].sensor.poll, Type : stream-thread-metrics**

| Metric name      | Description | Tags          |
| :---             |    :----:   |          ---: |
| <span style="color: #06389C; font-weight: bold;">poll-total</span>        | The total number of calls to poll        | thread_id  |
| <span style="color: #06389C; font-weight: bold;">poll-rate</span>        | The average per-second number of calls to poll       | thread_id  |
| <span style="color: #06389C; font-weight: bold;">poll-latency-avg</span>        | The average poll latency       | thread_id  |
| <span style="color: #06389C; font-weight: bold;">poll-latency-max</span>        | The maximum poll latency       | thread_id  |

**Sensor : internal.[THREAD_ID].sensor.poll-records, Type : stream-thread-metrics**

| Metric name      | Description | Tags          |
| :---             |    :----:   |          ---: |
| <span style="color: #06389C; font-weight: bold;">poll-records-max</span>        | The maximum number of records polled from consumer within an iteration       | thread_id  |
| <span style="color: #06389C; font-weight: bold;">poll-records-avg</span>        | The average number of records polled from consumer within an iteration       | thread_id  |

**Sensor : internal.[THREAD_ID].sensor.process-records, Type : stream-thread-metrics**

| Metric name      | Description | Tags          |
| :---             |    :----:   |          ---: |
| <span style="color: #06389C; font-weight: bold;">process-records-max</span>        | The maximum number of records processed from consumer within an iteration        | thread_id  |
| <span style="color: #06389C; font-weight: bold;">process-records-avg</span>        | The average number of records processed from consumer within an iteration       | thread_id  |

**Sensor : internal.[THREAD_ID].sensor.process-rate, Type : stream-thread-metrics**

| Metric name      | Description | Tags          |
| :---             |    :----:   |          ---: |
| <span style="color: #06389C; font-weight: bold;">process-rate-rate</span>        | The average per-second number of calls to process        | thread_id  |
| <span style="color: #06389C; font-weight: bold;">process-rate-total</span>       | The total number of calls to process       | thread_id  |

**Sensor : internal.[THREAD_ID].sensor.process-latency, Type : stream-thread-metrics**

| Metric name      | Description | Tags          |
| :---             |    :----:   |          ---: |
| <span style="color: #06389C; font-weight: bold;">process-latency-avg</span>        | The average process latency        | thread_id  |
| <span style="color: #06389C; font-weight: bold;">process-latency-max</span>       | The maximum process latency       | thread_id  |

**Sensor : internal.[THREAD_ID].sensor.process-ratio, Type : stream-thread-metrics**

| Metric name      | Description | Tags          |
| :---             |    :----:   |          ---: |
| <span style="color: #06389C; font-weight: bold;">process-ratio</span>        | The fraction of time the thread spent on processing active tasks        | thread_id  |

**Sensor : internal.[THREAD_ID].sensor.poll-ratio, Type : stream-thread-metrics**

| Metric name      | Description | Tags          |
| :---             |    :----:   |          ---: |
| <span style="color: #06389C; font-weight: bold;">poll-ratio</span>        | The fraction of time the thread spent on polling records from consumer        | thread_id  |

**Sensor : internal.[THREAD_ID].sensor.commit-ratio, Type : stream-thread-metrics**

| Metric name      | Description | Tags          |
| :---             |    :----:   |          ---: |
| <span style="color: #06389C; font-weight: bold;">commit-ratio</span>        | The fraction of time the thread spent on committing all tasks        | thread_id  |

### Task metrics

All the following metrics have a recording level of `DEBUG`.

**Sensor : internal.[THREAD_ID].task.[TASK_ID].sensor.process-latency, Type : stream-task-metrics**

| Metric name      | Description | Tags          |
| :---             |    :----:   |          ---: |
| <span style="color: #06389C; font-weight: bold;">process-latency-avg</span>        | The average latency of calls to process        | thread_id,task_id  |
| <span style="color: #06389C; font-weight: bold;">process-latency-max</span>       | The maximum latency of calls to process        | thread_id,task_id  |

**Sensor : internal.[THREAD_ID].task.[TASK_ID].sensor.enforced-processing, Type : stream-task-metrics**

| Metric name      | Description | Tags          |
| :---             |    :----:   |          ---: |
| <span style="color: #06389C; font-weight: bold;">enforced-processing-total</span>        | The total number of occurrences of enforced-processing operations        | thread_id,task_id  |
| <span style="color: #06389C; font-weight: bold;">enforced-processing-rate</span>        | The average number of occurrences of enforced-processing operations per second       | thread_id,task_id  |

**Sensor : internal.[THREAD_ID].task.[TASK_ID].sensor.process, Type : stream-task-metrics**

| Metric name      | Description | Tags          |
| :---             |    :----:   |          ---: |
| <span style="color: #06389C; font-weight: bold;">process-rate</span>        | The average number of calls to process per second        | thread_id,task_id  |
| <span style="color: #06389C; font-weight: bold;">process-total</span>       | The total number of calls to process        | thread_id,task_id  |

**Sensor : internal.[THREAD_ID].task.[TASK_ID].sensor.commit, Type : stream-task-metrics**

| Metric name      | Description | Tags          |
| :---             |    :----:   |          ---: |
| <span style="color: #06389C; font-weight: bold;">commit-rate</span>        | The average number of calls to commit per second        | thread_id,task_id  |
| <span style="color: #06389C; font-weight: bold;">commit-total</span>       | The total number of calls to commit        | thread_id,task_id  |

**Sensor : internal.[THREAD_ID].task.[TASK_ID].sensor.dropped-records, Type : stream-task-metrics**

| Metric name      | Description | Tags          |
| :---             |    :----:   |          ---: |
| <span style="color: #06389C; font-weight: bold;">dropped-records-rate</span>        | The average number of dropped records per second        | thread_id,task_id  |
| <span style="color: #06389C; font-weight: bold;">dropped-records-total</span>       | The total number of dropped records        | thread_id,task_id  |

**Sensor : internal.[THREAD_ID].task.[TASK_ID].sensor.active-buffer-count, Type : stream-task-metrics**

| Metric name      | Description | Tags          |
| :---             |    :----:   |          ---: |
| <span style="color: #06389C; font-weight: bold;">active-buffer-count</span>        | The count of buffered records that are polled from consumer and not yet processed for this active task        | thread_id,task_id  |

**Sensor : internal.[THREAD_ID].task.[TASK_ID].sensor.restoration-records, Type : stream-task-metrics**

| Metric name      | Description | Tags          |
| :---             |    :----:   |          ---: |
| <span style="color: #06389C; font-weight: bold;">restoration-records</span>        | The count of records not restored yet for this active task        | thread_id,task_id  |

**Sensor : internal.[THREAD_ID].task.[TASK_ID].sensor.active-restoration, Type : stream-task-metrics**

| Metric name      | Description | Tags          |
| :---             |    :----:   |          ---: |
| <span style="color: #06389C; font-weight: bold;">active-restoration</span>        | Indicate if the active task is in restoration or not (1 = restoration in progress, 0 = restoration done)        | thread_id,task_id  |

### Processor metrics

All the following metrics have a recording level of `DEBUG`.

**Sensor : internal.[THREAD_ID].task.[TASK_ID].node.[PROCESSOR_NODE_ID].sensor.process, Type : stream-processor-node-metrics**

| Metric name      | Description | Tags          |
| :---             |    :----:   |          ---: |
| <span style="color: #06389C; font-weight: bold;">process-rate</span>        | The average number of calls to process per second        | thread_id,task_id,processor_node_id  |
| <span style="color: #06389C; font-weight: bold;">process-total</span>       | The total number of calls to process        | thread_id,task_id,processor_node_id  |

### State store metrics

All the following metrics have a recording level of `DEBUG`. The `store-scope` value is specified in `IStoreSupplier.MetricsScope` for the user’s customized state stores; for built-in state stores, currently we have:
- in-memory_state
- in-memory-window_state
- rocksdb_state (for RocksDB backed key-value store)
- rocksdb-window_state (for RocksDB backed window store)


**Sensor : internal.[THREAD_ID].task.[TASK_ID].store.[STORE_NAME].sensor.put, Type : stream-state-metrics**

| Metric name      | Description | Tags          |
| :---             |    :----:   |          ---: |
| <span style="color: #06389C; font-weight: bold;">put-rate</span>        | The average number of calls to put per second        | thread_id,task_id,[STORE-TYPE]_id  |
| <span style="color: #06389C; font-weight: bold;">put-avg</span>       | The average latency of calls to put        | thread_id,task_id,[STORE-TYPE]_id  |
| <span style="color: #06389C; font-weight: bold;">put-max</span>       | The maximum latency of calls to put        | thread_id,task_id,[STORE-TYPE]_id  |

**Sensor : internal.[THREAD_ID].task.[TASK_ID].store.[STORE_NAME].sensor.put-if-absent, Type : stream-state-metrics**

| Metric name      | Description | Tags          |
| :---             |    :----:   |          ---: |
| <span style="color: #06389C; font-weight: bold;">put-rate</span>        | The average number of calls to put-if-absent per second        | thread_id,task_id,[STORE-TYPE]_id  |
| <span style="color: #06389C; font-weight: bold;">put-avg</span>       | The average latency of calls to put-if-absent        | thread_id,task_id,[STORE-TYPE]_id  |
| <span style="color: #06389C; font-weight: bold;">put-max</span>       | The maximum latency of calls to put-if-absent        | thread_id,task_id,[STORE-TYPE]_id  |

**Sensor : internal.[THREAD_ID].task.[TASK_ID].store.[STORE_NAME].sensor.put-all, Type : stream-state-metrics**

| Metric name      | Description | Tags          |
| :---             |    :----:   |          ---: |
| <span style="color: #06389C; font-weight: bold;">put-rate</span>        | The average number of calls to put-all per second        | thread_id,task_id,[STORE-TYPE]_id  |
| <span style="color: #06389C; font-weight: bold;">put-avg</span>       | The average latency of calls to put-all        | thread_id,task_id,[STORE-TYPE]_id  |
| <span style="color: #06389C; font-weight: bold;">put-max</span>       | The maximum latency of calls to put-all        | thread_id,task_id,[STORE-TYPE]_id  |

**Sensor : internal.[THREAD_ID].task.[TASK_ID].store.[STORE_NAME].sensor.get, Type : stream-state-metrics**

| Metric name      | Description | Tags          |
| :---             |    :----:   |          ---: |
| <span style="color: #06389C; font-weight: bold;">put-rate</span>        | The average number of calls to get per second        | thread_id,task_id,[STORE-TYPE]_id  |
| <span style="color: #06389C; font-weight: bold;">put-avg</span>       | The average latency of calls to get        | thread_id,task_id,[STORE-TYPE]_id  |
| <span style="color: #06389C; font-weight: bold;">put-max</span>       | The maximum latency of calls to get        | thread_id,task_id,[STORE-TYPE]_id  |

**Sensor : internal.[THREAD_ID].task.[TASK_ID].store.[STORE_NAME].sensor.fetch, Type : stream-state-metrics**

| Metric name      | Description | Tags          |
| :---             |    :----:   |          ---: |
| <span style="color: #06389C; font-weight: bold;">put-rate</span>        | The average number of calls to fetch per second        | thread_id,task_id,[STORE-TYPE]_id  |
| <span style="color: #06389C; font-weight: bold;">put-avg</span>       | The average latency of calls to fetch        | thread_id,task_id,[STORE-TYPE]_id  |
| <span style="color: #06389C; font-weight: bold;">put-max</span>       | The maximum latency of calls to fetch        | thread_id,task_id,[STORE-TYPE]_id  |

**Sensor : internal.[THREAD_ID].task.[TASK_ID].store.[STORE_NAME].sensor.all, Type : stream-state-metrics**

| Metric name      | Description | Tags          |
| :---             |    :----:   |          ---: |
| <span style="color: #06389C; font-weight: bold;">put-rate</span>        | The average number of calls to all per second        | thread_id,task_id,[STORE-TYPE]_id  |
| <span style="color: #06389C; font-weight: bold;">put-avg</span>       | The average latency of calls to all        | thread_id,task_id,[STORE-TYPE]_id  |
| <span style="color: #06389C; font-weight: bold;">put-max</span>       | The maximum latency of calls to all        | thread_id,task_id,[STORE-TYPE]_id  |

**Sensor : internal.[THREAD_ID].task.[TASK_ID].store.[STORE_NAME].sensor.range, Type : stream-state-metrics**

| Metric name      | Description | Tags          |
| :---             |    :----:   |          ---: |
| <span style="color: #06389C; font-weight: bold;">put-rate</span>        | The average number of calls to range per second        | thread_id,task_id,[STORE-TYPE]_id  |
| <span style="color: #06389C; font-weight: bold;">put-avg</span>       | The average latency of calls to range        | thread_id,task_id,[STORE-TYPE]_id  |
| <span style="color: #06389C; font-weight: bold;">put-max</span>       | The maximum latency of calls to range        | thread_id,task_id,[STORE-TYPE]_id  |

**Sensor : internal.[THREAD_ID].task.[TASK_ID].store.[STORE_NAME].sensor.flush, Type : stream-state-metrics**

| Metric name      | Description | Tags          |
| :---             |    :----:   |          ---: |
| <span style="color: #06389C; font-weight: bold;">put-rate</span>        | The average number of calls to flush per second        | thread_id,task_id,[STORE-TYPE]_id  |
| <span style="color: #06389C; font-weight: bold;">put-avg</span>       | The average latency of calls to flush        | thread_id,task_id,[STORE-TYPE]_id  |
| <span style="color: #06389C; font-weight: bold;">put-max</span>       | The maximum latency of calls to flush        | thread_id,task_id,[STORE-TYPE]_id  |

**Sensor : internal.[THREAD_ID].task.[TASK_ID].store.[STORE_NAME].sensor.delete, Type : stream-state-metrics**

| Metric name      | Description | Tags          |
| :---             |    :----:   |          ---: |
| <span style="color: #06389C; font-weight: bold;">put-rate</span>        | The average number of calls to delete per second        | thread_id,task_id,[STORE-TYPE]_id  |
| <span style="color: #06389C; font-weight: bold;">put-avg</span>       | The average latency of calls to delete        | thread_id,task_id,[STORE-TYPE]_id  |
| <span style="color: #06389C; font-weight: bold;">put-max</span>       | The maximum latency of calls to delete        | thread_id,task_id,[STORE-TYPE]_id  |

**Sensor : internal.[THREAD_ID].task.[TASK_ID].store.[STORE_NAME].sensor.remove, Type : stream-state-metrics**

| Metric name      | Description | Tags          |
| :---             |    :----:   |          ---: |
| <span style="color: #06389C; font-weight: bold;">put-rate</span>        | The average number of calls to remove per second        | thread_id,task_id,[STORE-TYPE]_id  |
| <span style="color: #06389C; font-weight: bold;">put-avg</span>       | The average latency of calls to remove        | thread_id,task_id,[STORE-TYPE]_id  |
| <span style="color: #06389C; font-weight: bold;">put-max</span>       | The maximum latency of calls to remove        | thread_id,task_id,[STORE-TYPE]_id  |

### RocksDb metrics

*Not available for now, stay tuned works in progress*


### Librdkafka metrics

Only main consumer and producer librdkafka statistics are exposed for now in Streamiz application. All the following metrics have a recording level of `INFO`.

#### Main consumer

**Sensor : librdkafka.[LIBRDKAFKA_CLIENT_ID].sensor.messages-consumed-total, Type : librdkafka-consumer-metrics**

| Metric name      | Description | Tags          |
| :---             |    :----:   |          ---: |
| <span style="color: #06389C; font-weight: bold;">messages-consumed-total</span>        | Total number of messages consumed, not including ignored messages (due to offset, etc), from Kafka brokers        | librdkafka_client_id, application_id  |

**Sensor : librdkafka.[LIBRDKAFKA_CLIENT_ID].sensor.bytes-consumed-total, Type : librdkafka-consumer-metrics**

| Metric name      | Description | Tags          |
| :---             |    :----:   |          ---: |
| <span style="color: #06389C; font-weight: bold;">bytes-consumed-total</span>        | Total number of bytes received from Kafka brokers        | librdkafka_client_id, application_id  |

**Sensor : librdkafka.[LIBRDKAFKA_CLIENT_ID].sensor.ops-waiting-queue-total, Type : librdkafka-consumer-metrics**

| Metric name      | Description | Tags          |
| :---             |    :----:   |          ---: |
| <span style="color: #06389C; font-weight: bold;">ops-waiting-queue-total</span>        | Number of ops (callbacks, events, etc) waiting in queue for application to serve with rd_kafka_poll().        | librdkafka_client_id, application_id  |

**Sensor : librdkafka.[LIBRDKAFKA_CLIENT_ID].sensor.responses-received-total, Type : librdkafka-consumer-metrics**

| Metric name      | Description | Tags          |
| :---             |    :----:   |          ---: |
| <span style="color: #06389C; font-weight: bold;">responses-received-total</span>        | Total number of responses received from Kafka brokers.        | librdkafka_client_id, application_id  |

**Sensor : librdkafka.[LIBRDKAFKA_CLIENT_ID].sensor.responses-bytes-received-total, Type : librdkafka-consumer-metrics**

| Metric name      | Description | Tags          |
| :---             |    :----:   |          ---: |
| <span style="color: #06389C; font-weight: bold;">responses-bytes-received-total</span>        | Total number of reponses bytes received from Kafka brokers.        | librdkafka_client_id, application_id  |

**Sensor : librdkafka.[LIBRDKAFKA_CLIENT_ID].sensor.time-rebalance-age, Type : librdkafka-consumer-metrics**

| Metric name      | Description | Tags          |
| :---             |    :----:   |          ---: |
| <span style="color: #06389C; font-weight: bold;">time-rebalance-age</span>        | Time elapsed since last rebalance (assign or revoke) (milliseconds).        | librdkafka_client_id, application_id  |

**Sensor : librdkafka.[LIBRDKAFKA_CLIENT_ID].sensor.rebalance-total, Type : librdkafka-consumer-metrics**

| Metric name      | Description | Tags          |
| :---             |    :----:   |          ---: |
| <span style="color: #06389C; font-weight: bold;">rebalance-total</span>        | Total number of rebalances (assign or revoke).        | librdkafka_client_id, application_id  |

##### Broker metrics

**Sensor : librdkafka.[LIBRDKAFKA_CLIENT_ID].sensor.broker-responses-received-total, Type : librdkafka-consumer-metrics**

| Metric name      | Description | Tags          |
| :---             |    :----:   |          ---: |
| <span style="color: #06389C; font-weight: bold;">broker-responses-received-total</span>        | Total number of responses received.        | librdkafka_client_id, application_id, broker_id  |

**Sensor : librdkafka.[LIBRDKAFKA_CLIENT_ID].sensor.broker-responses-bytes-received-total, Type : librdkafka-consumer-metrics**

| Metric name      | Description | Tags          |
| :---             |    :----:   |          ---: |
| <span style="color: #06389C; font-weight: bold;">broker-responses-bytes-received-total</span>        | Total number of bytes received.        | librdkafka_client_id, application_id, broker_id  |

**Sensor : librdkafka.[LIBRDKAFKA_CLIENT_ID].sensor.broker-error-received-total, Type : librdkafka-consumer-metrics**

| Metric name      | Description | Tags          |
| :---             |    :----:   |          ---: |
| <span style="color: #06389C; font-weight: bold;">broker-error-received-total</span>        | Total number of receive errors.        | librdkafka_client_id, application_id, broker_id  |

**Sensor : librdkafka.[LIBRDKAFKA_CLIENT_ID].sensor.broker-connection-total, Type : librdkafka-consumer-metrics**

| Metric name      | Description | Tags          |
| :---             |    :----:   |          ---: |
| <span style="color: #06389C; font-weight: bold;">broker-connection-total</span>        | Number of connection attempts, including successful and failed, and name resolution failures.        | librdkafka_client_id, application_id, broker_id  |

**Sensor : librdkafka.[LIBRDKAFKA_CLIENT_ID].sensor.broker-disconnection-total, Type : librdkafka-consumer-metrics**

| Metric name      | Description | Tags          |
| :---             |    :----:   |          ---: |
| <span style="color: #06389C; font-weight: bold;">broker-disconnection-total</span>        | Number of disconnects (triggered by broker, network, load-balancer, etc.).        | librdkafka_client_id, application_id, broker_id  |

**Sensor : librdkafka.[LIBRDKAFKA_CLIENT_ID].sensor.broker-latency-avg-micros, Type : librdkafka-consumer-metrics**

| Metric name      | Description | Tags          |
| :---             |    :----:   |          ---: |
| <span style="color: #06389C; font-weight: bold;">broker-latency-avg-micros</span>        |Broker latency / round-trip time in microseconds.        | librdkafka_client_id, application_id, broker_id  |

##### Topic metrics

**Sensor : librdkafka.[LIBRDKAFKA_CLIENT_ID].sensor.topic-batch-size-bytes-avg, Type : librdkafka-consumer-metrics**

| Metric name      | Description | Tags          |
| :---             |    :----:   |          ---: |
| <span style="color: #06389C; font-weight: bold;">topic-batch-size-bytes-avg</span>        |Batch sizes in bytes average.        | librdkafka_client_id, application_id, topic  |

**Sensor : librdkafka.[LIBRDKAFKA_CLIENT_ID].sensor.topic-batch-message-count-avg, Type : librdkafka-consumer-metrics**

| Metric name      | Description | Tags          |
| :---             |    :----:   |          ---: |
| <span style="color: #06389C; font-weight: bold;">topic-batch-message-count-avg</span>        |Batch message counts average.        | librdkafka_client_id, application_id, topic  |

##### Partition metrics

**Sensor : librdkafka.[LIBRDKAFKA_CLIENT_ID].sensor.consumer-lag, Type : librdkafka-consumer-metrics**

| Metric name      | Description | Tags          |
| :---             |    :----:   |          ---: |
| <span style="color: #06389C; font-weight: bold;">consumer-lag</span>        | Difference between (hi_offset or ls_offset) - max(app_offset, committed_offset). hi_offset is used when isolation.level=read_uncommitted, otherwise ls_offset.        | librdkafka_client_id, application_id, partition_id  |

**Sensor : librdkafka.[LIBRDKAFKA_CLIENT_ID].sensor.partition-messages-consumed-total, Type : librdkafka-consumer-metrics**

| Metric name      | Description | Tags          |
| :---             |    :----:   |          ---: |
| <span style="color: #06389C; font-weight: bold;">partition-messages-consumed-total</span>        | Total number of messages consumed, not including ignored messages (due to offset, etc).        | librdkafka_client_id, application_id, partition_id  |

**Sensor : librdkafka.[LIBRDKAFKA_CLIENT_ID].sensor.partition-bytes-consumed-total, Type : librdkafka-consumer-metrics**

| Metric name      | Description | Tags          |
| :---             |    :----:   |          ---: |
| <span style="color: #06389C; font-weight: bold;">partition-bytes-consumed-total</span>        | Total number of bytes received for rxmsgs.       | librdkafka_client_id, application_id, partition_id  |

#### Producer

**Sensor : librdkafka.[LIBRDKAFKA_CLIENT_ID].sensor.messages-produced-total, Type : librdkafka-producer-metrics**

| Metric name      | Description | Tags          |
| :---             |    :----:   |          ---: |
| <span style="color: #06389C; font-weight: bold;">messages-produced-total</span>        | Total number of messages transmitted (produced) to Kafka brokers        | librdkafka_client_id, application_id  |

**Sensor : librdkafka.[LIBRDKAFKA_CLIENT_ID].sensor.bytes-produced-total, Type : librdkafka-producer-metrics**

| Metric name      | Description | Tags          |
| :---             |    :----:   |          ---: |
| <span style="color: #06389C; font-weight: bold;">bytes-produced-total</span>        | Total number of message bytes (including framing, such as per-Message framing and MessageSet/batch framing) transmitted to Kafka brokers        | librdkafka_client_id, application_id  |

**Sensor : librdkafka.[LIBRDKAFKA_CLIENT_ID].sensor.ops-waiting-queue-total, Type : librdkafka-producer-metrics**

| Metric name      | Description | Tags          |
| :---             |    :----:   |          ---: |
| <span style="color: #06389C; font-weight: bold;">ops-waiting-queue-total</span>        | Number of ops (callbacks, events, etc) waiting in queue for application to serve with rd_kafka_poll()        | librdkafka_client_id, application_id  |

**Sensor : librdkafka.[LIBRDKAFKA_CLIENT_ID].sensor.messages-queue-current-total, Type : librdkafka-producer-metrics**

| Metric name      | Description | Tags          |
| :---             |    :----:   |          ---: |
| <span style="color: #06389C; font-weight: bold;">messages-queue-current-total</span>        | Current number of messages in producer queues        | librdkafka_client_id, application_id  |

**Sensor : librdkafka.[LIBRDKAFKA_CLIENT_ID].sensor.bytes-queue-current-total, Type : librdkafka-producer-metrics**

| Metric name      | Description | Tags          |
| :---             |    :----:   |          ---: |
| <span style="color: #06389C; font-weight: bold;">bytes-queue-current-total</span>        | Current total size of messages in producer queues        | librdkafka_client_id, application_id  |

**Sensor : librdkafka.[LIBRDKAFKA_CLIENT_ID].sensor.messages-queue-max, Type : librdkafka-producer-metrics**

| Metric name      | Description | Tags          |
| :---             |    :----:   |          ---: |
| <span style="color: #06389C; font-weight: bold;">messages-queue-max</span>        | Threshold: maximum number of messages allowed allowed on the producer queues        | librdkafka_client_id, application_id  |

**Sensor : librdkafka.[LIBRDKAFKA_CLIENT_ID].sensor.bytes-queue-max, Type : librdkafka-producer-metrics**

| Metric name      | Description | Tags          |
| :---             |    :----:   |          ---: |
| <span style="color: #06389C; font-weight: bold;">bytes-queue-max</span>        | Threshold: maximum total size of messages allowed on the producer queues        | librdkafka_client_id, application_id  |

**Sensor : librdkafka.[LIBRDKAFKA_CLIENT_ID].sensor.request-produced-total, Type : librdkafka-producer-metrics**

| Metric name      | Description | Tags          |
| :---             |    :----:   |          ---: |
| <span style="color: #06389C; font-weight: bold;">request-produced-total</span>        | Total number of requests sent to Kafka brokers        | librdkafka_client_id, application_id  |

**Sensor : librdkafka.[LIBRDKAFKA_CLIENT_ID].sensor.request-bytes-produced-total, Type : librdkafka-producer-metrics**

| Metric name      | Description | Tags          |
| :---             |    :----:   |          ---: |
| <span style="color: #06389C; font-weight: bold;">request-bytes-produced-total</span>        | Total number of bytes transmitted to Kafka brokers        | librdkafka_client_id, application_id  |

##### Broker metrics

**Sensor : librdkafka.[LIBRDKAFKA_CLIENT_ID].sensor.broker-request-awaiting-total, Type : librdkafka-producer-metrics**

| Metric name      | Description | Tags          |
| :---             |    :----:   |          ---: |
| <span style="color: #06389C; font-weight: bold;">broker-request-awaiting-total</span>        | Number of requests awaiting transmission to broker        | librdkafka_client_id, application_id,broker_id  |

**Sensor : librdkafka.[LIBRDKAFKA_CLIENT_ID].sensor.broker-message-awaiting-total, Type : librdkafka-producer-metrics**

| Metric name      | Description | Tags          |
| :---             |    :----:   |          ---: |
| <span style="color: #06389C; font-weight: bold;">broker-message-awaiting-total</span>        | Number of messages awaiting transmission to broker        | librdkafka_client_id, application_id,broker_id  |

**Sensor : librdkafka.[LIBRDKAFKA_CLIENT_ID].sensor.broker-request-in-flight-total, Type : librdkafka-producer-metrics**

| Metric name      | Description | Tags          |
| :---             |    :----:   |          ---: |
| <span style="color: #06389C; font-weight: bold;">broker-request-in-flight-total</span>        | Number of requests in-flight to broker awaiting response        | librdkafka_client_id, application_id,broker_id  |

**Sensor : librdkafka.[LIBRDKAFKA_CLIENT_ID].sensor.broker-message-in-flight-total, Type : librdkafka-producer-metrics**

| Metric name      | Description | Tags          |
| :---             |    :----:   |          ---: |
| <span style="color: #06389C; font-weight: bold;">broker-message-in-flight-total</span>        | Number of messages in-flight to broker awaiting response        | librdkafka_client_id, application_id,broker_id  |

**Sensor : librdkafka.[LIBRDKAFKA_CLIENT_ID].sensor.broker-request-sent-total, Type : librdkafka-producer-metrics**

| Metric name      | Description | Tags          |
| :---             |    :----:   |          ---: |
| <span style="color: #06389C; font-weight: bold;">broker-request-sent-total</span>        | Total number of requests sent        | librdkafka_client_id, application_id,broker_id  |

**Sensor : librdkafka.[LIBRDKAFKA_CLIENT_ID].sensor.broker-request-sent-bytes-total, Type : librdkafka-producer-metrics**

| Metric name      | Description | Tags          |
| :---             |    :----:   |          ---: |
| <span style="color: #06389C; font-weight: bold;">broker-request-sent-bytes-total</span>        | Total number of bytes sent        | librdkafka_client_id, application_id,broker_id  |

**Sensor : librdkafka.[LIBRDKAFKA_CLIENT_ID].sensor.broker-error-total, Type : librdkafka-producer-metrics**

| Metric name      | Description | Tags          |
| :---             |    :----:   |          ---: |
| <span style="color: #06389C; font-weight: bold;">broker-error-total</span>        | Total number of transmission errors        | librdkafka_client_id, application_id,broker_id  |

**Sensor : librdkafka.[LIBRDKAFKA_CLIENT_ID].sensor.broker-retries-total, Type : librdkafka-producer-metrics**

| Metric name      | Description | Tags          |
| :---             |    :----:   |          ---: |
| <span style="color: #06389C; font-weight: bold;">broker-retries-total</span>        | Total number of request retries        | librdkafka_client_id, application_id,broker_id  |

**Sensor : librdkafka.[LIBRDKAFKA_CLIENT_ID].sensor.broker-timeout-total, Type : librdkafka-producer-metrics**

| Metric name      | Description | Tags          |
| :---             |    :----:   |          ---: |
| <span style="color: #06389C; font-weight: bold;">broker-timeout-total</span>        | Total number of requests timed out        | librdkafka_client_id, application_id,broker_id  |

**Sensor : librdkafka.[LIBRDKAFKA_CLIENT_ID].sensor.broker-connection-total, Type : librdkafka-producer-metrics**

| Metric name      | Description | Tags          |
| :---             |    :----:   |          ---: |
| <span style="color: #06389C; font-weight: bold;">broker-connection-total</span>        | Number of connection attempts, including successful and failed, and name resolution failures.        | librdkafka_client_id, application_id,broker_id  |

**Sensor : librdkafka.[LIBRDKAFKA_CLIENT_ID].sensor.broker-disconnection-total, Type : librdkafka-producer-metrics**

| Metric name      | Description | Tags          |
| :---             |    :----:   |          ---: |
| <span style="color: #06389C; font-weight: bold;">broker-disconnection-total</span>        | Number of disconnects (triggered by broker, network, load-balancer, etc.).        | librdkafka_client_id, application_id,broker_id  |

**Sensor : librdkafka.[LIBRDKAFKA_CLIENT_ID].sensor.broker-internal-queue-latency-micros, Type : librdkafka-producer-metrics**

| Metric name      | Description | Tags          |
| :---             |    :----:   |          ---: |
| <span style="color: #06389C; font-weight: bold;">broker-internal-queue-latency-micros</span>        | Internal producer queue latency in microseconds.        | librdkafka_client_id, application_id,broker_id  |

**Sensor : librdkafka.[LIBRDKAFKA_CLIENT_ID].sensor.broker-internal-request-queue-latency-micros, Type : librdkafka-producer-metrics**

| Metric name      | Description | Tags          |
| :---             |    :----:   |          ---: |
| <span style="color: #06389C; font-weight: bold;">broker-internal-request-queue-latency-micros</span>        | Internal request queue latency in microseconds. This is the time between a request is enqueued on the transmit (outbuf) queue and the time the request is written to the TCP socket. Additional buffering and latency may be incurred by the TCP stack and network.        | librdkafka_client_id, application_id,broker_id  |

**Sensor : librdkafka.[LIBRDKAFKA_CLIENT_ID].sensor.broker-latency-avg-micros, Type : librdkafka-producer-metrics**

| Metric name      | Description | Tags          |
| :---             |    :----:   |          ---: |
| <span style="color: #06389C; font-weight: bold;">broker-latency-avg-micros</span>        | Broker latency / round-trip time in microseconds.        | librdkafka_client_id, application_id,broker_id  |

##### Topic metrics

**Sensor : librdkafka.[LIBRDKAFKA_CLIENT_ID].sensor.topic-batch-size-bytes-avg, Type : librdkafka-producer-metrics**

| Metric name      | Description | Tags          |
| :---             |    :----:   |          ---: |
| <span style="color: #06389C; font-weight: bold;">topic-batch-size-bytes-avg</span>        | Batch sizes in bytes average        | librdkafka_client_id, application_id,topic  |

**Sensor : librdkafka.[LIBRDKAFKA_CLIENT_ID].sensor.topic-batch-count-avg, Type : librdkafka-producer-metrics**

| Metric name      | Description | Tags          |
| :---             |    :----:   |          ---: |
| <span style="color: #06389C; font-weight: bold;">topic-batch-count-avg
</span>        | Batch message counts average        | librdkafka_client_id, application_id,topic  |

##### Partition metrics

**Sensor : librdkafka.[LIBRDKAFKA_CLIENT_ID].sensor.partition-messages-produced-total, Type : librdkafka-producer-metrics**

| Metric name      | Description | Tags          |
| :---             |    :----:   |          ---: |
| <span style="color: #06389C; font-weight: bold;">partition-messages-produced-total</span>        | Total number of messages transmitted (produced)        | librdkafka_client_id, application_id,partition_id  |

**Sensor : librdkafka.[LIBRDKAFKA_CLIENT_ID].sensor.partition-bytes-produced-total, Type : librdkafka-producer-metrics**

| Metric name      | Description | Tags          |
| :---             |    :----:   |          ---: |
| <span style="color: #06389C; font-weight: bold;">partition-bytes-produced-total</span>        | Total number of bytes transmitted for txmsgs        | librdkafka_client_id, application_id,partition_id  |

**Sensor : librdkafka.[LIBRDKAFKA_CLIENT_ID].sensor.partition-messages-in-flight-total, Type : librdkafka-producer-metrics**

| Metric name      | Description | Tags          |
| :---             |    :----:   |          ---: |
| <span style="color: #06389C; font-weight: bold;">partition-messages-in-flight-total</span>        | Current number of messages in-flight to/from broker        | librdkafka_client_id, application_id,partition_id  |

**Sensor : librdkafka.[LIBRDKAFKA_CLIENT_ID].sensor.partition-next-expected-ack, Type : librdkafka-producer-metrics**

| Metric name      | Description | Tags          |
| :---             |    :----:   |          ---: |
| <span style="color: #06389C; font-weight: bold;">partition-next-expected-ack</span>        | Next expected acked sequence (idempotent producer)        | librdkafka_client_id, application_id,partition_id  |

**Sensor : librdkafka.[LIBRDKAFKA_CLIENT_ID].sensor.partition-last-message-id-acked, Type : librdkafka-producer-metrics**

| Metric name      | Description | Tags          |
| :---             |    :----:   |          ---: |
| <span style="color: #06389C; font-weight: bold;">partition-last-message-id-acked</span>        | Last acked internal message id (idempotent producer)        | librdkafka_client_id, application_id,partition_id  |