using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Reflection;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Metrics.Internal;
using Streamiz.Kafka.Net.Processors.Internal;

namespace Streamiz.Kafka.Net.Metrics
{
    /// <summary>
    /// Steamiz metrics registry for adding metric sensors and collecting metric values.
    /// </summary>
    public class StreamMetricsRegistry
    {
        private ConcurrentDictionary<string, Sensor> sensors = new();
        private IList<string> clientLevelSensors = new List<string>();
        private ConcurrentDictionary<string, IList<string>> threadLevelSensors = new();
        private ConcurrentDictionary<string, IList<string>> taskLevelSensors = new();
        private ConcurrentDictionary<string, IList<string>> nodeLevelSensors = new();
        private ConcurrentDictionary<string, IList<string>> storeLevelSensors = new();
        private ConcurrentDictionary<string, IList<string>> librdkafkaSensors = new();

        private ConcurrentDictionary<string, IList<string>> threadScopeSensors = new();

        private readonly string clientId;
        private readonly MetricsRecordingLevel recordingLevel;
        
        #region Constants
        
        internal static readonly string SENSOR_PREFIX_DELIMITER = ".";
        internal static readonly string SENSOR_NAME_DELIMITER = ".sensor.";
        internal static readonly string SENSOR_TASK_LABEL = "task";
        internal static readonly string SENSOR_NODE_LABEL = "node";
        internal static readonly string SENSOR_STORE_LABEL = "store";
        internal static readonly string SENSOR_ENTITY_LABEL = "entity";
        internal static readonly string SENSOR_EXTERNAL_LABEL = "external";
        internal static readonly string SENSOR_INTERNAL_LABEL = "internal";
        internal static readonly string SENSOR_LIBRDKAFKA_LABEL = "librdkafka";
        // FOR TESTING
        internal static readonly string UNKNOWN_THREAD = "unknown-thread";
        
        /// <summary>
        /// Client Id tag
        /// </summary>
        public static readonly string CLIENT_ID_TAG = "client_id";
        /// <summary>
        /// Librdkafka Client Id tag
        /// </summary>
        public static readonly string LIBRDKAFKA_CLIENT_ID_TAG = "librdkafka_client_id";
        /// <summary>
        /// Application Id tag
        /// </summary>
        public static readonly string APPLICATION_ID_TAG = "application_id";
        /// <summary>
        /// Thread Id tag
        /// </summary>
        public static readonly string THREAD_ID_TAG = "thread_id";
        /// <summary>
        /// Task Id tag
        /// </summary>
        public static readonly string TASK_ID_TAG = "task_id";
        /// <summary>
        /// Processor Node Id tag
        /// </summary>
        public static readonly string PROCESSOR_NODE_ID_TAG = "processor_node_id";
        /// <summary>
        /// State id tag
        /// </summary>
        public static readonly string STORE_ID_TAG = "state_id";

        /// <summary>
        /// Latency suffix
        /// </summary>
        public static readonly string LATENCY_SUFFIX = "-latency";
        /// <summary>
        /// Records suffix
        /// </summary>
        public static readonly string RECORDS_SUFFIX = "-records";
        /// <summary>
        /// Average suffix
        /// </summary>
        public static readonly string AVG_SUFFIX = "-avg";
        /// <summary>
        /// Max suffix
        /// </summary>
        public static readonly string MAX_SUFFIX = "-max";
        /// <summary>
        /// Min suffix
        /// </summary>
        public static readonly string MIN_SUFFIX = "-min";
        /// <summary>
        /// Rate suffix
        /// </summary>
        public static readonly string RATE_SUFFIX = "-rate";
        /// <summary>
        /// Total suffix
        /// </summary>
        public static readonly string TOTAL_SUFFIX = "-total";
        /// <summary>
        /// Ratio suffix
        /// </summary>
        public static readonly string RATIO_SUFFIX = "-ratio";

        /// <summary>
        /// Logical group prefix
        /// </summary>
        public static readonly string GROUP_PREFIX_WO_DELIMITER = "stream";
        /// <summary>
        /// Logical group prefix
        /// </summary>
        public static readonly string GROUP_PREFIX = GROUP_PREFIX_WO_DELIMITER + "-";
        /// <summary>
        /// Logical group suffix
        /// </summary>
        public static readonly string GROUP_SUFFIX = "-metrics";
        /// <summary>
        /// Client Logical group
        /// </summary>
        public static readonly string CLIENT_LEVEL_GROUP = GROUP_PREFIX_WO_DELIMITER + GROUP_SUFFIX;
        /// <summary>
        /// Thread Logical group
        /// </summary>
        public static readonly string THREAD_LEVEL_GROUP = GROUP_PREFIX + "thread" + GROUP_SUFFIX;
        /// <summary>
        /// Librdkafka consumer Logical group
        /// </summary>
        public static readonly string LIBRDKAFKA_CONSUMER_LEVEL_GROUP = GROUP_PREFIX + "librdkafka-consumer" + GROUP_SUFFIX;
        /// <summary>
        /// Librdkafka producer Logical group
        /// </summary>
        public static readonly string LIBRDKAFKA_PRODUCER_LEVEL_GROUP = GROUP_PREFIX + "librdkafka-producer" + GROUP_SUFFIX;
        /// <summary>
        /// Task Logical group
        /// </summary>
        public static readonly string TASK_LEVEL_GROUP = GROUP_PREFIX + "task" + GROUP_SUFFIX;
        /// <summary>
        /// Processor node Logical group
        /// </summary>
        public static readonly string PROCESSOR_NODE_LEVEL_GROUP = GROUP_PREFIX + "processor-node" + GROUP_SUFFIX;
        /// <summary>
        /// State store Logical group
        /// </summary>
        public static readonly string STATE_STORE_LEVEL_GROUP = GROUP_PREFIX + "state" + GROUP_SUFFIX;
        
        /// <summary>
        /// Operation constant
        /// </summary>
        public static readonly string OPERATIONS = " operations";
        /// <summary>
        /// Total description
        /// </summary>
        public static readonly string TOTAL_DESCRIPTION = "The total number of ";
        /// <summary>
        /// Rate description
        /// </summary>
        public static readonly string RATE_DESCRIPTION = "The average per-second number of ";
        /// <summary>
        /// Avg latency description
        /// </summary>
        public static readonly string AVG_LATENCY_DESCRIPTION = "The average latency of ";
        /// <summary>
        /// Maximum latency description
        /// </summary>
        public static readonly string MAX_LATENCY_DESCRIPTION = "The maximum latency of ";
        /// <summary>
        /// Rate description prefix
        /// </summary>
        public static readonly string RATE_DESCRIPTION_PREFIX = "The average number of ";
        /// <summary>
        /// Rate description suffix
        /// </summary>
        public static readonly string RATE_DESCRIPTION_SUFFIX = " per second";
        
        #endregion

        internal RocksDbMetricsRecordingTrigger RocksDbMetricsRecordingTrigger { get; } = new();
        
        /// <summary>
        /// Create stream metrics registry in INFO level with a empty cliend Id.
        /// </summary>
        internal StreamMetricsRegistry() 
            : this(string.Empty, MetricsRecordingLevel.INFO)
        {
        }

        /// <summary>
        /// Create stream metrics registry
        /// </summary>
        /// <param name="clientId">Client Id derived from <see cref="IStreamConfig.ApplicationId"/></param>
        /// <param name="metricsRecordingLevel">Metrics recording level derived from <see cref="IStreamConfig.MetricsRecording"/></param>
        public StreamMetricsRegistry(
            string clientId,
            MetricsRecordingLevel metricsRecordingLevel)
        {
            this.clientId = clientId;
            recordingLevel = metricsRecordingLevel;
        }
        
        #region Client Level Metrics

        internal Sensor ClientLevelSensor(
            string sensorName,
            string description,
            MetricsRecordingLevel metricsRecordingLevel,
            params Sensor[] parents)
        {
            lock (clientLevelSensors)
            {
                string fullSensorName = $"{CLIENT_LEVEL_GROUP}{SENSOR_NAME_DELIMITER}{sensorName}";
                Sensor sensor = GetSensor(fullSensorName, description, metricsRecordingLevel, parents);
                if (!clientLevelSensors.Contains(fullSensorName))
                    clientLevelSensors.Add(fullSensorName);
                return sensor;
            }
        }
        
        internal IDictionary<string, string> ClientTags()
        {
            var tagDic = new Dictionary<string, string>();
            tagDic.Add(CLIENT_ID_TAG, clientId);
            return tagDic;
        }

        internal void RemoveClientSensors()
        {
            lock (clientLevelSensors)
                clientLevelSensors.Clear();
        }

        #endregion
        
        #region Thread Level Metrics

        internal Sensor ThreadLevelSensor(
            string threadId,
            string sensorName,
            string description,
            MetricsRecordingLevel metricsRecordingLevel,
            params Sensor[] parents)
        {
            lock (threadLevelSensors)
            {
                threadId ??= UNKNOWN_THREAD;
                string key = ThreadSensorPrefix(threadId);
                var sensor = GetSensor(threadLevelSensors, sensorName, key, description, metricsRecordingLevel, parents);
                AddSensorThreadScope(threadId, sensor.Name);
                return sensor;
            }
        }

        internal IDictionary<string, string> ThreadLevelTags(string threadId)
        {
            threadId ??= UNKNOWN_THREAD;
            var tagDic = new Dictionary<string, string>();
            tagDic.Add(THREAD_ID_TAG, threadId);
            return tagDic;
        }

        internal string ThreadSensorPrefix(string threadId)
        {
            threadId ??= UNKNOWN_THREAD;
            return $"{SENSOR_INTERNAL_LABEL}{SENSOR_PREFIX_DELIMITER}{threadId}";
        }

        internal void RemoveThreadSensors(string threadId)
        {
            lock (threadLevelSensors)
            {
                var key = ThreadSensorPrefix(threadId);
                if (threadLevelSensors.TryGetValue(key, out var sensorKeys))
                {
                    sensors.RemoveAll(sensorKeys);
                    threadScopeSensors[threadId].RemoveAll(sensorKeys);
                    threadLevelSensors.TryRemove(key, out _);
                }
            }
        }
        
        #endregion
        
        #region Task Level Metrics

        internal Sensor TaskLevelSensor(
            string threadId,
            TaskId taskId,
            string sensorName,
            string description,
            MetricsRecordingLevel metricsRecordingLevel,
            params Sensor[] parents)
        {
            lock (taskLevelSensors)
            {
                threadId ??= UNKNOWN_THREAD;
                string key = TaskSensorPrefix(threadId, taskId.ToString());
                var sensor = GetSensor(taskLevelSensors, sensorName, key, description, metricsRecordingLevel, parents);
                AddSensorThreadScope(threadId, sensor.Name);
                return sensor;
            }    
        }

        internal IDictionary<string, string> TaskLevelTags(string threadId, string taskId)
        {
            threadId ??= UNKNOWN_THREAD;
            var tagDic = ThreadLevelTags(threadId);
            tagDic.Add(TASK_ID_TAG, taskId);
            return tagDic;
        }

        internal string TaskSensorPrefix(string threadId, string taskId)
        {
            threadId ??= UNKNOWN_THREAD;
            return $"{ThreadSensorPrefix(threadId)}{SENSOR_PREFIX_DELIMITER}{SENSOR_TASK_LABEL}{SENSOR_PREFIX_DELIMITER}{taskId}";
        }

        internal void RemoveTaskSensors(string threadId, string taskId)
        {
            lock (taskLevelSensors)
            {
                var key = TaskSensorPrefix(threadId, taskId);
                if (taskLevelSensors.TryGetValue(key, out var sensorKeys))
                {
                    sensors.RemoveAll(sensorKeys);
                    threadScopeSensors[threadId].RemoveAll(sensorKeys);
                    taskLevelSensors.TryRemove(key, out _);
                }
            }
        }

        #endregion
        
        #region Node Level metrics

        internal Sensor NodeLevelSensor(
            string threadId,
            TaskId taskId,
            string processorName,
            string sensorName,
            string description,
            MetricsRecordingLevel metricsRecordingLevel,
            params Sensor[] parents)
        {
            lock (nodeLevelSensors)
            {
                threadId ??= UNKNOWN_THREAD;
                string key = NodeSensorPrefix(threadId, taskId.ToString(), processorName);
                var sensor = GetSensor(nodeLevelSensors, sensorName, key, description, metricsRecordingLevel, parents);
                AddSensorThreadScope(threadId, sensor.Name);
                return sensor;
            }   
        }

        internal IDictionary<string, string> NodeLevelTags(string threadId, string taskId, string processorName)
        {
            threadId ??= UNKNOWN_THREAD;
            var tagDic = TaskLevelTags(threadId, taskId);
            tagDic.Add(PROCESSOR_NODE_ID_TAG, processorName);
            return tagDic;
        }

        internal string NodeSensorPrefix(string threadId, string taskId, string processorName)
        {
            threadId ??= UNKNOWN_THREAD;
            return $"{TaskSensorPrefix(threadId, taskId)}{SENSOR_PREFIX_DELIMITER}{SENSOR_NODE_LABEL}{SENSOR_PREFIX_DELIMITER}{processorName}";
        }
        
        internal void RemoveNodeSensors(string threadId, string taskId, string processorName)
        {
            threadId ??= StreamMetricsRegistry.UNKNOWN_THREAD;
            lock (nodeLevelSensors)
            {
                var key = NodeSensorPrefix(threadId, taskId, processorName);
                if (nodeLevelSensors.TryGetValue(key, out var sensorKeys))
                {
                    sensors.RemoveAll(sensorKeys);
                    threadScopeSensors[threadId].RemoveAll(sensorKeys);
                    nodeLevelSensors.TryRemove(key, out _);
                }
            }
        }

        #endregion
        
        #region State Level Metrics

        internal Sensor StoreLevelSensor(
            string threadId,
            TaskId taskId,
            string storeName,
            string sensorName,
            string description,
            MetricsRecordingLevel metricsRecordingLevel,
            params Sensor[] parents)
        {
            lock (storeLevelSensors)
            {
                threadId ??= UNKNOWN_THREAD;
                string key = StoreSensorPrefix(threadId, taskId.ToString(), storeName);
                var sensor = GetSensor(storeLevelSensors, sensorName, key, description, metricsRecordingLevel, parents);
                AddSensorThreadScope(threadId, sensor.Name);
                return sensor;
            }   
        }

        internal IDictionary<string, string> StoreLevelTags(string threadId, string taskId, string storeName, string storeType)
        {
            threadId ??= UNKNOWN_THREAD;
            var tagDic = TaskLevelTags(threadId, taskId);
            tagDic.Add($"{storeType.Replace("-", "_")}_{STORE_ID_TAG}", storeName);
            return tagDic;
        }

        internal string StoreSensorPrefix(string threadId, string taskId, string storeName)
        {
            threadId ??= UNKNOWN_THREAD;
            return $"{TaskSensorPrefix(threadId, taskId)}{SENSOR_PREFIX_DELIMITER}{SENSOR_STORE_LABEL}{SENSOR_PREFIX_DELIMITER}{storeName}";
        }
        
        internal void RemoveStoreSensors(string threadId, string taskId, string storeName)
        {
            threadId ??= StreamMetricsRegistry.UNKNOWN_THREAD;
            lock (storeLevelSensors)
            {
                var key = StoreSensorPrefix(threadId, taskId, storeName);
                if (storeLevelSensors.TryGetValue(key, out var sensorKeys))
                {
                    sensors.RemoveAll(sensorKeys);
                    threadScopeSensors[threadId].RemoveAll(sensorKeys);
                    storeLevelSensors.TryRemove(key, out _);
                }
            }
        }

        #endregion
        
        #region Librdkafka Metrics

        internal Sensor LibrdKafkaSensor(
            string threadId,
            string librdKafkaClientId,
            string sensorName,
            string description,
            MetricsRecordingLevel metricsRecordingLevel,
            params Sensor[] parents)
            => LibrdKafkaSensor<Sensor>(threadId, librdKafkaClientId, sensorName, description, metricsRecordingLevel,
                parents);

        internal T LibrdKafkaSensor<T>(
            string threadId,
            string librdKafkaClientId,
            string sensorName,
            string description,
            MetricsRecordingLevel metricsRecordingLevel,
            params Sensor[] parents)
            where T : Sensor
        {
            lock (librdkafkaSensors)
            {
                threadId ??= UNKNOWN_THREAD;
                string key = LibrdKafkaSensorPrefix(librdKafkaClientId);
                var sensor = GetSensor<T>(librdkafkaSensors, sensorName, key, description, metricsRecordingLevel, parents);
                AddSensorThreadScope(threadId, sensor.Name);
                return sensor;
            }   
        }
        
        internal void RemoveLibrdKafkaSensors(string threadId, string librdKafkaClientId)
        {
            lock (librdkafkaSensors)
            {
                var key = LibrdKafkaSensorPrefix(librdKafkaClientId);
                if (librdkafkaSensors.TryGetValue(key, out var sensorKeys))
                {
                    sensors.RemoveAll(sensorKeys);
                    threadScopeSensors[threadId].RemoveAll(sensorKeys);
                    librdkafkaSensors.TryRemove(key, out _);
                }
            }
        }

        internal IDictionary<string, string> LibrdKafkaLevelTags(string threadId, string librdKafkaClientId, string streamsAppId)
        {
            var tagDic = ThreadLevelTags(threadId);
            tagDic.Add(LIBRDKAFKA_CLIENT_ID_TAG, librdKafkaClientId);
            tagDic.Add(APPLICATION_ID_TAG, streamsAppId);
            return tagDic;
        }

        internal string LibrdKafkaSensorPrefix(string librdKafkaClientId)
        {
            return $"{SENSOR_LIBRDKAFKA_LABEL}{SENSOR_PREFIX_DELIMITER}{librdKafkaClientId}";
        }
        
        #endregion

        #region Helpers

        private bool TestMetricsRecordingLevel(MetricsRecordingLevel metricsRecordingLevel)
            => recordingLevel.CompareTo(metricsRecordingLevel) >= 0;

        internal string FullSensorName(string sensorName, string key)
            => $"{key}{SENSOR_NAME_DELIMITER}{sensorName}";
        
        private Sensor GetSensor(
            IDictionary<string, IList<string>> listSensors,
            string sensorName,
            string key,
            string description,
            MetricsRecordingLevel metricsRecordingLevel,
            params Sensor[] parents)
            => GetSensor<Sensor>(listSensors, sensorName, key, description, metricsRecordingLevel, parents);

        private Sensor GetSensor(
            string name,
            string description,
            MetricsRecordingLevel metricsRecordingLevel,
            params Sensor[] parents)
            => GetSensor<Sensor>(name, description, metricsRecordingLevel, parents);

        private T GetSensor<T>(
            IDictionary<string, IList<string>> listSensors,
            string sensorName,
            string key,
            string description,
            MetricsRecordingLevel metricsRecordingLevel,
            params Sensor[] parents)
            where T : Sensor
        {
            string fullSensorName = FullSensorName(sensorName, key);
            T sensor = GetSensor<T>(fullSensorName, description, metricsRecordingLevel, parents);

            if (listSensors.TryGetValue(key, out var list))
                list.Add(fullSensorName);
            else
                listSensors.Add(key, new List<string> { fullSensorName });

            return sensor;
        }

        private T GetSensor<T>(
            string name,
            string description,
            MetricsRecordingLevel metricsRecordingLevel,
            params Sensor[] parents)
            where T : Sensor
        {
            if(sensors.TryGetValue(name, out var sensorObj) && sensorObj is T)
                return (T)sensors[name];
            else
            {
                T sensor = (T) (typeof(T)
                    .GetConstructor(
                        BindingFlags.NonPublic | BindingFlags.Instance,
                        null,
                        new Type[]
                        {
                            typeof(string),
                            typeof(string),
                            typeof(MetricsRecordingLevel)
                        }, null)
                    ?.Invoke(new object[] {name, description, metricsRecordingLevel}));

                if (sensor == null)
                    throw new StreamsException($"{typeof(T)} has no constructor with string, string, MetricsRecordingLevel parameters !");
                
                if (!TestMetricsRecordingLevel(metricsRecordingLevel))
                    sensor.NoRunnable = true;
                
                sensors.TryAdd(name, sensor);
                return sensor;   
                
            }
        }

        private void AddSensorThreadScope(string threadId, string fullSensorName)
        {
            if (threadScopeSensors.TryGetValue(threadId, out var list))
                list.Add(fullSensorName);
            else
                threadScopeSensors.TryAdd(threadId, new List<string> { fullSensorName });
        }
        
        internal IEnumerable<Sensor> GetThreadScopeSensor(string threadId)
        {
            var sensors = new List<Sensor>();
            RocksDbMetricsRecordingTrigger.Run(DateTime.Now.GetMilliseconds());
            
            foreach (var s in clientLevelSensors)
            {
                var sensor = this.sensors[s];
                if(TestMetricsRecordingLevel(sensor.MetricsRecording)) 
                    sensors.Add(sensor);
            }

            if (threadScopeSensors.TryGetValue(threadId, out var list))
            {
                foreach (var s in list)
                {
                    var sensor = this.sensors[s];
                    if(TestMetricsRecordingLevel(sensor.MetricsRecording)) 
                        sensors.Add(sensor);
                }
            }

            return sensors;
        }
        
        #endregion
        
        #region Public API

        /// <summary>
        /// Get all sensors function the metrics recording level set in constructor.
        /// Return a <see cref="ReadOnlyCollection{T}"/> of sensors.
        /// For each sensor, you can use <see cref="Sensor.Metrics"/> to get the current value of each metric.
        /// </summary>
        /// <returns>Return a <see cref="ReadOnlyCollection{T}"/> of sensors.</returns>
        public IEnumerable<Sensor> GetSensors()
        {
            RocksDbMetricsRecordingTrigger.Run(DateTime.Now.GetMilliseconds());
            return new ReadOnlyCollection<Sensor>(sensors.Values.Where(s => TestMetricsRecordingLevel(s.MetricsRecording)).ToList());
        }
        
        #endregion
    }
}