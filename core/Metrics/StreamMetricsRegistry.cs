using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Reflection;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Processors.Internal;

namespace Streamiz.Kafka.Net.Metrics
{
    public class StreamMetricsRegistry
    {
        private IDictionary<string, Sensor> sensors = new Dictionary<string, Sensor>();
        private IList<string> clientLevelSensors = new List<string>();
        private IDictionary<string, IList<string>> threadLevelSensors = new Dictionary<string, IList<string>>();
        private IDictionary<string, IList<string>> taskLevelSensors = new Dictionary<string, IList<string>>();
        private IDictionary<string, IList<string>> nodeLevelSensors = new Dictionary<string, IList<string>>();
        private IDictionary<string, IList<string>> storeLevelSensors = new Dictionary<string, IList<string>>();
        private IDictionary<string, IList<string>> librdkafkaSensors = new Dictionary<string, IList<string>>();

        private IDictionary<string, IList<string>> threadScopeSensors = new Dictionary<string, IList<string>>();

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
        
        public static readonly string CLIENT_ID_TAG = "client_id";
        public static readonly string LIBRDKAFKA_CLIENT_ID_TAG = "librdkafka_client_id";
        public static readonly string APPLICATION_ID_TAG = "application_id";
        public static readonly string THREAD_ID_TAG = "thread_id";
        public static readonly string TASK_ID_TAG = "task_id";
        public static readonly string PROCESSOR_NODE_ID_TAG = "processor_node_id";
        public static readonly string STORE_ID_TAG = "state_id";
        
        
        public static readonly string LATENCY_SUFFIX = "-latency";
        public static readonly string RECORDS_SUFFIX = "-records";
        public static readonly string AVG_SUFFIX = "-avg";
        public static readonly string MAX_SUFFIX = "-max";
        public static readonly string MIN_SUFFIX = "-min";
        public static readonly string RATE_SUFFIX = "-rate";
        public static readonly string TOTAL_SUFFIX = "-total";
        public static readonly string RATIO_SUFFIX = "-ratio";

        public static readonly string GROUP_PREFIX_WO_DELIMITER = "stream";
        public static readonly string GROUP_PREFIX = GROUP_PREFIX_WO_DELIMITER + "-";
        public static readonly string GROUP_SUFFIX = "-metrics";
        public static readonly string CLIENT_LEVEL_GROUP = GROUP_PREFIX_WO_DELIMITER + GROUP_SUFFIX;
        public static readonly string THREAD_LEVEL_GROUP = GROUP_PREFIX + "thread" + GROUP_SUFFIX;
        public static readonly string LIBRDKAFKA_CONSUMER_LEVEL_GROUP = GROUP_PREFIX + "librdkafka-consumer" + GROUP_SUFFIX;
        public static readonly string LIBRDKAFKA_PRODUCER_LEVEL_GROUP = GROUP_PREFIX + "librdkafka-producer" + GROUP_SUFFIX;
        public static readonly string TASK_LEVEL_GROUP = GROUP_PREFIX + "task" + GROUP_SUFFIX;
        public static readonly string PROCESSOR_NODE_LEVEL_GROUP = GROUP_PREFIX + "processor-node" + GROUP_SUFFIX;
        public static readonly string STATE_STORE_LEVEL_GROUP = GROUP_PREFIX + "state" + GROUP_SUFFIX;
        
        public static readonly string OPERATIONS = " operations";
        public static readonly string TOTAL_DESCRIPTION = "The total number of ";
        public static readonly string RATE_DESCRIPTION = "The average per-second number of ";
        public static readonly string AVG_LATENCY_DESCRIPTION = "The average latency of ";
        public static readonly string MAX_LATENCY_DESCRIPTION = "The maximum latency of ";
        public static readonly string RATE_DESCRIPTION_PREFIX = "The average number of ";
        public static readonly string RATE_DESCRIPTION_SUFFIX = " per second";
        
        #endregion

        public StreamMetricsRegistry() 
            : this(string.Empty, MetricsRecordingLevel.INFO)
        {
        }

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
            tagDic.Add($"{storeType}_{STORE_ID_TAG}", storeName);
            return tagDic;
        }

        internal string StoreSensorPrefix(string threadId, string taskId, string storeName)
        {
            threadId ??= UNKNOWN_THREAD;
            return $"{TaskSensorPrefix(threadId, taskId)}{SENSOR_PREFIX_DELIMITER}{SENSOR_STORE_LABEL}{SENSOR_PREFIX_DELIMITER}{storeName}";
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
        {
            lock (librdkafkaSensors)
            {
                threadId ??= UNKNOWN_THREAD;
                string key = LibrdKafkaSensorPrefix(librdKafkaClientId);
                var sensor = GetSensor(librdkafkaSensors, sensorName, key, description, metricsRecordingLevel, parents);
                AddSensorThreadScope(threadId, sensor.Name);
                return sensor;
            }   
        }
        
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
            
            if (!listSensors.ContainsKey(key))
                listSensors.Add(key, new List<string> { fullSensorName});
            else if(!listSensors[key].Contains(fullSensorName))
                listSensors[key].Add(fullSensorName);
            
            return sensor;
        }

        private T GetSensor<T>(
            string name,
            string description,
            MetricsRecordingLevel metricsRecordingLevel,
            params Sensor[] parents)
            where T : Sensor
        {
            if (sensors.ContainsKey(name) && sensors[name] is T)
                return (T)sensors[name];
            else
            {
                BindingFlags flags = BindingFlags.NonPublic | BindingFlags.Instance;
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
                
                sensors.Add(name, sensor);
                return sensor;   
                
            }
        }

        private void AddSensorThreadScope(string threadId, string fullSensorName)
        {
            if (!threadScopeSensors.ContainsKey(threadId))
                threadScopeSensors.Add(threadId, new List<string> { fullSensorName});
            else if(!threadScopeSensors[threadId].Contains(fullSensorName))
                threadScopeSensors[threadId].Add(fullSensorName);
        }
        
        #endregion
        
        #region Public API
        
        /*** GET METRICS ***/
        public IEnumerable<Sensor> GetFullScope()
            => new ReadOnlyCollection<Sensor>(sensors.Values.Where(s => TestMetricsRecordingLevel(s.MetricsRecording)).ToList());

        public IEnumerable<Sensor> GetThreadScopeSensor(string threadId)
        {
            var sensors = new List<Sensor>();

            foreach (var s in clientLevelSensors)
            {
                var sensor = this.sensors[s];
                if(TestMetricsRecordingLevel(sensor.MetricsRecording)) 
                    sensors.Add(sensor);
            }

            if (threadScopeSensors.ContainsKey(threadId))
            {
                foreach (var s in threadScopeSensors[threadId])
                {
                    var sensor = this.sensors[s];
                    if(TestMetricsRecordingLevel(sensor.MetricsRecording)) 
                        sensors.Add(sensor);
                }
            }

            return sensors;
        }
        /*** GET METRICS ***/

        // TODO: add sensor
        
        #endregion
    }
}