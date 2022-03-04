using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using Streamiz.Kafka.Net.Metrics.Internal;
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

        private IDictionary<string, IList<string>> threadScopeSensors = new Dictionary<string, IList<string>>();

        private readonly string clientId;
        private readonly MetricsRecordingLevel recordingLevel;
        
        #region Constants
        
        private static readonly string SENSOR_PREFIX_DELIMITER = ".";
        private static readonly string SENSOR_NAME_DELIMITER = ".sensor.";
        private static readonly string SENSOR_TASK_LABEL = "task";
        private static readonly string SENSOR_NODE_LABEL = "node";
        private static readonly string SENSOR_STORE_LABEL = "store";
        private static readonly string SENSOR_ENTITY_LABEL = "entity";
        private static readonly string SENSOR_EXTERNAL_LABEL = "external";
        private static readonly string SENSOR_INTERNAL_LABEL = "internal";
        
        public static readonly string CLIENT_ID_TAG = "client-id";
        public static readonly string APPLICATION_ID_TAG = "application-id";
        public static readonly string THREAD_ID_TAG = "thread-id";
        public static readonly string TASK_ID_TAG = "task-id";
        public static readonly string PROCESSOR_NODE_ID_TAG = "processor-node-id";
        public static readonly string STORE_ID_TAG = "state-id";
        
        
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
                string key = ThreadSensorPrefix(threadId);
                var sensor = GetSensor(threadLevelSensors, sensorName, key, description, metricsRecordingLevel, parents);
                AddSensorThreadScope(threadId, sensor.Name);
                return sensor;
            }
        }

        internal IDictionary<string, string> ThreadLevelTags(string threadId)
        {
            var tagDic = new Dictionary<string, string>();
            tagDic.Add(THREAD_ID_TAG, threadId);
            return tagDic;
        }

        private string ThreadSensorPrefix(string threadId)
            => $"{SENSOR_INTERNAL_LABEL}{SENSOR_PREFIX_DELIMITER}{threadId}";
        
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
                string key = TaskSensorPrefix(threadId, taskId.ToString());
                var sensor = GetSensor(taskLevelSensors, sensorName, key, description, metricsRecordingLevel, parents);
                AddSensorThreadScope(threadId, sensor.Name);
                return sensor;
            }    
        }

        internal IDictionary<string, string> TaskLevelTags(string threadId, string taskId)
        {
            var tagDic = ThreadLevelTags(threadId);
            tagDic.Add(TASK_ID_TAG, taskId);
            return tagDic;
        }
        
        private string TaskSensorPrefix(string threadId, string taskId)
            => $"{ThreadSensorPrefix(threadId)}{SENSOR_PREFIX_DELIMITER}{SENSOR_TASK_LABEL}{SENSOR_PREFIX_DELIMITER}{taskId}";
        
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
                string key = NodeSensorPrefix(threadId, taskId.ToString(), processorName);
                var sensor = GetSensor(nodeLevelSensors, sensorName, key, description, metricsRecordingLevel, parents);
                AddSensorThreadScope(threadId, sensor.Name);
                return sensor;
            }   
        }

        internal IDictionary<string, string> NodeLevelTags(string threadId, string taskId, string processorName)
        {
            var tagDic = TaskLevelTags(threadId, taskId);
            tagDic.Add(PROCESSOR_NODE_ID_TAG, processorName);
            return tagDic;
        }
        
        private string NodeSensorPrefix(string threadId, string taskId, string processorName)
            => $"{TaskSensorPrefix(threadId, taskId)}{SENSOR_PREFIX_DELIMITER}{SENSOR_NODE_LABEL}{SENSOR_PREFIX_DELIMITER}{processorName}";
        
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
                string key = StoreSensorPrefix(threadId, taskId.ToString(), storeName);
                var sensor = GetSensor(storeLevelSensors, sensorName, key, description, metricsRecordingLevel, parents);
                AddSensorThreadScope(threadId, sensor.Name);
                return sensor;
            }   
        }

        internal IDictionary<string, string> StoreLevelTags(string threadId, string taskId, string storeName, string storeType)
        {
            var tagDic = TaskLevelTags(threadId, taskId);
            tagDic.Add($"{storeType}-{STORE_ID_TAG}", storeName);
            return tagDic;
        }
        
        private string StoreSensorPrefix(string threadId, string taskId, string storeName)
            => $"{TaskSensorPrefix(threadId, taskId)}{SENSOR_PREFIX_DELIMITER}{SENSOR_STORE_LABEL}{SENSOR_PREFIX_DELIMITER}{storeName}";
        
        #endregion
        
        #region Helpers

        private bool TestMetricsRecordingLevel(MetricsRecordingLevel metricsRecordingLevel)
            => recordingLevel.CompareTo(metricsRecordingLevel) >= 0;

        private Sensor GetSensor(
            IDictionary<string, IList<string>> listSensors,
            string sensorName,
            string key,
            string description,
            MetricsRecordingLevel metricsRecordingLevel,
            params Sensor[] parents)
        {
            string fullSensorName = $"{key}{SENSOR_NAME_DELIMITER}{sensorName}";
            Sensor sensor = GetSensor(fullSensorName, description, metricsRecordingLevel, parents);
            if (!listSensors.ContainsKey(key))
                listSensors.Add(key, new List<string> { fullSensorName});
            else if(!listSensors[key].Contains(fullSensorName))
                listSensors[key].Add(fullSensorName);
            return sensor;
        }

        private Sensor GetSensor(
            string name,
            string description,
            MetricsRecordingLevel metricsRecordingLevel,
            params Sensor[] parents)
        {
            if (sensors.ContainsKey(name))
                return sensors[name];
            else
            {
                Sensor sensor = null;
                if (TestMetricsRecordingLevel(metricsRecordingLevel))
                    sensor = new Sensor(name, description, metricsRecordingLevel);
                else
                    sensor = new NoRunnableSensor(name, description, metricsRecordingLevel);
                
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
            => new ReadOnlyCollection<Sensor>(sensors.Values.ToList());

        public IEnumerable<Sensor> GetThreadScopeSensor(string threadId)
        {
            var sensors = new List<Sensor>();
            
            foreach (var s in clientLevelSensors)
                sensors.Add(this.sensors[s]);

            if (threadScopeSensors.ContainsKey(threadId))
            {
                foreach (var s in threadScopeSensors[threadId])
                    sensors.Add(this.sensors[s]);
            }

            return sensors;
        }
        /*** GET METRICS ***/

        // TODO: add sensor
        
        #endregion
    }
}