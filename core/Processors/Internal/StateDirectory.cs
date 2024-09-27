using System;
using System.IO;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Streamiz.Kafka.Net.Crosscutting;

namespace Streamiz.Kafka.Net.Processors.Internal
{
    /// <summary>
    /// Manages the directories where the state of Tasks owned by a <see cref="StreamThread"/> are stored.
    /// This class is thread-safe.
    /// </summary>
    internal class StateDirectory
    {
        private static StateDirectory staticDirectoryInstance;

        private static ILogger log = Logger.GetLogger(typeof(StateDirectory));
        private bool hasPersistentStores;
        private IStreamConfig config;

        internal StateDirectory(IStreamConfig streamConfig, bool hasPersistentStores)
        {
            this.hasPersistentStores = hasPersistentStores;
            config = streamConfig;
        }

        private const String PROCESS_FILE_NAME = "streamiz-process-metadata";
        
        private class StateDirectoryProcessFile {
            
            [JsonProperty("processId")]
            public  Guid ProcessId { get; set; }

            public StateDirectoryProcessFile(Guid processId)
            {
                ProcessId = processId;
            }

            public StateDirectoryProcessFile() {
                
            }
        }

        public static StateDirectory GetInstance(IStreamConfig config, bool hasPersistentStores)
        {
            if (staticDirectoryInstance == null)
                staticDirectoryInstance = new StateDirectory(config, hasPersistentStores);

            return staticDirectoryInstance;
        }
        
        [MethodImpl(MethodImplOptions.Synchronized)]
        public Guid InitializeProcessId()
        {
            if (!hasPersistentStores)
            {
                Guid processId = Guid.NewGuid();
                log.LogInformation($"Created new process id: {processId}");
                return processId;
            }

            string path = Path.Combine(config.StateDir, PROCESS_FILE_NAME);
            if (File.Exists(path))
            {
                try
                {
                    string jsonString = File.ReadAllText(path);
                    StateDirectoryProcessFile processFile =
                        JsonConvert.DeserializeObject<StateDirectoryProcessFile>(jsonString);
                    log.LogInformation($"Reading UUID from process file: {processFile.ProcessId}");
                    return processFile.ProcessId;
                }
                catch (Exception e)
                {
                    log.LogWarning($"Failed to read json process file : {e.Message}");
                }
            }

            StateDirectoryProcessFile processFileData = new StateDirectoryProcessFile(Guid.NewGuid());
            log.LogInformation($"No process id found on disk, got fresh process id {processFileData.ProcessId}");
            
            File.WriteAllText(path, JsonConvert.SerializeObject(processFileData));
            return processFileData.ProcessId;
        }
    }
}