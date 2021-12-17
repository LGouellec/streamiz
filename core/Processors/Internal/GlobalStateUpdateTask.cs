using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Stream.Internal;

namespace Streamiz.Kafka.Net.Processors.Internal
{
    internal class GlobalStateUpdateTask : IGlobalStateMaintainer
    {
        private readonly IGlobalStateManager globalStateManager;
        private readonly ProcessorTopology topology;
        private readonly ILogger log = Logger.GetLogger(typeof(GlobalStateUpdateTask));
        private readonly ProcessorContext context;
        private IDictionary<string, IProcessor> topicToProcessor = new Dictionary<string, IProcessor>();

        public GlobalStateUpdateTask(IGlobalStateManager globalStateManager, ProcessorTopology topology, ProcessorContext context)
        {
            this.globalStateManager = globalStateManager;
            this.topology = topology;
            this.context = context;
        }

        public void Close()
        {
            globalStateManager.Close();
        }

        public void FlushState()
        {
            globalStateManager.Flush();
        }

        public IDictionary<TopicPartition, long> Initialize()
        {
            topicToProcessor =
                globalStateManager
                .Initialize()
                .Select(storeName => topology.StoresToTopics[storeName])
                .ToDictionary(
                    topic => topic,
                    topic => topology.SourceOperators.Single(x => (x.Value as ISourceProcessor).TopicName == topic).Value);

            InitTopology();
            
            globalStateManager.InitializeOffsetsFromCheckpoint();
            
            return globalStateManager.ChangelogOffsets;
        }

        public void Update(ConsumeResult<byte[], byte[]> record)
        {
            var processor = topicToProcessor[record.Topic];

            context.SetRecordMetaData(record);

            var recordInfo = $"Topic:{record.Topic}|Partition:{record.Partition.Value}|Offset:{record.Offset}|Timestamp:{record.Message.Timestamp.UnixTimestampMs}";
            log.LogDebug("Start processing one record [{RecordInfo}]", recordInfo);
            processor.Process(record);
            log.LogDebug("Completed processing one record [{RecordInfo}]", recordInfo);
        }

        private void InitTopology()
        {
            foreach (var processor in topology.ProcessorOperators.Values)
            {
                log.LogDebug("Initializing topology with processor source : {Processor}", processor);
                processor.Init(context);
            }
        }
    }
}
