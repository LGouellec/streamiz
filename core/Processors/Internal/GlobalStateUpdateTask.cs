using Confluent.Kafka;
using log4net;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Stream.Internal;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Streamiz.Kafka.Net.Processors.Internal
{
    internal class GlobalStateUpdateTask : IGlobalStateMaintainer
    {
        private readonly IGlobalStateManager globalStateManager;
        private readonly ProcessorTopology topology;
        private readonly ILog log = Logger.GetLogger(typeof(GlobalStateUpdateTask));
        private readonly ProcessorContext context;
        private readonly IDictionary<TopicPartition, long> offsets = new Dictionary<TopicPartition, long>();
        private IDictionary<string, IProcessor> topicToProcessor = new Dictionary<string, IProcessor>();

        public GlobalStateUpdateTask(IGlobalStateManager globalStateManager, ProcessorTopology topology, ProcessorContext context)
        {
            this.globalStateManager = globalStateManager;
            this.topology = topology;
            this.context = context;
        }

        public void Close()
        {
            this.globalStateManager.Close();
        }

        public void FlushState()
        {
            // TODO
            this.globalStateManager.Flush();
            //this.globalStateManager.Checkpoint(this.offsets);
        }

        public IDictionary<TopicPartition, long> Initialize()
        {
            this.topicToProcessor =
                this.globalStateManager
                .Initialize()
                .Select(storeName => this.topology.StoresToTopics[storeName])
                .ToDictionary(
                    topic => topic,
                    topic => this.topology.SourceOperators.Single(x => (x.Value as ISourceProcessor).TopicName == topic).Value);

            InitTopology();
            return this.globalStateManager.ChangelogOffsets;
        }

        public void Update(ConsumeResult<byte[], byte[]> record)
        {
            var processor = this.topicToProcessor[record.Topic];

            this.context.SetRecordMetaData(record);

            var recordInfo = $"Topic:{record.Topic}|Partition:{record.Partition.Value}|Offset:{record.Offset}|Timestamp:{record.Message.Timestamp.UnixTimestampMs}";
            log.Debug($"Start processing one record [{recordInfo}]");
            processor.Process(record.Message.Key, record.Message.Value);
            log.Debug($"Completed processing one record [{recordInfo}]");

            // TODO: java implementation uses record.Offset + 1
            // why the difference? (StreamTask.Process does same thing as below)
            offsets[record.TopicPartition] = record.Offset;

            // TODO: Commit? Or maybe GlobalStreamThread calling FlushState() does it already?
        }

        private void InitTopology()
        {
            foreach (var processor in this.topology.ProcessorOperators.Values)
            {
                log.Debug($"Initializing topology with processor source : {processor}.");
                processor.Init(this.context);
            }
        }
    }
}
