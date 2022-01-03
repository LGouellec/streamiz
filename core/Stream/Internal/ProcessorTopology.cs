using Streamiz.Kafka.Net.Processors;
using System.Collections.Generic;
using System.Linq;

namespace Streamiz.Kafka.Net.Stream.Internal
{
    internal class ProcessorTopology
    {
        internal static readonly ProcessorTopology EMPTY = new ProcessorTopology(
            new RootProcessor(),
            new Dictionary<string, IProcessor>(),
            new Dictionary<string, IProcessor>(),
            new Dictionary<string, IProcessor>(),
            new Dictionary<string, IStateStore>(),
            new Dictionary<string, IStateStore>(),
            new Dictionary<string, string>(),
            new List<string>());

        internal IList<string> SourceProcessorNames => new List<string>(SourceOperators.Keys);
        internal IList<string> SinkProcessorNames => new List<string>(SinkOperators.Keys);
        internal IList<string> ProcessorNames => new List<string>(ProcessorOperators.Keys);


        internal IProcessor Root { get; }
        internal IDictionary<string, IProcessor> SourceOperators { get; } = new Dictionary<string, IProcessor>();
        internal IDictionary<string, IProcessor> SinkOperators { get; } = new Dictionary<string, IProcessor>();
        internal IDictionary<string, IProcessor> ProcessorOperators { get; } = new Dictionary<string, IProcessor>();
        internal IDictionary<string, IStateStore> StateStores { get; } = new Dictionary<string, IStateStore>();

        internal IDictionary<string, IStateStore> GlobalStateStores { get; }
        internal IDictionary<string, string> StoresToTopics { get; }
        internal IList<string> RepartitionTopics { get; }

        internal ProcessorTopology(IProcessor rootProcessor,
            IDictionary<string, IProcessor> sources,
            IDictionary<string, IProcessor> sinks,
            IDictionary<string, IProcessor> processors,
            IDictionary<string, IStateStore> stateStores,
            IDictionary<string, IStateStore> globalStateStores,
            IDictionary<string, string> storesToTopics,
            IList<string> repartitionTopics)
        {
            Root = rootProcessor;
            SourceOperators = sources;
            SinkOperators = sinks;
            ProcessorOperators = processors;
            StateStores = stateStores;
            GlobalStateStores = globalStateStores;
            StoresToTopics = storesToTopics;
            RepartitionTopics = repartitionTopics;
        }

        internal ISourceProcessor GetSourceProcessor(string name)
        {
            if (SourceOperators.ContainsKey(name))
                return SourceOperators[name] as ISourceProcessor;
            else
            {
                var processor = SourceOperators.FirstOrDefault(kp =>
                    kp.Value is ISourceProcessor && (kp.Value as ISourceProcessor).TopicName.Equals(name));
                return processor.Value as ISourceProcessor;
            }
        }

        internal IEnumerable<string> GetSourceTopics() => SourceOperators.Select(o => (o.Value as ISourceProcessor).TopicName);
    }
}
