using Streamiz.Kafka.Net.Processors;
using System.Collections.Generic;
using System.Linq;

namespace Streamiz.Kafka.Net.Stream.Internal
{
    internal class ProcessorTopology
    {
        internal IList<string> SourceProcessorNames => new List<string>(SourceOperators.Keys);
        internal IList<string> SinkProcessorNames => new List<string>(SinkOperators.Keys);
        internal IList<string> ProcessorNames => new List<string>(ProcessorOperators.Keys);


        internal IProcessor Root { get; }
        internal IDictionary<string, IProcessor> SourceOperators { get; } = new Dictionary<string, IProcessor>();
        internal IDictionary<string, IProcessor> SinkOperators { get; } = new Dictionary<string, IProcessor>();
        internal IDictionary<string, IProcessor> ProcessorOperators { get; } = new Dictionary<string, IProcessor>();
        internal IDictionary<string, IStateStore> StateStores { get; } = new Dictionary<string, IStateStore>();


        internal ProcessorTopology(
            IProcessor rootProcessor,
            IDictionary<string, IProcessor> sources,
            IDictionary<string, IProcessor> sinks,
            IDictionary<string, IProcessor> processors,
            IDictionary<string, IStateStore> stateStores)
        {
            Root = rootProcessor;
            SourceOperators = sources;
            SinkOperators = sinks;
            ProcessorOperators = processors;
            StateStores = stateStores;
        }

        internal IProcessor GetSourceProcessor(string name)
        {
            var processor = SourceOperators.FirstOrDefault(kp => kp.Value is ISourceProcessor && (kp.Value as ISourceProcessor).TopicName.Equals(name));
            if (processor.Value != null)
            {
                return processor.Value.Clone() as IProcessor;
            }
            else
                return null;
        }

        internal IEnumerable<string> GetSourceTopics() => SourceOperators.Select(o => (o.Value as ISourceProcessor).TopicName);
    }
}
