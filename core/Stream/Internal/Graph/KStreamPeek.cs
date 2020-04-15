using Kafka.Streams.Net.Processors;
using System;
using System.Collections.Generic;
using System.Text;

namespace Kafka.Streams.Net.Stream.Internal.Graph
{
    internal class KStreamPeek<K, V> : IProcessorSupplier<K, V>
    {
        public bool ForwardDownStream { get; }
        public Action<K, V> Action { get; }

        public KStreamPeek(Action<K, V> action, bool forwardDownStream)
        {
            this.Action = action;
            this.ForwardDownStream = forwardDownStream;
        }

        public IProcessor<K, V> Get() => new KStreamPeekProcessor<K, V>(this.Action, this.ForwardDownStream);
    }
}
