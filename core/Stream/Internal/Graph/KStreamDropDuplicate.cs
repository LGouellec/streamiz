using System;
using Streamiz.Kafka.Net.Processors;

namespace Streamiz.Kafka.Net.Stream.Internal.Graph
{
    internal class KStreamDropDuplicate<K, V> : IProcessorSupplier<K, V>
    {
        private readonly string _name;
        private readonly string _storeName;
        private readonly Func<K, V, V, bool> _valueComparer;
        private readonly TimeSpan _interval;

        public KStreamDropDuplicate(
            string name,
            string storeName,
            Func<K, V, V, bool> valueComparer,
            TimeSpan interval)

        {
            _name = name;
            _storeName = storeName;
            _valueComparer = valueComparer;
            _interval = interval;
        }

        public IProcessor<K, V> Get()
            => new KStreamDropDuplicateProcessor<K, V>(_name, _storeName, _valueComparer, (long)_interval.TotalMilliseconds);
    }
}