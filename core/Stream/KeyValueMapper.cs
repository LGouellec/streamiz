using System;

namespace kafka_stream_core.Stream
{
    public interface IKeyValueMapper<K, V, VR>
    {
        VR Apply(K key, V value);
    }

    public class WrappedKeyValueMapper<K, V, VR> : IKeyValueMapper<K, V, VR>
    {
        private readonly Func<K, V, VR> wrappedFunction;

        public WrappedKeyValueMapper(Func<K, V, VR> function)
        {
            this.wrappedFunction = function;
        }
        public VR Apply(K key, V value) => wrappedFunction.Invoke(key, value);
    }
}
