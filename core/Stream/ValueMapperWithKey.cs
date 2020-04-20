using System;
using System.Collections.Generic;
using System.Text;

namespace Streamiz.Kafka.Net.Stream
{
    public interface IValueMapperWithKey<K, V, VR>
    {
        VR Apply(K keyReadonly, V value);
    }

    internal class WrapperValueMapperWithKey<K, V, VR> : IValueMapperWithKey<K, V, VR>
    {
        private readonly Func<K, V, VR> mapper;

        public WrapperValueMapperWithKey(Func<K, V, VR> mapper)
            => this.mapper = mapper;

        public VR Apply(K readOnlyKey, V value) => this.mapper(readOnlyKey, value);
    }
}
