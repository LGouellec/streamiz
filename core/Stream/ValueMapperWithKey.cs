using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.Stream
{
    public interface IValueMapperWithKey<K, V, VR>
    {
        VR apply(K keyReadonly, V value);
    }

    public class WrapperValueMapperWithKey<K, V, VR> : IValueMapperWithKey<K, V, VR>
    {
        private readonly Func<K, V, VR> mapper;

        public WrapperValueMapperWithKey(Func<K, V, VR> mapper)
            => this.mapper = mapper;

        public VR apply(K readOnlyKey, V value) => this.mapper(readOnlyKey, value);
    }
}
