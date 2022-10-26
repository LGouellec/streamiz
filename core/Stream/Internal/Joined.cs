
using System;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Kafka.Net.Stream.Internal
{
    internal class Joined<K, V, VO>
    {
        public ISerDes<K> KeySerdes { get; }
        public ISerDes<V> ValueSerdes { get; }
        public ISerDes<VO> OtherValueSerDes { get; }
        public string Name { get; }

        public Joined(ISerDes<K> keySerdes,
                        ISerDes<V> valueSerdes,
                        ISerDes<VO> otherValueSerDes,
                        String name)
        {
            KeySerdes = keySerdes;
            ValueSerdes = valueSerdes;
            OtherValueSerDes = otherValueSerDes;
            Name = name;
        }
    }
}