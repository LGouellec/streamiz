using kafka_stream_core.SerDes;
using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.Stream
{
    public class Grouped<K,V>
    {
        protected readonly string named;
        protected readonly ISerDes<K> keySerdes;
        protected readonly ISerDes<V> valueSerdes;

        public string Named => named;
        public ISerDes<K> Key => keySerdes;
        public ISerDes<V> Value => valueSerdes;

        private Grouped(string named, ISerDes<K> keySerdes, ISerDes<V> valueSerdes)
        {
            this.named = named;
            this.keySerdes = keySerdes;
            this.valueSerdes = valueSerdes;
        }

        public static Grouped<K, V> Create(string name) 
            => Create(name, null, null);

        public static Grouped<K, V> Create(ISerDes<K> keySerdes, ISerDes<V> valueSerdes) 
            => Create(null, keySerdes, valueSerdes);

        public static Grouped<K, V> Create(string name, ISerDes<K> keySerdes, ISerDes<V> valueSerdes)
            => new Grouped<K, V>(name, keySerdes, valueSerdes);

        public static Grouped<K, V> Create<KS, VS>()
            where KS : ISerDes<K>, new()
            where VS : ISerDes<V>, new()
            => Create<KS, VS>(null);

        public static Grouped<K, V> Create<KS, VS>(string name)
            where KS : ISerDes<K>, new()
            where VS : ISerDes<V>, new()
            => new Grouped<K, V>(name, new KS(), new VS());
    }
}
