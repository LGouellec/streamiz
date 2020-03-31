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

        public static Grouped<K, V> @As(string named) 
            => new Grouped<K, V>(named, null, null);

        public static Grouped<K, V> WithKeySerde(ISerDes<K> keySerdes) 
            => new Grouped<K, V>(null, keySerdes, null);

        public static Grouped<K, V> WithValueSerde(ISerDes<V> valueSerdes) 
            => new Grouped<K, V>(null, null, valueSerdes);

        public static Grouped<K, V> With(string named, ISerDes<K> keySerdes, ISerDes<V> valueSerdes) 
            => new Grouped<K, V>(named, keySerdes, valueSerdes);

        public static Grouped<K, V> With(ISerDes<K> keySerdes, ISerDes<V> valueSerdes)
            => new Grouped<K, V>(null, keySerdes, valueSerdes);
    }
}
