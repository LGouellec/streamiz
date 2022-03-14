using System.Collections.Generic;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State.Enumerator;
using Streamiz.Kafka.Net.State.Internal;

namespace Streamiz.Kafka.Net.State.Metered
{
    internal class MeteredKeyValueStore<K, V> :
        WrappedKeyValueStore<K, V>,
        IKeyValueStore<K, V>
    {
        public MeteredKeyValueStore(IKeyValueStore<Bytes, byte[]> wrapped, ISerDes<K> keySerdes, ISerDes<V> valueSerdes) 
            : base(wrapped, keySerdes, valueSerdes)
        {
        }

        public V Get(K key)
        {
            throw new System.NotImplementedException();
        }

        public IKeyValueEnumerator<K, V> Range(K @from, K to)
        {
            throw new System.NotImplementedException();
        }

        public IKeyValueEnumerator<K, V> ReverseRange(K @from, K to)
        {
            throw new System.NotImplementedException();
        }

        public IEnumerable<KeyValuePair<K, V>> All()
        {
            throw new System.NotImplementedException();
        }

        public IEnumerable<KeyValuePair<K, V>> ReverseAll()
        {
            throw new System.NotImplementedException();
        }

        public long ApproximateNumEntries()
        {
            throw new System.NotImplementedException();
        }

        public void Put(K key, V value)
        {
            throw new System.NotImplementedException();
        }

        public V PutIfAbsent(K key, V value)
        {
            throw new System.NotImplementedException();
        }

        public void PutAll(IEnumerable<KeyValuePair<K, V>> entries)
        {
            throw new System.NotImplementedException();
        }

        public V Delete(K key)
        {
            throw new System.NotImplementedException();
        }
    }
}