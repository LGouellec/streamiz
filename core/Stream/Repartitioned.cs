using System;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Kafka.Net.Stream
{
    /// <summary>
    /// This class is used to provide the optional parameters for internal repartition topics.
    /// </summary>
    /// <typeparam name="K">key type</typeparam>
    /// <typeparam name="V">value type</typeparam>
    public class Repartitioned<K, V>
    {
        public string Named { get; private set; }
        public ISerDes<K> KeySerdes { get; private set; }
        public ISerDes<V> ValueSerdes { get; private set; }
        public int? NumberOfPartition { get; private set; }
        public Func<string, K, V, int> StreamPartitioner { get; private set; }

        private Repartitioned(
            string named,
            ISerDes<K> keySerdes, 
            ISerDes<V> valueSerdes, 
            int? numberOfPartition,
            Func<string, K, V, int> streamPartitioner)
        {
            Named = named;
            KeySerdes = keySerdes;
            ValueSerdes = valueSerdes;
            NumberOfPartition = numberOfPartition;
            StreamPartitioner = streamPartitioner;
        }

        #region Static

        internal static Repartitioned<K, V> Empty() => new(null, null, null, null, null);
        public static Repartitioned<K, V> As(string name) => new(name, null, null, null, null);
        public static Repartitioned<K, V> With(ISerDes<K> keySerdes, ISerDes<V> valueSerdes) => new(null, keySerdes, valueSerdes, null, null);
        public static Repartitioned<K, V> NumberOfPartitions(int numberPartitions) => new(null, null, null, numberPartitions, null);
        public static Repartitioned<K, V> Partitioner(Func<string, K, V, int> streamPartitioner) => new(null, null, null, null, streamPartitioner);
        
        #endregion

        #region Methods
        
        public Repartitioned<K, V> WithName(string name)
        {
            Named = name;
            return this;
        }

        public Repartitioned<K, V> WithKeySerdes(ISerDes<K> keySerdes)
        {
            KeySerdes = keySerdes;
            return this;
        }
        
        public Repartitioned<K, V> WithValueSerdes(ISerDes<V> valueSerdes)
        {
            ValueSerdes = valueSerdes;
            return this;
        }

        public Repartitioned<K, V> WithNumberOfPartitions(int numberOfPartitions)
        {
            NumberOfPartition = numberOfPartitions;
            return this;
        }

        public Repartitioned<K, V> WithStreamPartitioner(Func<string, K, V, int> streamPartitioner)
        {
            StreamPartitioner = streamPartitioner;
            return this;
        }
        
        #endregion
    }
}