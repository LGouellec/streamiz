using System;
using Confluent.Kafka;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Processors.Internal;
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
        internal string Named { get; private set; }
        internal ISerDes<K> KeySerdes { get; private set; }
        internal ISerDes<V> ValueSerdes { get; private set; }
        internal int? NumberOfPartition { get; private set; }
        internal IStreamPartitioner<K, V> StreamPartitioner { get; private set; }

        private Repartitioned(
            string named,
            ISerDes<K> keySerdes, 
            ISerDes<V> valueSerdes, 
            int? numberOfPartition,
            IStreamPartitioner<K, V> streamPartitioner)
        {
            Named = named;
            KeySerdes = keySerdes;
            ValueSerdes = valueSerdes;
            NumberOfPartition = numberOfPartition;
            StreamPartitioner = streamPartitioner;
        }

        #region Static

        internal static Repartitioned<K, V> Empty() => new(null, null, null, null, null);
        
        /// <summary>
        /// Create a <see cref="Repartitioned{K,V}"/> instance with the provided name used as part of the repartition topic.
        /// </summary>
        /// <param name="name">name used as a processor name and part of the repartition topic name</param>
        /// <returns>A new instance of <see cref="Repartitioned{K,V}"/></returns>
        public static Repartitioned<K, V> As(string name) => new(name, null, null, null, null);
        
        /// <summary>
        /// Create a <see cref="Repartitioned{K,V}"/> instance with provided key and value serdes.
        /// </summary>
        /// <param name="keySerdes">Serdes to use the repartition key</param>
        /// <param name="valueSerdes">Serdes to use the repartition value</param>
        /// <returns>A new instance of <see cref="Repartitioned{K,V}"/></returns>
        public static Repartitioned<K, V> With(ISerDes<K> keySerdes, ISerDes<V> valueSerdes) => new(null, keySerdes, valueSerdes, null, null);
        
        /// <summary>
        /// Create a <see cref="Repartitioned{K,V}"/> instance with provided key and value serdes.
        /// </summary>
        /// <typeparam name="KS">Repartition key serdes type</typeparam>
        /// <typeparam name="VS">Repartition value serdes typer</typeparam>
        /// <returns>A new instance of <see cref="Repartitioned{K,V}"/></returns>
        public static Repartitioned<K, V> Create<KS, VS>()
            where KS : ISerDes<K>, new()
            where VS : ISerDes<V>, new()
            => new(null, new KS(), new VS(), null, null);

        /// <summary>
        /// Create a <see cref="Repartitioned{K,V}"/> instance with provided number of partitions for repartition topic.
        /// </summary>
        /// <param name="numberPartitions">number of partitions used when creating repartition topic</param>
        /// <returns>A new instance of <see cref="Repartitioned{K,V}"/></returns>
        public static Repartitioned<K, V> NumberOfPartitions(int numberPartitions) => new(null, null, null, numberPartitions, null);
        
        /// <summary>
        /// Create a <see cref="Repartitioned{K,V}"/> instance with provided partitioner.
        /// </summary>
        /// <param name="streamPartitioner">the function used to determine how records are distributed among partitions of the topic</param>
        /// <returns>A new instance of <see cref="Repartitioned{K,V}"/></returns>
        public static Repartitioned<K, V> Partitioner(IStreamPartitioner<K, V> streamPartitioner) => new(null, null, null, null, streamPartitioner);
        
        /// <summary>
        /// Create a <see cref="Repartitioned{K,V}"/> instance with provided partitioner.
        /// </summary>
        /// <param name="streamPartitioner">the function used to determine how records are distributed among partitions of the topic</param>
        /// <returns>A new instance of <see cref="Repartitioned{K,V}"/></returns>
        public static Repartitioned<K, V> Partitioner(Func<string, K, V, Partition, int, Partition> streamPartitioner) 
            => Partitioner(new WrapperStreamPartitioner<K, V>(streamPartitioner));

        
        #endregion

        #region Methods
        
        /// <summary>
        /// Set the repartition name (using for processor name and part of the repartition topic name)
        /// </summary>
        /// <param name="name">Repartition name</param>
        /// <returns>this</returns>
        public Repartitioned<K, V> WithName(string name)
        {
            Named = name;
            return this;
        }

        /// <summary>
        /// Set the key serdes
        /// </summary>
        /// <param name="keySerdes">repartition key serdes</param>
        /// <returns>this</returns>
        public Repartitioned<K, V> WithKeySerdes(ISerDes<K> keySerdes)
        {
            KeySerdes = keySerdes;
            return this;
        }
        
        /// <summary>
        /// Set the value serdes
        /// </summary>
        /// <param name="valueSerdes">repartition value serdes</param>
        /// <returns>this</returns>
        public Repartitioned<K, V> WithValueSerdes(ISerDes<V> valueSerdes)
        {
            ValueSerdes = valueSerdes;
            return this;
        }

        /// <summary>
        /// Set the number of partitions for repartition topic
        /// </summary>
        /// <param name="numberOfPartitions">Number of partitions used during creation for repartition topic</param>
        /// <returns>this</returns>
        public Repartitioned<K, V> WithNumberOfPartitions(int numberOfPartitions)
        {
            NumberOfPartition = numberOfPartitions;
            return this;
        }

        /// <summary>
        /// Set the stream partitioner
        /// </summary>
        /// <param name="streamPartitioner">Function to determine the partition where the repartition record will be persist</param>
        /// <returns>this</returns>
        public Repartitioned<K, V> WithStreamPartitioner(IStreamPartitioner<K, V> streamPartitioner)
        {
            StreamPartitioner = streamPartitioner;
            return this;
        }

        /// <summary>
        /// Set the stream partitioner
        /// </summary>
        /// <param name="streamPartitioner">Function to determine the partition where the repartition record will be persist</param>
        /// <returns>this</returns>
        public Repartitioned<K, V> WithStreamPartitioner(Func<string, K, V, Partition, int, Partition> streamPartitioner)
            => WithStreamPartitioner(new WrapperStreamPartitioner<K, V>(streamPartitioner));


        #endregion
    }
}