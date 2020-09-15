using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State.Supplier;

namespace Streamiz.Kafka.Net.Stream
{
    /// <summary>
    /// Class used to configure the name of the join processor, the repartition topic name,
    /// state stores or state store names in  Stream-Stream join.
    /// </summary>
    public class StreamJoinProps
    {
        /// <summary>
        /// supplier to store value left join
        /// </summary>
        protected readonly WindowBytesStoreSupplier thisStoreSupplier;

        /// <summary>
        /// supplier to store value rigth join
        /// </summary>
        protected readonly WindowBytesStoreSupplier otherStoreSupplier;

        /// <summary>
        /// name of stream join
        /// </summary>
        protected readonly string name;

        /// <summary>
        /// Name of store
        /// </summary>
        protected readonly string storeName;

        internal StreamJoinProps(WindowBytesStoreSupplier thisStoreSupplier, WindowBytesStoreSupplier otherStoreSupplier, string name, string storeName)
        {
            this.thisStoreSupplier = thisStoreSupplier;
            this.otherStoreSupplier = otherStoreSupplier;
            this.name = name;
            this.storeName = storeName;
        }

        /// <summary>
        /// Name of stream join operation
        /// </summary>
        public string Name => name;

        /// <summary>
        /// Name of store
        /// </summary>
        public string StoreName => storeName;

        /// <summary>
        /// Supplier to store value left join
        /// </summary>
        public WindowBytesStoreSupplier LeftStoreSupplier => thisStoreSupplier;

        /// <summary>
        /// Supplier to store value rigth join
        /// </summary>
        public WindowBytesStoreSupplier RightStoreSupplier => otherStoreSupplier;

        /// <summary>
        /// Creates a StreamJoined instance with the provided store suppliers. The store suppliers must implement
        /// the <see cref="WindowBytesStoreSupplier"/> interface.  The store suppliers must provide unique names or a
        /// <see cref="Streamiz.Kafka.Net.Errors.StreamsException"/> is thrown.
        /// </summary>
        /// <param name="storeSupplier">this store supplier</param>
        /// <param name="otherStoreSupplier">other store supplier</param>
        /// <returns>Return <see cref="StreamJoinProps"/> instance</returns>
        public static StreamJoinProps With(WindowBytesStoreSupplier storeSupplier, WindowBytesStoreSupplier otherStoreSupplier)
        {
            return new StreamJoinProps(
                storeSupplier,
                otherStoreSupplier,
                null,
                null
            );
        }


        /// <summary>
        /// Creates a StreamJoined instance with the provided store suppliers. The store suppliers must implement
        /// the <see cref="WindowBytesStoreSupplier"/> interface.  The store suppliers must provide unique names or a
        /// <see cref="Streamiz.Kafka.Net.Errors.StreamsException"/> is thrown.
        /// </summary>
        /// <typeparam name="K">the key type</typeparam>
        /// <typeparam name="V1">the value type</typeparam>
        /// <typeparam name="V2">the other value type</typeparam>        
        /// <param name="storeSupplier">this store supplier</param>
        /// <param name="otherStoreSupplier">other store supplier</param>
        /// <returns>Return <see cref="StreamJoinProps{K, V1, V2}"/> instance</returns>
        public static StreamJoinProps<K, V1, V2> With<K, V1, V2>(WindowBytesStoreSupplier storeSupplier, WindowBytesStoreSupplier otherStoreSupplier)
        {
            return new StreamJoinProps<K, V1, V2>(
                null,
                null,
                null,
                storeSupplier,
                otherStoreSupplier,
                null,
                null
            );
        }

        /// <summary>
        /// Creates a StreamJoined instance with the provided serdes.
        /// </summary>
        /// <typeparam name="K">the key type</typeparam>
        /// <typeparam name="V1">the value type</typeparam>
        /// <typeparam name="V2">the other value type</typeparam>  
        /// <param name="keySerde">key serdes</param>
        /// <param name="valueSerde">value serdes</param>
        /// <param name="otherValueSerde">other value serdes</param>
        /// <returns>Return <see cref="StreamJoinProps{K, V1, V2}"/> instance</returns>
        public static StreamJoinProps<K, V1, V2> With<K, V1, V2>(ISerDes<K> keySerde, ISerDes<V1> valueSerde, ISerDes<V2> otherValueSerde)
        {
            return new StreamJoinProps<K, V1, V2>(
                keySerde,
                valueSerde,
                otherValueSerde,
                null,
                null,
                null,
                null
            );
        }

        /// <summary>
        /// Creates a <see cref="StreamJoinProps"/> instance using the provided name for the state stores and hence the changelog
        /// topics for the join stores.  The name for the stores will be ${applicationId}-&lt;storeName&gt;-this-join and ${applicationId}-&lt;storeName&gt;-other-join
        /// or ${applicationId}-&lt;storeName&gt;-outer-this-join and ${applicationId}-&lt;storeName&gt;-outer-other-join depending if the join is an inner-join
        /// or an outer join. The changelog topics will have the -changelog suffix.  The user should note that even though the join stores will have a
        /// specified name, the stores will remain unavailable for querying.
        /// </summary>
        /// <param name="storeName">The name to use for the store</param>
        /// <returns>Return <see cref="StreamJoinProps"/> instance</returns>
        public static StreamJoinProps As(string storeName)
        {
            return new StreamJoinProps(
                null,
                null,
                null,
                storeName
            );
        }

        /// <summary>
        /// Creates a <see cref="StreamJoinProps"/> instance using the provided name for the state stores and hence the changelog
        /// topics for the join stores.  The name for the stores will be ${applicationId}-&lt;storeName&gt;-this-join and ${applicationId}-&lt;storeName&gt;-other-join
        /// or ${applicationId}-&lt;storeName&gt;-outer-this-join and ${applicationId}-&lt;storeName&gt;-outer-other-join depending if the join is an inner-join
        /// or an outer join. The changelog topics will have the -changelog suffix.  The user should note that even though the join stores will have a
        /// specified name, the stores will remain unavailable for querying.
        /// </summary>
        /// <param name="name">The name to use for stream join processor</param>
        /// <param name="storeName">The name to use for the store</param>
        /// <returns>Return <see cref="StreamJoinProps"/> instance</returns>
        public static StreamJoinProps As(string name, string storeName)
        {
            return new StreamJoinProps(
                null,
                null,
                name,
                storeName
            );
        }

        /// <summary>
        /// Creates a <see cref="StreamJoinProps"/> instance using the provided name for the state stores and hence the changelog
        /// topics for the join stores.  The name for the stores will be ${applicationId}-&lt;storeName&gt;-this-join and ${applicationId}-&lt;storeName&gt;-other-join
        /// or ${applicationId}-&lt;storeName&gt;-outer-this-join and ${applicationId}-&lt;storeName&gt;-outer-other-join depending if the join is an inner-join
        /// or an outer join. The changelog topics will have the -changelog suffix.  The user should note that even though the join stores will have a
        /// specified name, the stores will remain unavailable for querying.
        /// </summary>
        /// <typeparam name="K">the key type</typeparam>
        /// <typeparam name="V1">the value type</typeparam>
        /// <typeparam name="V2">the other value type</typeparam>  
        /// <param name="storeName">The name to use for the store</param>
        /// <returns>Return <see cref="StreamJoinProps{K, V1, V2}"/> instance</returns>
        public static StreamJoinProps<K, V1, V2> As<K, V1, V2>(string storeName)
        {
            return new StreamJoinProps<K, V1, V2>(
              null,
              null,
              null,
              null,
              null,
              null,
              storeName
          );
        }

        /// <summary>
        /// Creates a <see cref="StreamJoinProps"/> instance using the provided name for the state stores and hence the changelog
        /// topics for the join stores.  The name for the stores will be ${applicationId}-&lt;storeName&gt;-this-join and ${applicationId}-&lt;storeName&gt;-other-join
        /// or ${applicationId}-&lt;storeName&gt;-outer-this-join and ${applicationId}-&lt;storeName&gt;-outer-other-join depending if the join is an inner-join
        /// or an outer join. The changelog topics will have the -changelog suffix.  The user should note that even though the join stores will have a
        /// specified name, the stores will remain unavailable for querying.
        /// </summary>
        /// <typeparam name="K">the key type</typeparam>
        /// <typeparam name="V1">the value type</typeparam>
        /// <typeparam name="V2">the other value type</typeparam>  
        /// <param name="name">The name to use for stream join processor</param>
        /// <param name="storeName">The name to use for the store</param>
        /// <returns>Return <see cref="StreamJoinProps{K, V1, V2}"/> instance</returns>
        public static StreamJoinProps<K, V1, V2> As<K, V1, V2>(string name, string storeName)
        {
            return new StreamJoinProps<K, V1, V2>(
              null,
              null,
              null,
              null,
              null,
              name,
              storeName
            );
        }


        internal static StreamJoinProps<T1, T2, T3> From<T1, T2, T3>(StreamJoinProps props)
        {
            if (props != null)
            {
                return new StreamJoinProps<T1, T2, T3>(null, null, null, props.LeftStoreSupplier, props.RightStoreSupplier, props.Name, props.StoreName);
            }
            else
            {
                return new StreamJoinProps<T1, T2, T3>(null, null, null, null, null, null, null);
            }
        }
    }

    /// <summary>
    /// Class used to configure the name of the join processor, the repartition topic name,
    /// state stores or state store names in  Stream-Stream join.
    /// </summary>
    /// <typeparam name="K">the key type</typeparam>
    /// <typeparam name="V1">the value type</typeparam>
    /// <typeparam name="V2">other value type</typeparam>
    public class StreamJoinProps<K, V1, V2> : StreamJoinProps
    {
        internal StreamJoinProps(
            ISerDes<K> keySerdes,
            ISerDes<V1> valueSerdes,
            ISerDes<V2> otherValueSerdes,
            WindowBytesStoreSupplier thisStoreSupplier,
            WindowBytesStoreSupplier otherStoreSupplier,
            string name,
            string storeName)
            : base(thisStoreSupplier, otherStoreSupplier, name, storeName)
        {
            KeySerdes = keySerdes;
            LeftValueSerdes = valueSerdes;
            RightValueSerdes = otherValueSerdes;
        }

        /// <summary>
        /// Key serdes
        /// </summary>
        public ISerDes<K> KeySerdes { get; internal set; }

        /// <summary>
        /// Value serdes
        /// </summary>
        public ISerDes<V1> LeftValueSerdes { get; internal set; }

        /// <summary>
        /// Other value serdes
        /// </summary>
        public ISerDes<V2> RightValueSerdes { get; internal set; }
    }
}