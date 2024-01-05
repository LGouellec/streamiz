using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Kafka.Net.Stream
{
    /// <summary>
    /// Class used to configure the key and value serdes in Stream-Table join operation.
    /// </summary>
    public class StreamTableJoinProps<K, V1, V2>
    {
        /// <summary>
        /// Creates a <see cref="StreamTableJoinProps{K,V1,V2}"/>
        /// </summary>
        /// <typeparam name="K">the key type</typeparam>
        /// <typeparam name="V1">the value type</typeparam>
        /// <typeparam name="V2">the other value type</typeparam>
        public StreamTableJoinProps(
            ISerDes<K> keySerdes,
            ISerDes<V1> valueSerdes,
            ISerDes<V2> otherValueSerdes)
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
        /// Stream value serdes
        /// </summary>
        public ISerDes<V1> LeftValueSerdes { get; internal set; }

        /// <summary>
        /// Table value serdes
        /// </summary>
        public ISerDes<V2> RightValueSerdes { get; internal set; }
    }
}