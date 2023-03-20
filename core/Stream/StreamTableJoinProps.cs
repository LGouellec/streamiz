using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Kafka.Net.Stream
{
    public class StreamTableJoinProps<K, V1, V2>
    {
        internal StreamTableJoinProps(
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
        /// Value serdes
        /// </summary>
        public ISerDes<V1> LeftValueSerdes { get; internal set; }

        /// <summary>
        /// Other value serdes
        /// </summary>
        public ISerDes<V2> RightValueSerdes { get; internal set; }
    }
}