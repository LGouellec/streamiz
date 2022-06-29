using Streamiz.Kafka.Net.Stream;

namespace Streamiz.Kafka.Net.SerDes
{
    /// <summary>
    /// Request serdes utils class.
    /// <para>
    /// Used for Async processors : <see cref="IKStream{K,V}.MapAsync{K1,V1}"/>, <see cref="IKStream{K,V}.FlatMapAsync{K1,V1}"/>, <see cref="IKStream{K,V}.MapValuesAsync{V1}"/>, <see cref="IKStream{K,V}.FlatMapValuesAsync{V1}"/>, <see cref="IKStream{K,V}.ForeachAsync"/>
    /// </para> 
    /// </summary>
    /// <typeparam name="K">Key type</typeparam>
    /// <typeparam name="V">Value type</typeparam>
    public class RequestSerDes<K, V>
    {
        internal ISerDes<K> RequestKeySerDes { get; private set; }
        internal ISerDes<V> RequestValueSerDes { get; private set; }

        internal static RequestSerDes<K, V> Empty => new(null, null);
        
        /// <summary>
        /// Constructor with request key and value serdes
        /// </summary>
        /// <param name="requestKeySerDes">Request key serdes</param>
        /// <param name="requestValueSerDes">Request value serdes</param>
        public RequestSerDes(
            ISerDes<K> requestKeySerDes,
            ISerDes<V> requestValueSerDes)
        {
            RequestKeySerDes = requestKeySerDes;
            RequestValueSerDes = requestValueSerDes;
        }
    }
}