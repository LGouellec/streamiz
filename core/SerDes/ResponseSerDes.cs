using Streamiz.Kafka.Net.Stream;

namespace Streamiz.Kafka.Net.SerDes
{
    /// <summary>
    /// Response serdes utils class.
    /// <para>
    /// Used for Async processors : <see cref="IKStream{K,V}.MapAsync{K1,V1}"/>, <see cref="IKStream{K,V}.FlatMapAsync{K1,V1}"/>, <see cref="IKStream{K,V}.MapValuesAsync{V1}"/>, <see cref="IKStream{K,V}.FlatMapValuesAsync{V1}"/>
    /// </para> 
    /// </summary>
    /// <typeparam name="K">Key type</typeparam>
    /// <typeparam name="V">Value type</typeparam>
    public class ResponseSerDes<K, V>
    {
        internal ISerDes<K> ResponseKeySerDes { get; private set; }
        internal ISerDes<V> ResponseValueSerDes { get; private set; }

        internal static ResponseSerDes<K, V> Empty => new ResponseSerDes<K, V>(null, null);
        
        /// <summary>                                                        
        /// Constructor with response key and value serdes                    
        /// </summary>                                                       
        /// <param name="responseKeySerDes">Response key serdes</param>        
        /// <param name="responseValueSerDes">Response value serdes</param>    
        public ResponseSerDes(
            ISerDes<K> responseKeySerDes,
            ISerDes<V> responseValueSerDes)
        {
            ResponseKeySerDes = responseKeySerDes;
            ResponseValueSerDes = responseValueSerDes;
        }
    }
}