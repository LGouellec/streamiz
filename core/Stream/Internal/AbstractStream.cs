using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream.Internal.Graph.Nodes;
using System;
using System.Collections.Generic;
using System.Text;

namespace Streamiz.Kafka.Net.Stream.Internal
{
    internal abstract class AbstractStream<K, V>
    {
        protected string nameNode;
        protected ISerDes<K> keySerdes;
        protected ISerDes<V> valueSerdes;
        protected List<String> setSourceNodes;
        protected StreamGraphNode node;
        protected readonly InternalStreamBuilder builder;


        public AbstractStream(AbstractStream<K, V> stream)
        {
            this.nameNode = stream.nameNode;
            this.builder = stream.builder;
            this.keySerdes = stream.keySerdes;
            this.valueSerdes = stream.valueSerdes;
            this.setSourceNodes = stream.setSourceNodes;
            this.node = stream.node;
        }

        internal AbstractStream(String name,
               ISerDes<K> keySerde,
               ISerDes<V> valSerde,
               List<String> sourceNodes,
               StreamGraphNode streamsGraphNode,
               InternalStreamBuilder builder)
        {
            if (sourceNodes == null || sourceNodes.Count == 0)
            {
                throw new ArgumentException("parameter <sourceNodes> must not be null or empty");
            }

            this.nameNode = name;
            this.builder = builder;
            this.keySerdes = keySerde;
            this.valueSerdes = valSerde;
            this.setSourceNodes = sourceNodes;
            this.node = streamsGraphNode;
        }


        protected static WrapperValueMapperWithKey<K, V, VR> WithKey<VR>(Func<V, VR> valueMapper)
        {
            valueMapper = valueMapper ?? throw new ArgumentNullException(nameof(valueMapper));

            return new WrapperValueMapperWithKey<K, V, VR>((readOnlyKey, value) => valueMapper(value));
        }

        protected static WrapperValueMapperWithKey<K, V, VR> WithKey<VR>(IValueMapper<V, VR> valueMapper)
        {
            valueMapper = valueMapper ?? throw new ArgumentNullException(nameof(valueMapper));

            return new WrapperValueMapperWithKey<K, V, VR>((readOnlyKey, value) => valueMapper.Apply(value));
        }
    }
}
