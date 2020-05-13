using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream.Internal.Graph.Nodes;
using System;
using System.Collections.Generic;

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


        protected AbstractStream(AbstractStream<K, V> stream)
        {
            nameNode = stream.nameNode;
            builder = stream.builder;
            keySerdes = stream.keySerdes;
            valueSerdes = stream.valueSerdes;
            setSourceNodes = stream.setSourceNodes;
            node = stream.node;
        }

        protected AbstractStream(String name,
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

            nameNode = name;
            this.builder = builder;
            keySerdes = keySerde;
            valueSerdes = valSerde;
            setSourceNodes = sourceNodes;
            node = streamsGraphNode;
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
