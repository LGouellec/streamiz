using kafka_stream_core.Processors.Internal;
using kafka_stream_core.SerDes;
using kafka_stream_core.Stream.Internal.Graph.Nodes;
using System;
using System.Collections.Generic;
using System.Text;

namespace kafka_stream_core.Stream.Internal
{
    internal abstract class AbstractStream<K, V>
    {
        protected String nameNode;
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


        protected static WrapperValueMapperWithKey<K, V, VR> withKey<VR>(Func<V, VR> valueMapper)
        {
            valueMapper = valueMapper ?? throw new ArgumentNullException(nameof(valueMapper));

            return new WrapperValueMapperWithKey<K, V, VR>((readOnlyKey, value) => valueMapper(value));
        }

        protected static WrapperValueMapperWithKey<K, V, VR> withKey<VR>(IValueMapper<V, VR> valueMapper)
        {
            valueMapper = valueMapper ?? throw new ArgumentNullException(nameof(valueMapper));

            return new WrapperValueMapperWithKey<K, V, VR>((readOnlyKey, value) => valueMapper.apply(value));
        }

        List<String> ensureCopartitionWith(List<AbstractStream<K, V>> otherStreams)
        {
            List<String> allSourceNodes = new List<string>(setSourceNodes);
            foreach (var other in otherStreams)
            {
                allSourceNodes.AddRange(other.setSourceNodes);
            }
            // TODO : builder.internalTopologyBuilder.copartitionSources(allSourceNodes);

            return allSourceNodes;
        }
    }
}
