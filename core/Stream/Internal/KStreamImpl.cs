using kafka_stream_core.Processors;
using kafka_stream_core.Processors.Internal;
using kafka_stream_core.SerDes;
using kafka_stream_core.Stream.Internal.Graph;
using kafka_stream_core.Stream.Internal.Graph.Nodes;
using System;
using System.Collections.Generic;

namespace kafka_stream_core.Stream.Internal
{
    internal class KStreamImpl<K, V> : AbstractStream<K, V>, KStream<K, V>
    {
        #region Constants

        internal static String JOINTHIS_NAME = "KSTREAM-JOINTHIS-";

        internal static String JOINOTHER_NAME = "KSTREAM-JOINOTHER-";

        internal static String JOIN_NAME = "KSTREAM-JOIN-";

        internal static String LEFTJOIN_NAME = "KSTREAM-LEFTJOIN-";

        internal static String MERGE_NAME = "KSTREAM-MERGE-";

        internal static String OUTERTHIS_NAME = "KSTREAM-OUTERTHIS-";

        internal static String OUTEROTHER_NAME = "KSTREAM-OUTEROTHER-";

        internal static String WINDOWED_NAME = "KSTREAM-WINDOWED-";

        internal static String SOURCE_NAME = "KSTREAM-SOURCE-";

        internal static String SINK_NAME = "KSTREAM-SINK-";

        internal static String REPARTITION_TOPIC_SUFFIX = "-repartition";

        internal static String BRANCH_NAME = "KSTREAM-BRANCH-";

        internal static String BRANCHCHILD_NAME = "KSTREAM-BRANCHCHILD-";

        internal static String FILTER_NAME = "KSTREAM-FILTER-";

        internal static String PEEK_NAME = "KSTREAM-PEEK-";

        internal static String FLATMAP_NAME = "KSTREAM-FLATMAP-";

        internal static String FLATMAPVALUES_NAME = "KSTREAM-FLATMAPVALUES-";

        internal static String MAP_NAME = "KSTREAM-MAP-";

        internal static String MAPVALUES_NAME = "KSTREAM-MAPVALUES-";

        internal static String PROCESSOR_NAME = "KSTREAM-PROCESSOR-";

        internal static String PRINTING_NAME = "KSTREAM-PRINTER-";

        internal static String KEY_SELECT_NAME = "KSTREAM-KEY-SELECT-";

        internal static String TRANSFORM_NAME = "KSTREAM-TRANSFORM-";

        internal static String TRANSFORMVALUES_NAME = "KSTREAM-TRANSFORMVALUES-";

        internal static String FOREACH_NAME = "KSTREAM-FOREACH-";

        #endregion

        internal KStreamImpl(string name, ISerDes<K> keySerdes, ISerDes<V> valueSerdes, List<string> setSourceNodes, StreamGraphNode node, InternalStreamBuilder builder)
            : base(name, keySerdes, valueSerdes, setSourceNodes, node, builder)
        {
        }

        #region Branch

        public KStream<K, V>[] branch(params Func<K, V, bool>[] predicates) => doBranch(string.Empty, predicates);

        public KStream<K, V>[] branch(string named, params Func<K, V, bool>[] predicates) => doBranch(named, predicates);

        #endregion

        #region Filter

        public KStream<K, V> filter(Func<K, V, bool> predicate)
        {
            string name = this.builder.newProcessorName(FILTER_NAME);
            ProcessorParameters<K, V> processorParameters = new ProcessorParameters<K, V>(new KStreamFilter<K, V>(predicate), name);
            ProcessorGraphNode<K, V> filterProcessorNode = new ProcessorGraphNode<K, V>(name, processorParameters);

            this.builder.addGraphNode(node, filterProcessorNode);
            return new KStreamImpl<K, V>(name, this.keySerdes, this.valueSerdes, this.setSourceNodes, filterProcessorNode, this.builder);
        }

        public KStream<K, V> filterNot(Func<K, V, bool> predicate)
        {
            string name = this.builder.newProcessorName(FILTER_NAME);
            ProcessorParameters<K, V> processorParameters = new ProcessorParameters<K, V>(new KStreamFilter<K, V>(predicate, true), name);
            ProcessorGraphNode<K, V> filterProcessorNode = new ProcessorGraphNode<K, V>(name, processorParameters);
            this.builder.addGraphNode(node, filterProcessorNode);
            return new KStreamImpl<K, V>(name, this.keySerdes, this.valueSerdes, this.setSourceNodes, filterProcessorNode, this.builder);
        }

        #endregion

        #region Transform

        #endregion

        #region To

        public void to(string topicName, Produced<K, V> produced) => to(new StaticTopicNameExtractor<K, V>(topicName), produced);

        public void to(string topicName) => to(topicName, Produced<K, V>.with(keySerdes, valueSerdes));

        public void to(TopicNameExtractor<K, V> topicExtractor) => to(topicExtractor, Produced<K, V>.with(keySerdes, valueSerdes));

        public void to(TopicNameExtractor<K, V> topicExtractor, Produced<K, V> produced) => doTo(topicExtractor, produced);

        #endregion

        #region FlatMap

        public KStream<KR, VR> flatMap<KR, VR>(Func<K, V, IEnumerable<KeyValuePair<KR, VR>>> mapper)
            => this.flatMap(mapper, string.Empty);

        public KStream<KR, VR> flatMap<KR, VR>(Func<K, V, IEnumerable<KeyValuePair<KR, VR>>> mapper, string named)
            => this.flatMap(new WrappedKeyValueMapper<K, V, IEnumerable<KeyValuePair<KR, VR>>>(mapper), named);

        public KStream<KR, VR> flatMap<KR, VR>(IKeyValueMapper<K, V, IEnumerable<KeyValuePair<KR, VR>>> mapper)
            => this.flatMap(mapper, string.Empty);

        public KStream<KR, VR> flatMap<KR, VR>(IKeyValueMapper<K, V, IEnumerable<KeyValuePair<KR, VR>>> mapper, string named)
        {
            var name = this.builder.newProcessorName(FLATMAP_NAME);
            ProcessorParameters<K, V> processorParameters = new ProcessorParameters<K, V>(new KStreamFlatMap<K, V, KR, VR>(mapper), name);
            ProcessorGraphNode<K, V> flatMapNode = new ProcessorGraphNode<K, V>(name, processorParameters);
            flatMapNode.KeyChangingOperation = true;

            builder.addGraphNode(node, flatMapNode);

            // key and value serde cannot be preserved
            return new KStreamImpl<KR, VR>(name, null, null, setSourceNodes, flatMapNode, builder);
        }

        #endregion

        #region FlatMapValues

        public KStream<K, VR> flatMapValues<VR>(Func<V, IEnumerable<VR>> mapper)
            => this.flatMapValues(mapper, string.Empty);

        public KStream<K, VR> flatMapValues<VR>(Func<V, IEnumerable<VR>> mapper, string named)
            => this.flatMapValues(new WrappedValueMapper<V, IEnumerable<VR>>(mapper), named);

        public KStream<K, VR> flatMapValues<VR>(Func<K, V, IEnumerable<VR>> mapper)
            => this.flatMapValues(mapper, string.Empty);

        public KStream<K, VR> flatMapValues<VR>(Func<K, V, IEnumerable<VR>> mapper, string named)
            => this.flatMapValues(new WrapperValueMapperWithKey<K, V, IEnumerable<VR>>(mapper), named);

        public KStream<K, VR> flatMapValues<VR>(IValueMapper<V, IEnumerable<VR>> mapper)
            => this.flatMapValues(mapper, null);

        public KStream<K, VR> flatMapValues<VR>(IValueMapper<V, IEnumerable<VR>> mapper, string named)
            => this.flatMapValues(withKey(mapper), named);

        public KStream<K, VR> flatMapValues<VR>(IValueMapperWithKey<K, V, IEnumerable<VR>> mapper)
            => this.flatMapValues(mapper, null);

        public KStream<K, VR> flatMapValues<VR>(IValueMapperWithKey<K, V, IEnumerable<VR>> mapper, string named)
        {
             var name = this.builder.newProcessorName(FLATMAPVALUES_NAME);

            ProcessorParameters<K,V> processorParameters = new ProcessorParameters<K, V>(new KStreamFlatMapValues<K, V, VR>(mapper), name);
            ProcessorGraphNode<K, V> flatMapValuesNode = new ProcessorGraphNode<K, V>(name, processorParameters);
            flatMapValuesNode.ValueChangingOperation = true;

            builder.addGraphNode(this.node, flatMapValuesNode);

            // value serde cannot be preserved
            return new KStreamImpl<K, VR>(
                name,
                this.keySerdes,
                null,
                this.setSourceNodes,
                flatMapValuesNode,
                builder);
        }

        #endregion

        #region Foreach

        public void @foreach(Action<K, V> action) => this.@foreach(action, string.Empty);

        public void @foreach(Action<K, V> action, string named)
        {
            String name = this.builder.newProcessorName(FOREACH_NAME);
            ProcessorParameters<K, V> processorParameters = new ProcessorParameters<K, V>(new KStreamPeek<K, V>(action, false), name);
            ProcessorGraphNode<K, V> foreachNode = new ProcessorGraphNode<K, V>(name, processorParameters);

            this.builder.addGraphNode(node, foreachNode);
        }

        #endregion

        #region Peek

        public KStream<K, V> peek(Action<K, V> action) => this.peek(action, string.Empty);

        public KStream<K, V> peek(Action<K, V> action, string named)
        {
            String name = this.builder.newProcessorName(PEEK_NAME);
            ProcessorParameters<K, V> processorParameters = new ProcessorParameters<K, V>(new KStreamPeek<K, V>(action, true), name);
            ProcessorGraphNode<K, V> peekNode = new ProcessorGraphNode<K, V>(name, processorParameters);

            builder.addGraphNode(node, peekNode);

            return new KStreamImpl<K, V>(
                name,
                keySerdes,
                valueSerdes,
                setSourceNodes,
                peekNode,
                builder);
        }

        #endregion

        #region Map

        public KStream<KR, VR> map<KR, VR>(Func<K, V, KeyValuePair<KR, VR>> mapper)
            => this.map(mapper, string.Empty);

        public KStream<KR, VR> map<KR, VR>(Func<K, V, KeyValuePair<KR, VR>> mapper, string named)
            => this.map(new WrappedKeyValueMapper<K, V, KeyValuePair<KR, VR>>(mapper), named);

        public KStream<KR, VR> map<KR, VR>(IKeyValueMapper<K, V, KeyValuePair<KR, VR>> mapper)
            => this.map(mapper, string.Empty);

        public KStream<KR, VR> map<KR, VR>(IKeyValueMapper<K, V, KeyValuePair<KR, VR>> mapper, string named)
        {
            string name = this.builder.newProcessorName(MAP_NAME);
            ProcessorParameters<K, V> processorParameters = new ProcessorParameters<K, V>(new KStreamMap<K, V, KR, VR>(mapper), name);
            ProcessorGraphNode<K, V> mapProcessorNode = new ProcessorGraphNode<K, V>(name, processorParameters);
            mapProcessorNode.KeyChangingOperation = true;

            builder.addGraphNode(node, mapProcessorNode);

            // key and value serde cannot be preserved
            return new KStreamImpl<KR, VR>(
                    name,
                    null,
                    null,
                    setSourceNodes,
                    mapProcessorNode,
                    builder);
        }

        #endregion

        #region MapValues

        public KStream<K, VR> mapValues<VR>(Func<V, VR> mapper)
            => this.mapValues(mapper, string.Empty);

        public KStream<K, VR> mapValues<VR>(Func<V, VR> mapper, string named)
            => this.mapValues(new WrappedValueMapper<V, VR>(mapper), named);

        public KStream<K, VR> mapValues<VR>(Func<K, V, VR> mapper)
            => this.mapValues(mapper, string.Empty);

        public KStream<K, VR> mapValues<VR>(Func<K, V, VR> mapper, string named)
            => this.mapValues(new WrapperValueMapperWithKey<K, V, VR>(mapper), named);

        public KStream<K, VR> mapValues<VR>(IValueMapper<V, VR> mapper)
            => this.mapValues<VR>(mapper, null);

        public KStream<K, VR> mapValues<VR>(IValueMapper<V, VR> mapper, string named)
            => this.mapValues<VR>(withKey(mapper), named);

        public KStream<K, VR> mapValues<VR>(IValueMapperWithKey<K, V, VR> mapper)
            => this.mapValues<VR>(mapper, null);

        public KStream<K, VR> mapValues<VR>(IValueMapperWithKey<K, V, VR> mapper, string named)
        {
            String name = this.builder.newProcessorName(MAPVALUES_NAME);

            ProcessorParameters<K, V> processorParameters = new ProcessorParameters<K, V>(new KStreamMapValues<K, V, VR>(mapper), name);
            ProcessorGraphNode<K, V> mapValuesProcessorNode = new ProcessorGraphNode<K, V>(name, processorParameters);
            mapValuesProcessorNode.ValueChangingOperation = true;

            builder.addGraphNode(this.node, mapValuesProcessorNode);

            // value serde cannot be preserved
            return new KStreamImpl<K, VR>(
                    name,
                    this.keySerdes,
                    null,
                    this.setSourceNodes,
                    mapValuesProcessorNode,
                    builder);
        }

        #endregion

        #region Print

        public void print(Printed<K, V> printed)
        {
            String name = this.builder.newProcessorName(PRINTING_NAME);
            ProcessorParameters<K, V> processorParameters = new ProcessorParameters<K, V>(printed.build(this.nameNode), name);
            ProcessorGraphNode<K, V> printNode = new ProcessorGraphNode<K, V>(name, processorParameters);

            builder.addGraphNode(node, printNode);
        }

        #endregion

        #region SelectKey

        public KStream<KR, V> selectKey<KR>(Func<K, V, KR> mapper)
            => this.selectKey(mapper, string.Empty);

        public KStream<KR, V> selectKey<KR>(Func<K, V, KR> mapper, string named)
            => this.selectKey(new WrappedKeyValueMapper<K, V, KR>(mapper), named);

        public KStream<KR, V> selectKey<KR>(IKeyValueMapper<K, V, KR> mapper)
            => this.selectKey(mapper, string.Empty);

        public KStream<KR, V> selectKey<KR>(IKeyValueMapper<K, V, KR> mapper, string named)
        {
            ProcessorGraphNode<K, V> selectKeyProcessorNode = internalSelectKey(mapper, named);
            selectKeyProcessorNode.KeyChangingOperation = true;

            builder.addGraphNode(node, selectKeyProcessorNode);

            // key serde cannot be preserved
            return new KStreamImpl<KR, V>(
                selectKeyProcessorNode.streamGraphNode,
                null,
                valueSerdes,
                setSourceNodes,
                selectKeyProcessorNode,
                builder);
        }

        #endregion

        #region Private

        private void doTo(TopicNameExtractor<K, V> topicExtractor, Produced<K, V> produced)
        {
            string name = this.builder.newProcessorName(SINK_NAME);

            StreamSinkNode<K, V> sinkNode = new StreamSinkNode<K, V>(topicExtractor, name, produced);
            this.builder.addGraphNode(node, sinkNode);
        }

        private KStream<K, V>[] doBranch(string named, params Func<K, V, bool>[] predicates)
        {
            if (predicates.Length == 0)
                throw new ArgumentException("branch() requires at least one predicate");

            String branchName = this.builder.newProcessorName(BRANCH_NAME);
            String[] childNames = new String[predicates.Length];
            for (int i = 0; i < predicates.Length; i++)
                childNames[i] = $"{this.builder.newProcessorName(BRANCHCHILD_NAME)}- predicate-{i}";

            ProcessorParameters<K, V> processorParameters = new ProcessorParameters<K, V>(new KStreamBranch<K, V>(predicates, childNames), branchName);
            ProcessorGraphNode<K, V> branchNode = new ProcessorGraphNode<K, V>(branchName, processorParameters);

            this.builder.addGraphNode(node, branchNode);

            KStream<K, V>[] branchChildren = new KStream<K, V>[predicates.Length];
            for (int i = 0; i < predicates.Length; i++)
            {
                ProcessorParameters<K, V> innerProcessorParameters = new ProcessorParameters<K, V>(new PassThrough<K, V>(), childNames[i]);
                ProcessorGraphNode<K, V> branchChildNode = new ProcessorGraphNode<K, V>(childNames[i], innerProcessorParameters);

                builder.addGraphNode(branchNode, branchChildNode);
                branchChildren[i] = new KStreamImpl<K, V>(childNames[i], this.keySerdes, this.valueSerdes, setSourceNodes, branchChildNode, builder);
            }

            return branchChildren;
        }

        private ProcessorGraphNode<K, V> internalSelectKey<KR>(IKeyValueMapper<K, V, KR> mapper, string named)
        {
            String name = this.builder.newProcessorName(KEY_SELECT_NAME);
            KStreamMap<K, V, KR, V> kStreamMap = new KStreamMap<K, V, KR, V>((key, value) => new KeyValuePair<KR, V>(mapper.apply(key, value), value));
            ProcessorParameters<K, V> processorParameters = new ProcessorParameters<K, V>(kStreamMap, name);

            return new ProcessorGraphNode<K, V>(name, processorParameters);
        }

        #endregion
    }
}