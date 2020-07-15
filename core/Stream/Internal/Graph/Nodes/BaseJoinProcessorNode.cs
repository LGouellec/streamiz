using System;

namespace Streamiz.Kafka.Net.Stream.Internal.Graph.Nodes
{
    /// <summary>
    /// Utility base class contains the common fields between a stream-stream join and a table-table join
    /// </summary>
    /// <typeparam name="K"></typeparam>
    /// <typeparam name="V1"></typeparam>
    /// <typeparam name="V2"></typeparam>
    /// <typeparam name="VR"></typeparam>
    internal abstract class BaseJoinProcessorNode<K, V1, V2, VR> : StreamGraphNode
    {
        public BaseJoinProcessorNode(
            string name,
            IValueJoiner<V1, V2, VR> valueJoiner,
            ProcessorParameters<K, V1> joinLeftParams,
            ProcessorParameters<K, V2> joinRightParams,
            ProcessorParameters<K, VR> joinMergeParams,
            string leftJoinSideName,
            string rightJoinSideName)
            : base(name)
        {
            ValueJoiner = valueJoiner ?? throw new ArgumentNullException(nameof(valueJoiner));
            JoinLeftParams = joinLeftParams ?? throw new ArgumentNullException(nameof(joinLeftParams));
            JoinRightParams = joinRightParams ?? throw new ArgumentNullException(nameof(joinRightParams));
            JoinMergeParams = joinMergeParams ?? throw new ArgumentNullException(nameof(joinMergeParams));
            LeftJoinSideName = leftJoinSideName;
            RightJoinSideName = rightJoinSideName;
        }

        public IValueJoiner<V1, V2, VR> ValueJoiner { get; }
        public ProcessorParameters<K, V1> JoinLeftParams { get; }
        public ProcessorParameters<K, V2> JoinRightParams { get; }
        public ProcessorParameters<K, VR> JoinMergeParams { get; }
        public string LeftJoinSideName { get; }
        public string RightJoinSideName { get; }
    }
}
