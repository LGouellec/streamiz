using Streamiz.Kafka.Net.Processors;
using System.Linq;

namespace Streamiz.Kafka.Net.Table.Internal.Graph
{
    internal class KTableKTableJoinMerger<K, V1, V2, VR> : IKTableProcessorSupplier<K, VR, VR>
    {
        private readonly IKTableProcessorSupplier<K, V1, VR> leftParent;
        private readonly IKTableProcessorSupplier<K, V2, VR> rightParent;
        private readonly string queryableName;
        private bool sendOldValues = false;

        public KTableKTableJoinMerger(
            IKTableProcessorSupplier<K, V1, VR> leftParent,
            IKTableProcessorSupplier<K, V2, VR> rightParent,
            string queryableName)
        {
            this.leftParent = leftParent;
            this.rightParent = rightParent;
            this.queryableName = queryableName;
        }

        public IKTableValueGetterSupplier<K, VR> View
        {
            get
            {
                // if the result KTable is materialized, use the materialized store to return getter value;
                // otherwise rely on the parent getter and apply join on-the-fly
                if (!string.IsNullOrEmpty(queryableName))
                {
                    return new KTableMaterializedValueGetterSupplier<K, VR>(queryableName);
                }
                else
                {
                    var stores = leftParent.View.StoreNames.Concat(rightParent.View.StoreNames).Distinct().ToArray();
                    return new GenericKTableValueGetterSupplier<K, VR>(stores, leftParent.View.Get());
                }
            }
        }

        public void EnableSendingOldValues()
        {
            leftParent.EnableSendingOldValues();
            rightParent.EnableSendingOldValues();
            sendOldValues = true;
        }

        public IProcessor<K, Change<VR>> Get()
            => new KTableKTableJoinMergeProcessor<K, VR>(queryableName, sendOldValues);
    }
}