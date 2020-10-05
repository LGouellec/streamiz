using Streamiz.Kafka.Net.Processors;
using System;
using System.Collections.Generic;
using System.Text;

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

        public IKTableValueGetterSupplier<K, VR> View => throw new NotImplementedException();

        public void EnableSendingOldValues()
        {
            throw new NotImplementedException();
        }

        public IProcessor<K, Change<VR>> Get()
        {
            throw new NotImplementedException();
        }
    }
}