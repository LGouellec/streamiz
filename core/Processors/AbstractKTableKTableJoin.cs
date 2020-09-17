//using Streamiz.Kafka.Net.Stream;
//using Streamiz.Kafka.Net.Table;
//using Streamiz.Kafka.Net.Table.Internal;

//namespace Streamiz.Kafka.Net.Processors
//{
//    internal abstract class AbstractKTableKTableJoin<K, VR, V1, V2> : IKTableProcessorSupplier<K, V1, VR>
//    {
//        private bool sendOldValues = false;
//        private readonly IKTableGetter<K, V1> table1;
//        private readonly IKTableGetter<K, V2> table2;
//        private readonly IValueJoiner<V1, V2, VR> valueJoiner;

//        public abstract IKTableValueGetterSupplier<K, VR> View { get; }
//        public abstract IProcessor<K, Change<V1>> Get();


//        public AbstractKTableKTableJoin(
//            IKTableGetter<K, V1> table1,
//            IKTableGetter<K, V2> table2,
//            IValueJoiner<V1, V2, VR> valueJoiner)
//        {
//            this.table1 = table1;
//            this.table2 = table2;
//            this.valueJoiner = valueJoiner;
//        }


//        public void EnableSendingOldValues()
//        {
//            table1.EnableSendingOldValues();
//            table2.EnableSendingOldValues();
//            sendOldValues = true;
//        }
//    }
//}
