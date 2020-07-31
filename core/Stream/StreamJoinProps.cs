using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State.Supplier;

namespace Streamiz.Kafka.Net.Stream
{
    public class StreamJoinProps
    {
        protected readonly WindowBytesStoreSupplier thisStoreSupplier;
        protected readonly WindowBytesStoreSupplier otherStoreSupplier;
        protected readonly string name;
        protected readonly string storeName;

        internal StreamJoinProps(WindowBytesStoreSupplier thisStoreSupplier, WindowBytesStoreSupplier otherStoreSupplier, string name, string storeName)
        {
            this.thisStoreSupplier = thisStoreSupplier;
            this.otherStoreSupplier = otherStoreSupplier;
            this.name = name;
            this.storeName = storeName;
        }

        public string Name => name;
        public string StoreName => storeName;
        public WindowBytesStoreSupplier LeftStoreSupplier => thisStoreSupplier;
        public WindowBytesStoreSupplier RightStoreSupplier => otherStoreSupplier;


        public static StreamJoinProps With(WindowBytesStoreSupplier storeSupplier, WindowBytesStoreSupplier otherStoreSupplier)
        {
            return new StreamJoinProps(
                storeSupplier,
                otherStoreSupplier,
                null,
                null
            );
        }

        public static StreamJoinProps<K, V1, V2> With<K, V1, V2>(ISerDes<K> keySerde,
                                                      ISerDes<V1> valueSerde,
                                                      ISerDes<V2> otherValueSerde)
        {
            return new StreamJoinProps<K, V1, V2>(
                keySerde,
                valueSerde,
                otherValueSerde,
                null,
                null,
                null,
                null
            );
        }

        public static StreamJoinProps As(string storeName)
        {
            return new StreamJoinProps(
                null,
                null,
                null,
                storeName
            );
        }

        public static StreamJoinProps As(string name, string storeName)
        {
            return new StreamJoinProps(
                null,
                null,
                name,
                storeName
            );
        }

        internal static StreamJoinProps<T1, T2, T3> From<T1, T2, T3>(StreamJoinProps props)
        {
            if (props != null)
            {
                return new StreamJoinProps<T1, T2, T3>(null, null, null, props.LeftStoreSupplier, props.RightStoreSupplier, props.Name, props.StoreName);
            }
            else
            {
                return new StreamJoinProps<T1, T2, T3>(null, null, null, null, null, null, null);
            }
        }
    }

    public class StreamJoinProps<K, V1, V2> : StreamJoinProps
    {
        internal StreamJoinProps(
            ISerDes<K> keySerdes,
            ISerDes<V1> valueSerdes,
            ISerDes<V2> otherValueSerdes,
            WindowBytesStoreSupplier thisStoreSupplier,
            WindowBytesStoreSupplier otherStoreSupplier,
            string name,
            string storeName)
            : base(thisStoreSupplier, otherStoreSupplier, name, storeName)
        {
            KeySerdes = keySerdes;
            LeftValueSerdes = valueSerdes;
            RightValueSerdes = otherValueSerdes;
        }

        public ISerDes<K> KeySerdes { get; internal set; }
        public ISerDes<V1> LeftValueSerdes { get; internal set; }
        public ISerDes<V2> RightValueSerdes { get; internal set; }
    }
}
