using Confluent.Kafka;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.State.Helper;
using Streamiz.Kafka.Net.Stream;

namespace Streamiz.Kafka.Net.SerDes
{
    /// <summary>
    /// Full time window serdes
    /// </summary>
    /// <typeparam name="T">Value type</typeparam>
    public class TimeWindowedSerDes<T> : AbstractSerDes<Windowed<T>>
    {
        private readonly ISerDes<T> innerSerdes;
        private readonly long windowSize;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="innerSerdes">Inner value serdes</param>
        /// <param name="windowSize">Window size in ms</param>
        public TimeWindowedSerDes(ISerDes<T> innerSerdes, long windowSize)
        {
            this.innerSerdes = innerSerdes;
            this.windowSize = windowSize;
        }

        /// <summary>
        /// Deserialize data array to <see cref="Windowed{K}"/>
        /// </summary>
        /// <param name="data">Data array</param>
        /// <param name="context">serialization context</param>
        /// <returns>Return <see cref="Windowed{K}"/> instance</returns>
        public override Windowed<T> Deserialize(byte[] data, SerializationContext context)
        {
            if (data == null || data.Length == 0)
                return null;

            long start = WindowKeyHelper.ExtractStoreTimestamp(data);

            return new Windowed<T>(
                innerSerdes.Deserialize(WindowKeyHelper.ExtractStoreKeyBytes(data), context),
                new TimeWindow(start, start + windowSize));
        }

        /// <summary>
        /// Serialize an <see cref="Windowed{K}"/> instance to byte array
        /// </summary>
        /// <param name="data">Instance to serialize</param>
        /// <param name="context">serialization context</param>
        /// <returns>Return an array of byte</returns>
        public override byte[] Serialize(Windowed<T> data, SerializationContext context)
        {
            if (data == null)
                return null;

            var bytesKey = innerSerdes.Serialize(data.Key, context);
            var bytes = WindowKeyHelper.ToStoreKeyBinary(bytesKey, data.Window.StartMs, 0);
            return bytes.Get;
        }

        /// <summary>
        /// Initialize method with a current context which contains <see cref="IStreamConfig"/>.
        /// Can be used to initialize the serdes according to some parameters present in the configuration such as the schema.registry.url
        /// </summary>
        /// <param name="context">SerDesContext with stream configuration</param>
        public override void Initialize(SerDesContext context)
        {
            base.Initialize(context);
            innerSerdes.Initialize(context);
        }
    }
}
