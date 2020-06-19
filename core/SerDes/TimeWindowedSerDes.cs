using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.State.Helper;
using Streamiz.Kafka.Net.Stream;
using System;
using System.IO;

namespace Streamiz.Kafka.Net.SerDes
{
    public class TimeWindowedSerDes<T> : AbstractSerDes<Windowed<T>>
    {
        private readonly ISerDes<T> innerSerdes;
        private readonly long windowSize;

        public TimeWindowedSerDes(ISerDes<T> innerSerdes, long windowSize)
        {
            this.innerSerdes = innerSerdes;
            this.windowSize = windowSize;
        }

        public override Windowed<T> Deserialize(byte[] data)
        {
            if (data == null || data.Length == 0)
                return null;

            var start = data.ExtractStoreTimestamp();

            return new Windowed<T>(
                innerSerdes.Deserialize(data.ExtractStoreKeyBytes()),
                new TimeWindow(start, start + windowSize));
        }

        public override byte[] Serialize(Windowed<T> data)
        {
            if (data == null)
                return null;

            using (var mStream = new MemoryStream())
            {
                using (var bufferStream = new BufferedStream(mStream))
                {
                    bufferStream.Write(innerSerdes.Serialize(data.Key));
                    bufferStream.Write(BitConverter.GetBytes(data.Window.StartMs));
                }
                return mStream.ToArray();
            }
        }
    }
}
