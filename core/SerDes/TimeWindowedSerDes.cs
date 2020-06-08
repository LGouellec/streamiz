using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Streamiz.Kafka.Net.SerDes
{
    internal class TimeWindowedSerDes<T> : AbstractSerDes<Windowed<T>>
    {
        private static readonly int timestampSize = 8;
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

            byte[] bytes = new byte[data.Length - timestampSize];

            using (var mStream = new MemoryStream(data))
            {
                using (var bufferStream = new BufferedStream(mStream))
                {
                    byte[] time = new byte[timestampSize];

                    bufferStream.Read(bytes, 0, bytes.Length);
                    bufferStream.Read(time, bytes.Length, timestampSize);

                    T key = innerSerdes.Deserialize(bytes);
                    long start = BitConverter.ToInt64(time);

                    return new Windowed<T>(key, new TimeWindow(start, start + windowSize ));
                }
            }
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
