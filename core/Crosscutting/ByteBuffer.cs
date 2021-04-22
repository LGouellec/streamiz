using System;
using System.IO;

namespace Streamiz.Kafka.Net.Crosscutting
{
    internal class ByteBuffer : IDisposable
    {
        private readonly byte[] bytes;
        private readonly MemoryStream stream;
        private readonly BinaryWriter writer;
        private readonly BinaryReader reader;
        private readonly bool isReadMode;

        private ByteBuffer(byte[] bytes)
        {
            this.bytes = bytes;
            stream = new MemoryStream(this.bytes);
            reader = new BinaryReader(stream);
            isReadMode = true;
        }

        private ByteBuffer(int capacity)
        {
            stream = new MemoryStream(capacity);
            writer = new BinaryWriter(stream);
            isReadMode = false;
        }

        internal static ByteBuffer Build(byte[] bytes)
            => new ByteBuffer(bytes);

        internal static ByteBuffer Build(int capacity)
            => new ByteBuffer(capacity);

        public void Dispose()
        {
            reader?.Dispose();
            writer?.Dispose();
            stream?.Dispose();
        }

        #region Write Operation

        public void Put(byte[] bytes)
        {
            writer.BaseStream.Seek(0, SeekOrigin.End);
            writer.Write(bytes);
        }

        public void PutLong(long @long)
        {
            writer.BaseStream.Seek(0, SeekOrigin.End);
            writer.Write(@long);
        }

        public void PutInt(int @int)
        {
            writer.BaseStream.Seek(0, SeekOrigin.End);
            writer.Write(@int);
        }

        #endregion

        #region ReadOperation

        public long GetLong(int offset)
        {
            reader.BaseStream.Seek(offset, SeekOrigin.Begin);
            return reader.ReadInt64();
        }

        public int GetInt(int offset)
        {
            reader.BaseStream.Seek(offset, SeekOrigin.Begin);
            return reader.ReadInt32();
        }

        #endregion

        public byte[] ToArray()
            => stream.ToArray();
    }
}
