using System;
using System.IO;

namespace Streamiz.Kafka.Net.Crosscutting
{
    internal class ByteBuffer : IDisposable
    {
        private readonly MemoryStream stream;
        private readonly BinaryWriter writer;
        private readonly BinaryReader reader;

        private ByteBuffer(byte[] bytes)
        {
            stream = new MemoryStream(bytes);
            reader = new BinaryReader(stream);
            writer = new BinaryWriter(stream);
        }

        private ByteBuffer(int capacity)
        {
            stream = new MemoryStream(capacity);
            reader = new BinaryReader(stream);
            writer = new BinaryWriter(stream);
        }

        internal static ByteBuffer Build(byte[] bytes)
            => new ByteBuffer(bytes);

        internal static ByteBuffer Build(int capacity)
            => new ByteBuffer(capacity);

        public void Dispose()
        {
            reader.Dispose();
            writer.Dispose();
            stream.Dispose();
        }

        #region Write Operation

        public ByteBuffer Put(byte b)
        {
            writer.BaseStream.Seek(0, SeekOrigin.End);
            writer.Write(b);
            return this;
        }

        public ByteBuffer Put(byte[] bytes)
        {
            writer.BaseStream.Seek(0, SeekOrigin.End);
            writer.Write(bytes);
            return this;
        }

        public ByteBuffer PutLong(long @long)
        {
            writer.BaseStream.Seek(0, SeekOrigin.End);
            writer.Write(@long);
            return this;
        }

        public ByteBuffer PutInt(int @int)
        {
            writer.BaseStream.Seek(0, SeekOrigin.End);
            writer.Write(@int);
            return this;
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
