using System;
using System.IO;
using System.Linq;

namespace Streamiz.Kafka.Net.Crosscutting
{
    internal class ByteBuffer : IDisposable
    {
        private readonly bool bigEndian;
        private readonly MemoryStream stream;
        private readonly BinaryWriter writer;
        private readonly BinaryReader reader;

        private ByteBuffer(byte[] bytes, bool bigEndian)
        {
            this.bigEndian = bigEndian;
            stream = new MemoryStream(bytes);
            reader = new BinaryReader(stream);
            writer = new BinaryWriter(stream);
        }

        private ByteBuffer(int capacity, bool bigEndian)
        {
            this.bigEndian = bigEndian;
            stream = new MemoryStream(capacity);
            reader = new BinaryReader(stream);
            writer = new BinaryWriter(stream);
        }

        internal static ByteBuffer Build(byte[] bytes, bool bigEndian = true)
            => new ByteBuffer(bytes, bigEndian);

        internal static ByteBuffer Build(int capacity, bool bigEndian = true)
            => new ByteBuffer(capacity, bigEndian);

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
            if (bytes != null)
            {
                writer.BaseStream.Seek(0, SeekOrigin.End);
                writer.Write(bytes);
            }

            return this;
        }

        public ByteBuffer PutLong(long @long)
        {
            writer.BaseStream.Seek(0, SeekOrigin.End);
            var bytes = BitConverter.GetBytes(@long);
            
            if(BitConverter.IsLittleEndian && bigEndian)
                bytes = bytes.Reverse().ToArray();
            
            writer.Write(bytes);
            return this;
        }

        public ByteBuffer PutInt(int @int)
        {
            writer.BaseStream.Seek(0, SeekOrigin.End);
            var bytes = BitConverter.GetBytes(@int);
            
            if(BitConverter.IsLittleEndian && bigEndian)
                bytes = bytes.Reverse().ToArray();

            writer.Write(bytes);
            return this;
        }

        #endregion

        #region ReadOperation

        public long GetLong(int offset)
        {
            reader.BaseStream.Seek(offset, SeekOrigin.Begin);
            var bytes = reader.ReadBytes(sizeof(long));
            
            if (BitConverter.IsLittleEndian && bigEndian)
                bytes = bytes.Reverse().ToArray();

            return BitConverter.ToInt64(bytes, 0);
        }

        public int GetInt(int offset)
        {
            reader.BaseStream.Seek(offset, SeekOrigin.Begin);
            var bytes = reader.ReadBytes(sizeof(int));
            
            if (BitConverter.IsLittleEndian && bigEndian)
                bytes = bytes.Reverse().ToArray();
            
            return BitConverter.ToInt32(bytes, 0);
        }

        public byte[] GetBytes(int offset, int size)
        {
            reader.BaseStream.Seek(offset, SeekOrigin.Begin);
            return reader.ReadBytes(size);
        }

        #endregion

        public byte[] ToArray()
            => stream.ToArray();
    }
}
