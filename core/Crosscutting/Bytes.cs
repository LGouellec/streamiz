using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;

namespace Streamiz.Kafka.Net.Crosscutting
{
    internal class ReverseBytesComparer : IEqualityComparer<Bytes>, IComparer<Bytes>
    {
        private readonly BytesComparer sortComparer = new BytesComparer();

        public int Compare(Bytes x, Bytes y)
            => sortComparer.Compare(y, x);

        public bool Equals(Bytes x, Bytes y)
            => sortComparer.Equals(x, y);

        public int GetHashCode(Bytes obj)
            => sortComparer.GetHashCode(obj);
    }

    internal class BytesComparer : IEqualityComparer<Bytes>, IComparer<Bytes>
    {
        public bool Equals(Bytes x, Bytes y)
        {
            return x.Equals(y);
        }

        public int GetHashCode(Bytes obj)
        {
            return obj.GetHashCode();
        }

        public int Compare(Bytes x, Bytes y)
            => Compare(x.Get, 0, x.Get.Length, y.Get, 0, y.Get.Length);

        internal static int Compare(byte[] buffer1, byte[] buffer2)
            => Compare(buffer1, 0, buffer1.Length, buffer2, 0, buffer2.Length);

        private static int Compare(byte[] buffer1, int offset1, int length1,
                            byte[] buffer2, int offset2, int length2)
        {
            if (buffer1 == buffer2 &&
                    offset1 == offset2 &&
                    length1 == length2)
            {
                return 0;
            }

            int end1 = offset1 + length1;
            int end2 = offset2 + length2;
            for (int i = offset1, j = offset2; i < end1 && j < end2; i++, j++)
            {
                int a = buffer1[i] & 0xff;
                int b = buffer2[j] & 0xff;
                if (a != b)
                {
                    return a - b;
                }
            }
            return length1 - length2;
        }

    }

    /// <summary>
    /// Utility class that handles immutable byte arrays.
    /// </summary>
    public class Bytes : IEquatable<Bytes>, IComparable<Bytes>
    {
        /// <summary>
        /// Get the data from the Bytes.
        /// </summary>
        public virtual byte[] Get { get; }

        /// <summary>
        /// Create a Bytes using the byte array.
        /// </summary>
        /// <param name="bytes">This array becomes the backing storage for the object.</param>
        [Obsolete("Will be removed last release version")]
        public Bytes(byte[] bytes)
        {
            Get = bytes;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override int GetHashCode()
        {
            return new BigInteger(Get).GetHashCode();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public override bool Equals(object obj)
        {
            return obj is Bytes && Equals((Bytes)obj);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="other"></param>
        /// <returns></returns>
        public bool Equals(Bytes other)
        {
            return Get.SequenceEqual(other.Get);
        }

        internal static Bytes Wrap(byte[] bytes)
        {
            if (bytes == null)
                return null;
            return new Bytes(bytes);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="other"></param>
        /// <returns></returns>
        public virtual int CompareTo(Bytes other)
        {
            BytesComparer comparer = new BytesComparer();
            if (other == null || other.Get == null)
                return 1;
            if (Get == null)
                return -1;
            return comparer.Compare(this, other);
        }
    }
}
