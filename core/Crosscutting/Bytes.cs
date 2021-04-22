using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;

namespace Streamiz.Kafka.Net.Crosscutting
{
    internal class BytesComparer : IEqualityComparer<Bytes>
    {
        public bool Equals(Bytes x, Bytes y)
        {
            return x.Equals(y);
        }

        public int GetHashCode(Bytes obj)
        {
            return obj.GetHashCode();
        }
    }

    /// <summary>
    /// Utility class that handles immutable byte arrays.
    /// </summary>
    public sealed class Bytes : IEquatable<Bytes>
    {
        /// <summary>
        /// Get the data from the Bytes.
        /// </summary>
        public byte[] Get { get; }

        /// <summary>
        /// Create a Bytes using the byte array.
        /// </summary>
        /// <param name="bytes">This array becomes the backing storage for the object.</param>
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
    }
}
