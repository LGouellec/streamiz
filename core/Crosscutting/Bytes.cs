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
    public class Bytes : IEquatable<Bytes>
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
            this.Get = bytes;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override int GetHashCode()
        {
            return new BigInteger(this.Get).GetHashCode();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="other"></param>
        /// <returns></returns>
        public override bool Equals(object other)
        {
            return other is Bytes && Equals((Bytes)other);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="other"></param>
        /// <returns></returns>
        public bool Equals(Bytes other)
        {
            return this.Get.SequenceEqual(((Bytes)other).Get);
        }
    }
}
