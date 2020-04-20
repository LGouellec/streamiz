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

#pragma warning disable CS1591 // Commentaire XML manquant pour le type ou le membre visible publiquement
        public override int GetHashCode()
#pragma warning restore CS1591 // Commentaire XML manquant pour le type ou le membre visible publiquement
        {
            return new BigInteger(this.Get).GetHashCode();
        }

#pragma warning disable CS1591 // Commentaire XML manquant pour le type ou le membre visible publiquement
        public bool Equals(Bytes other)
#pragma warning restore CS1591 // Commentaire XML manquant pour le type ou le membre visible publiquement
        {
            return this.Get.SequenceEqual(other.Get);
        }
    }
}
