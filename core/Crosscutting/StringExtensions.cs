using Streamiz.Kafka.Net.SerDes;
using System;
using System.Collections.Generic;
using System.Text;

namespace Streamiz.Kafka.Net.Crosscutting
{
    public static class StringExtensions
    {
        public static ISerDes CreateSerDes(this string value)
        {
            if (!string.IsNullOrEmpty(value))
            {
                var type = Type.GetType(value, false);
                return type != null ? Activator.CreateInstance(type) as ISerDes : null;
            }
            else
                return null;
        }
    }
}
