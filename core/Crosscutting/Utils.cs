using Streamiz.Kafka.Net.Stream;
using System;
using System.Globalization;
using System.Text.RegularExpressions;

namespace Streamiz.Kafka.Net.Crosscutting
{
    internal static class Utils
    {
        private static readonly string HOST_PORT_PATTERN = ".*?\\[?([0-9a-zA-Z\\-%._:]*)\\]?:([0-9]+)";

        public static string GetHost(string address)
        {
            Regex regex = new Regex(HOST_PORT_PATTERN);
            var match = regex.Match(address);
            return match.Success ? match.Groups[0].Value : null;
        }

        public static int? GetPort(string address)
        {
            Regex regex = new Regex(HOST_PORT_PATTERN);
            var match = regex.Match(address);
            return match.Success ? Convert.ToInt32(match.Groups[1].Value) : (int?)null;
        }

        public static IValueJoiner<V2, V1, VR> Reverse<V1, V2, VR>(this IValueJoiner<V1, V2, VR> joiner)
        {
            return new WrappedValueJoiner<V2, V1, VR>((v2, v1) => joiner.Apply(v1, v2));
        }
        
        public static bool IsNumeric(object expression, out Double number)
        {
            if (expression == null)
            {
                number = Double.NaN;
                return false;
            }

            return Double.TryParse( Convert.ToString( expression
                    , CultureInfo.InvariantCulture)
                , System.Globalization.NumberStyles.Any
                , NumberFormatInfo.InvariantInfo
                , out number);
        }

        public static void CheckIfNotNull(object parameter, string nameAccessor)
        {
            if(parameter == null)
                throw new ArgumentException($"{nameAccessor} must not be null");
        }
    }
}
