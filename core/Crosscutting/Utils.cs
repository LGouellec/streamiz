using System;
using System.Collections.Generic;
using System.Text;
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
    }
}
