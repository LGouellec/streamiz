using System;
using System.Collections.Generic;
using System.Text;

namespace Streamiz.Kafka.Net.Errors
{
    public class ProductionException : Exception
    {
        public ProductionException(string message) 
            : base(message)
        {
        }
    }
}
