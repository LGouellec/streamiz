using Newtonsoft.Json;
using Streamiz.Kafka.Net.SerDes;
using System.Collections.Generic;
using System.Text;

namespace sample_test_driver
{
    public class DictionarySerDes : AbstractSerDes<Dictionary<char, int>>
    {
        public override Dictionary<char, int> Deserialize(byte[] data)
        {
            var s = Encoding.UTF8.GetString(data);
            return JsonConvert.DeserializeObject<Dictionary<char, int>>(s);
        }

        public override byte[] Serialize(Dictionary<char, int> data)
        {
            return Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(data, Formatting.Indented));
        }
    }
}
