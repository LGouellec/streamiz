namespace sample_stream_registry_chr_avro.Models
{
    public class OrderAgg
    {
        public int OrderId { get; set; }
        public float Price { get; set; }
        public int ProductId { get; set; }
        public float TotalPrice { get; set; }
    }
}
