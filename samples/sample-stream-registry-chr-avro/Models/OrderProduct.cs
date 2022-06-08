namespace sample_stream_registry_chr_avro.Models
{
    public class OrderProduct
    {
        public int OrderId { get; set; }
        public float Price { get; set; }
        public int ProductId { get; set; }
        public string? ProductName { get; set; }
        public float ProductPrice { get; set; }
    }
}
