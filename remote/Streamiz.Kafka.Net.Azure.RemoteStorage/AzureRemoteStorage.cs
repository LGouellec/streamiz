using System.Runtime.CompilerServices;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Table;

[assembly: InternalsVisibleTo("Streamiz.Kafka.Net.Tests, PublicKey=00240000048000009400000006020000002400005253413100040000010001000d9d4a8e90a3b987f68f047ec499e5a3405b46fcad30f52abadefca93b5ebce094d05976950b38cc7f0855f600047db0a351ede5e0b24b9d5f1de6c59ab55dee145da5d13bb86f7521b918c35c71ca5642fc46ba9b04d4900725a2d4813639ff47898e1b762ba4ccd5838e2dd1e1664bd72bf677d872c87749948b1174bd91ad")]
[assembly: InternalsVisibleTo("DynamicProxyGenAssembly2, PublicKey=0024000004800000940000000602000000240000525341310004000001000100c547cac37abd99c8db225ef2f6c8a3602f3b3606cc9891605d02baa56104f4cfc0734aa39b93bf7852f7d9266654753cc297e7d2edfe0bac1cdcf9f717241550e0a7b191195b7667bb4f64bcb8e2121380fd1d9d46ad2d92d2d15605093924cceaf74c4861eff62abf69b9291ed0a340e113be11e6a7d3113e92484cf7045cc7")]


namespace Streamiz.Kafka.Net.Azure.RemoteStorage
{
    public static class AzureRemoteStorage
    {
        public static Materialized<K, V, IKeyValueStore<Bytes, byte[]>> @As<K, V>( 
            AzureRemoteStorageOptions azureRemoteStorageOptions = null,
            string storeName = null)
        {
            Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized
                = Materialized<K, V, IKeyValueStore<Bytes, byte[]>>
                    .Create(new AzureRemoteStorageSupplier(azureRemoteStorageOptions, storeName))
                    .WithName(storeName)
                    .WithLoggingDisabled(); // explicitly disabled logging
            
            return materialized;
        }
        
        public static Materialized<K, V, IKeyValueStore<Bytes, byte[]>> @As<K, V>(
            string storeName = null)
        {
            Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized
                = Materialized<K, V, IKeyValueStore<Bytes, byte[]>>
                    .Create(new AzureRemoteStorageSupplier(null, storeName))
                    .WithName(storeName)
                    .WithLoggingDisabled(); // explicitly disabled logging
            
            return materialized;
        }
        
        public static Materialized<K, V, IKeyValueStore<Bytes, byte[]>> @As<K, V>()
        {
            Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized
                = Materialized<K, V, IKeyValueStore<Bytes, byte[]>>
                    .Create(new AzureRemoteStorageSupplier(null, null))
                    .WithName(null)
                    .WithLoggingDisabled(); // explicitly disabled logging
            
            return materialized;
        }
    }
}