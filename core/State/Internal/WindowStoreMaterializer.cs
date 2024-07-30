using System;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.State.Supplier;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Kafka.Net.State.Internal
{
    internal class WindowStoreMaterializer<K, V, W, WS>
        where WS : IStateStore
        where W : Window
    {
        protected readonly WindowOptions<W> windowsOptions;
        protected readonly Materialized<K, V, IWindowStore<Bytes, byte[]>> materializedInternal;

        public WindowStoreMaterializer(WindowOptions<W> windowsOptions, Materialized<K, V, IWindowStore<Bytes, byte[]>> materializedInternal)
        {
            this.windowsOptions = windowsOptions;
            this.materializedInternal = materializedInternal;
        }

        public IStoreBuilder<WS> Materialize()
        {
            var supplier = GetWindowSupplier();
            var builder = GetWindowStoreBuilder(supplier);

            if (materializedInternal.LoggingEnabled)
                builder.WithLoggingEnabled(materializedInternal.TopicConfig);
            else
                builder.WithLoggingDisabled();

            if (materializedInternal.CachingEnabled)
                builder.WithCachingEnabled(materializedInternal.CacheSize);

            return builder;
        }

        protected IWindowBytesStoreSupplier GetWindowSupplier()
        {
            IWindowBytesStoreSupplier supplier = (IWindowBytesStoreSupplier)materializedInternal.StoreSupplier;
            if (supplier == null)
            {
                if (windowsOptions.Size + windowsOptions.GracePeriodMs > materializedInternal.Retention.TotalMilliseconds)
                    throw new ArgumentException($"The retention period of the window store { materializedInternal.StoreName } must be no smaller than its window size plus the grace period. Got size=[{windowsOptions.Size}], grace=[{windowsOptions.GracePeriodMs}], retention=[{materializedInternal.Retention.TotalMilliseconds}].");

                supplier = Stores.DefaultWindowStore(
                    materializedInternal.StoreName,
                    materializedInternal.Retention,
                    TimeSpan.FromMilliseconds(windowsOptions.Size));
            }
            else
                supplier.WindowSize = !supplier.WindowSize.HasValue ? windowsOptions.Size : supplier.WindowSize.Value;

            return supplier;
        }

        protected virtual IStoreBuilder<WS> GetWindowStoreBuilder(IWindowBytesStoreSupplier supplier)
        {
            return (IStoreBuilder<WS>)
                Stores.WindowStoreBuilder(supplier, materializedInternal.KeySerdes, materializedInternal.ValueSerdes);
        }
    }
}