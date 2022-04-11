#!/bin/bash

tags=("Version" "PackageVersion" "AssemblyVersion" "FileVersion")
files=(
  "core/Streamiz.Kafka.Net.csproj"
  "metrics/Streamiz.Kafka.Net.Metrics.Prometheus/Streamiz.Kafka.Net.Metrics.Prometheus.csproj"
  "serdes/Streamiz.Kafka.Net.SchemaRegistry.SerDes/Streamiz.Kafka.Net.SchemaRegistry.SerDes.csproj"
  "serdes/Streamiz.Kafka.Net.SchemaRegistry.SerDes.Avro/Streamiz.Kafka.Net.SchemaRegistry.SerDes.Avro.csproj"
  "serdes/Streamiz.Kafka.Net.SchemaRegistry.SerDes.Protobuf/Streamiz.Kafka.Net.SchemaRegistry.SerDes.Protobuf.csproj"
  )

for tag in ${tags[@]}; do
  for file in ${files[@]}; do
  sed -i '' -e "s/<$tag>\(.*\)<\/$tag>/<$tag>$1<\/$tag>/g" $file
  done
  echo "$tag done"
done


# sed -i '' -e "s/<$tag>\(.*\)<\/$tag>/<$tag>$1<\/$tag>/g" core/Streamiz.Kafka.Net.csproj
 # sed -i '' -e "s/<$tag>\(.*\)<\/$tag>/<$tag>$1<\/$tag>/g" metrics/Streamiz.Kafka.Net.Metrics.Prometheus/Streamiz.Kafka.Net.Metrics.Prometheus.csproj
 # #sed -i '' -e "s/<$tag>\(.*\)<\/$tag>/<$tag>$1<\/$tag>/g" serdes/Streamiz.Kafka.Net.SchemaRegistry.SerDes/Streamiz.Kafka.Net.SchemaRegistry.SerDes.csproj
 # sed -i '' -e "s/<$tag>\(.*\)<\/$tag>/<$tag>$1<\/$tag>/g" serdes/Streamiz.Kafka.Net.SchemaRegistry.SerDes.Avro/Streamiz.Kafka.Net.SchemaRegistry.SerDes.Avro.csproj
  #sed -i '' -e "s/<$tag>\(.*\)<\/$tag>/<$tag>$1<\/$tag>/g" serdes/Streamiz.Kafka.Net.SchemaRegistry.SerDes.Protobuf/Streamiz.Kafka.Net.SchemaRegistry.SerDes.Protobuf.csproj
 