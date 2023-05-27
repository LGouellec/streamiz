#!/bin/bash

for file in `find . -type f -name "*.csproj"`; do
  sed -i '' -e "s/Include=\"Confluent\(.*\)\" Version=\"\(.*\)\"/Include=\"Confluent\1\" Version=\"$1\"/g" $file
done