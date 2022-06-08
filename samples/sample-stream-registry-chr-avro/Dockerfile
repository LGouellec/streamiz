FROM mcr.microsoft.com/dotnet/runtime:6.0 AS base
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:6.0 AS build
WORKDIR /src
COPY . .
COPY ["samples/sample-stream-registry-chr-avro/sample-stream-registry-chr-avro.csproj", "sample-stream-registry-chr-avro/"]
RUN dotnet restore "samples/sample-stream-registry-chr-avro/sample-stream-registry-chr-avro.csproj"
COPY . .
RUN dotnet build "samples/sample-stream-registry-chr-avro/sample-stream-registry-chr-avro.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "samples/sample-stream-registry-chr-avro/sample-stream-registry-chr-avro.csproj" -c Release -o /app/publish

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "sample-stream-registry-chr-avro.dll"]
