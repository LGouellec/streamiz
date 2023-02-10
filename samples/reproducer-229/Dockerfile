FROM mcr.microsoft.com/dotnet/runtime:6.0-alpine AS base
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:6.0-alpine AS build
WORKDIR /src
COPY ["reproducer-229.csproj", "reproducer-229/"]
RUN dotnet restore "reproducer-229/reproducer-229.csproj"
COPY . reproducer-229/
WORKDIR "/src/reproducer-229"
RUN dotnet build "reproducer-229.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "reproducer-229.csproj" -c Release -o /app/publish

FROM base AS final
ARG ROCKSDB_VERSION=v7.4.3
WORKDIR /app
RUN apk add --no-cache rocksdb libstdc++
RUN ln -s /usr/lib/librocksdb.so.7 /usr/lib/librocksdb.so
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "reproducer-229.dll"]
