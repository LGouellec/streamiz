FROM mcr.microsoft.com/dotnet/runtime:6.0-alpine AS base
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:6.0-alpine AS build
WORKDIR /src
COPY ["sample-stream.csproj", "sample-stream/"]
RUN dotnet restore "sample-stream/sample-stream.csproj"
COPY . .
COPY ["Program.cs", "sample-stream/"]
COPY ["log4net.config", "sample-stream/"]
WORKDIR "/src/sample-stream"
RUN dotnet build "sample-stream.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "sample-stream.csproj" -c Release -o /app/publish

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
RUN mkdir /app/store
ENTRYPOINT ["dotnet", "sample-stream.dll"]
