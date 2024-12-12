FROM mcr.microsoft.com/dotnet/runtime:5.0 AS base
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:5.0 AS build
WORKDIR /src
COPY ["samples/sample-stream-demo/sample-stream-demo.csproj", "sample-stream-demo/"]
RUN dotnet restore "samples/sample-stream-demo/sample-stream-demo.csproj"
COPY . .
WORKDIR "/src/sample-stream-demo"
RUN dotnet build "sample-stream-demo.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "sample-stream-demo.csproj" -c Release -o /app/publish

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "sample-stream-demo.dll"]
