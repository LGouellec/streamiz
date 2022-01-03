FROM mcr.microsoft.com/dotnet/aspnet:5.0 AS base
WORKDIR /app
EXPOSE 80
EXPOSE 443

FROM mcr.microsoft.com/dotnet/sdk:5.0 AS build
WORKDIR /src
COPY ["samples/sample-micro-service/sample-micro-service.csproj", "sample-micro-service/"]
RUN dotnet restore "samples/sample-micro-service/sample-micro-service.csproj"
COPY . .
WORKDIR "/src/sample-micro-service"
RUN dotnet build "sample-micro-service.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "sample-micro-service.csproj" -c Release -o /app/publish

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "sample-micro-service.dll"]
