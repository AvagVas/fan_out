# Build context is the repository root.
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY Directory.Build.props ./
COPY src/Poc.Contracts/Poc.Contracts.csproj src/Poc.Contracts/
COPY src/Poc.Kafka/Poc.Kafka.csproj src/Poc.Kafka/
COPY src/Poc.Proxy.Api/Poc.Proxy.Api.csproj src/Poc.Proxy.Api/
RUN dotnet restore src/Poc.Proxy.Api/Poc.Proxy.Api.csproj
COPY src/ src/
RUN dotnet publish src/Poc.Proxy.Api/Poc.Proxy.Api.csproj -c Release -o /app --no-restore

FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS runtime
WORKDIR /app
COPY --from=build /app ./
ENV ASPNETCORE_URLS=http://+:8080
EXPOSE 8080
ENTRYPOINT ["dotnet", "Poc.Proxy.Api.dll"]
