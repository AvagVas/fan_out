# Build context is the repository root.
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY Directory.Build.props ./
COPY src/Poc.Contracts/Poc.Contracts.csproj src/Poc.Contracts/
COPY src/Poc.Kafka/Poc.Kafka.csproj src/Poc.Kafka/
COPY src/Poc.Enricher.Streams/Poc.Enricher.Streams.csproj src/Poc.Enricher.Streams/
RUN dotnet restore src/Poc.Enricher.Streams/Poc.Enricher.Streams.csproj
COPY src/ src/
RUN dotnet publish src/Poc.Enricher.Streams/Poc.Enricher.Streams.csproj -c Release -o /app --no-restore

FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS runtime
WORKDIR /app
COPY --from=build /app ./
ENV ASPNETCORE_URLS=http://+:8080
# Durable join state lives here; mount a volume in compose so it survives restarts.
ENV Enricher__StatePath=/data/enricher.db
EXPOSE 8080
ENTRYPOINT ["dotnet", "Poc.Enricher.Streams.dll"]
