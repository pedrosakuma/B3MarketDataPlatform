FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build

# AOT requires clang and build tools
RUN apt-get update && apt-get install -y --no-install-recommends \
    clang zlib1g-dev && rm -rf /var/lib/apt/lists/*

WORKDIR /src

COPY Directory.Build.props global.json SbeB3UmdfConsumer.slnx ./
COPY schemas/ schemas/
COPY src/ src/

RUN dotnet restore src/B3.Umdf.ConsoleApp/B3.Umdf.ConsoleApp.csproj
RUN dotnet publish src/B3.Umdf.ConsoleApp/B3.Umdf.ConsoleApp.csproj \
    -c Release -o /app --no-restore

FROM mcr.microsoft.com/dotnet/runtime-deps:10.0-noble-chiseled AS runtime

WORKDIR /app
COPY --from=build /app/B3.Umdf.ConsoleApp .

EXPOSE 8080

STOPSIGNAL SIGTERM

HEALTHCHECK --interval=10s --timeout=3s --start-period=30s --retries=3 \
    CMD ["/app/B3.Umdf.ConsoleApp", "--health-check"]

ENTRYPOINT ["/app/B3.Umdf.ConsoleApp"]
