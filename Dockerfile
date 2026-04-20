FROM mcr.microsoft.com/dotnet/sdk:10.0 AS tools

# .NET diagnostic tools: shipped inside the runtime image so we can
# `docker exec consumer dotnet-trace ...` without a sidecar.
RUN dotnet tool install --tool-path /tools dotnet-trace \
 && dotnet tool install --tool-path /tools dotnet-counters \
 && dotnet tool install --tool-path /tools dotnet-dump \
 && dotnet tool install --tool-path /tools dotnet-gcdump \
 && dotnet tool install --tool-path /tools dotnet-stack

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build

WORKDIR /src

COPY Directory.Build.props global.json SbeB3UmdfConsumer.slnx ./
COPY schemas/ schemas/
COPY src/ src/

RUN dotnet restore src/B3.Umdf.ConsoleApp/B3.Umdf.ConsoleApp.csproj
RUN dotnet publish src/B3.Umdf.ConsoleApp/B3.Umdf.ConsoleApp.csproj \
    -c Release -o /app --no-restore

FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS runtime

WORKDIR /app
COPY --from=build /app .
COPY --from=tools /tools /usr/local/bin

# DOTNET_DiagnosticPorts default lives at /tmp/dotnet-diagnostic-<pid> — fine as-is.
EXPOSE 8080

STOPSIGNAL SIGTERM

HEALTHCHECK --interval=10s --timeout=3s --start-period=30s --retries=3 \
    CMD ["/app/B3.Umdf.ConsoleApp", "--health-check"]

ENTRYPOINT ["/app/B3.Umdf.ConsoleApp"]
