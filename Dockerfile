FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

COPY Directory.Build.props global.json SbeB3UmdfConsumer.slnx ./
COPY schemas/ schemas/
COPY src/ src/

RUN dotnet restore src/B3.Umdf.ConsoleApp/B3.Umdf.ConsoleApp.csproj
RUN dotnet publish src/B3.Umdf.ConsoleApp/B3.Umdf.ConsoleApp.csproj \
    -c Release -o /app --no-restore

FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS runtime

# Non-root user for security
RUN groupadd -r umdf && useradd -r -g umdf -s /sbin/nologin umdf

WORKDIR /app
COPY --from=build /app .
COPY docker-entrypoint.sh /app/
RUN chmod +x /app/docker-entrypoint.sh && chown -R umdf:umdf /app

USER umdf

EXPOSE 8080

STOPSIGNAL SIGTERM

HEALTHCHECK --interval=10s --timeout=3s --start-period=30s --retries=3 \
    CMD curl -sf http://localhost:8080/live || exit 1

ENTRYPOINT ["/app/docker-entrypoint.sh"]
