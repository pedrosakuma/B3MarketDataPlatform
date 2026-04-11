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
COPY docker-entrypoint.sh /app/
RUN chmod +x /app/docker-entrypoint.sh

ENTRYPOINT ["/app/docker-entrypoint.sh"]
