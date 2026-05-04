using B3.Umdf.Book;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;

namespace B3.Umdf.Server.Hosting;

internal sealed class RecoveryEndpointMapper
{
    private readonly Func<Func<int, IReadOnlyList<RecoveryEvent>>?> _eventProvider;
    private readonly Func<Func<long>?> _eventTotalProvider;

    public RecoveryEndpointMapper(
        Func<Func<int, IReadOnlyList<RecoveryEvent>>?> eventProvider,
        Func<Func<long>?> eventTotalProvider)
    {
        _eventProvider = eventProvider;
        _eventTotalProvider = eventTotalProvider;
    }

    public void Map(WebApplication app)
    {
        // /api/recovery/recent surfaces the in-memory ring buffer of recent
        // recovery audit events for ops triage. When no provider is wired
        // (tests, embedded scenarios) the endpoint returns an empty list
        // rather than 404 so the contract stays stable.
        app.MapGet("/api/recovery/recent", (int? limit) =>
        {
            var capped = Math.Clamp(limit ?? 50, 1, 1000);
            var events = _eventProvider()?.Invoke(capped) ?? Array.Empty<RecoveryEvent>();
            var dto = new RecoveryEventLogResponse
            {
                TotalRecorded = _eventTotalProvider()?.Invoke() ?? 0,
                Returned = events.Count,
                Events = new RecoveryEventDto[events.Count],
            };
            for (int i = 0; i < events.Count; i++)
            {
                var e = events[i];
                dto.Events[i] = new RecoveryEventDto
                {
                    TimestampUnixMs = e.TimestampUnixMs,
                    Kind = (int)e.Kind,
                    KindName = e.Kind.ToString(),
                    GroupId = e.GroupId,
                    SecurityId = e.SecurityId,
                    SnapshotRptSeq = e.SnapshotRptSeq,
                    PriorRptSeq = e.PriorRptSeq,
                    Detail = e.Detail,
                };
            }
            return Results.Json(dto, AppJsonContext.Default.RecoveryEventLogResponse);
        });
    }
}
