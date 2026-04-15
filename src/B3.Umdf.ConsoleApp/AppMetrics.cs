using System.Diagnostics.Metrics;
using B3.Umdf.Book;
using B3.Umdf.Feed;
using B3.Umdf.Server;

/// <summary>
/// Central OTEL-compatible metrics registration using System.Diagnostics.Metrics.
/// All instruments are observable (pull-based) — zero overhead on the hot path.
/// Monitor with: dotnet-counters monitor --counters B3.Umdf.Consumer
/// </summary>
static class AppMetrics
{
    public static readonly Meter Meter = new("B3.Umdf.Consumer", "1.0.0");

    public static void Register(
        Stats stats,
        IReadOnlyList<BookManager> bookManagers,
        IReadOnlyList<MarketDataManager> marketDataManagers,
        IReadOnlyList<int> groupIds,
        MultiFeedManager? multiFeed,
        FeedHandler? singleFeed,
        SubscriptionManager? subscriptionManager,
        IReadOnlyList<GroupConflationHandler>? groupHandlers,
        SymbolRegistry symbolRegistry)
    {
        // ── Helpers ──

        static KeyValuePair<string, object?> Tag(string key, object? value) => new(key, value);

        IEnumerable<(string Label, FeedHandler Handler)> FeedHandlers()
        {
            if (multiFeed is not null)
            {
                foreach (var (gid, h) in multiFeed.Handlers)
                    yield return ($"G{gid}", h);
            }
            else if (singleFeed is not null)
            {
                yield return ($"G{groupIds[0]}", singleFeed);
            }
        }

        IEnumerable<Measurement<long>> PerGroupBook(Func<BookManager, long> selector)
        {
            for (int i = 0; i < bookManagers.Count; i++)
                yield return new Measurement<long>(selector(bookManagers[i]),
                    Tag("group", $"G{groupIds[i]}"));
        }

        IEnumerable<Measurement<int>> PerGroupBookInt(Func<BookManager, int> selector)
        {
            for (int i = 0; i < bookManagers.Count; i++)
                yield return new Measurement<int>(selector(bookManagers[i]),
                    Tag("group", $"G{groupIds[i]}"));
        }

        IEnumerable<Measurement<int>> PerGroupMdInt(Func<MarketDataManager, int> selector)
        {
            for (int i = 0; i < marketDataManagers.Count; i++)
                yield return new Measurement<int>(selector(marketDataManagers[i]),
                    Tag("group", $"G{groupIds[i]}"));
        }

        // ── Feed counters ──

        Meter.CreateObservableCounter("b3.umdf.feed.packets",
            () => FeedHandlers().Select(h =>
                new Measurement<long>(h.Handler.PacketCount, Tag("group", h.Label))),
            unit: "{packets}", description: "Packets received from feed");

        Meter.CreateObservableCounter("b3.umdf.feed.duplicates",
            () => FeedHandlers().Select(h =>
                new Measurement<long>(h.Handler.IncrementalHandler.DuplicatesSkipped, Tag("group", h.Label))),
            unit: "{packets}", description: "Duplicate packets skipped");

        Meter.CreateObservableCounter("b3.umdf.feed.gaps",
            () => FeedHandlers().Select(h =>
                new Measurement<long>(h.Handler.IncrementalHandler.GapsDetected, Tag("group", h.Label))),
            description: "Sequence gaps detected");

        Meter.CreateObservableCounter("b3.umdf.feed.instrument_definitions",
            () => FeedHandlers().Select(h =>
                new Measurement<long>(h.Handler.InstrDefReceived, Tag("group", h.Label))),
            description: "Instrument definitions received");

        // ── Feed gauges ──

        Meter.CreateObservableGauge("b3.umdf.feed.state",
            () => FeedHandlers().Select(h =>
                new Measurement<int>((int)h.Handler.State, Tag("group", h.Label))),
            description: "Feed state per group (0=WaitInstrDef, 1=WaitSnapshot, 2=CatchUp, 3=RealTime, 4=Recovery)");

        Meter.CreateObservableGauge("b3.umdf.feed.last_packet_age",
            () => FeedHandlers().Select(h =>
            {
                long ticks = h.Handler.LastPacketTicks;
                long ageMs = ticks > 0 ? (DateTime.UtcNow.Ticks - ticks) / TimeSpan.TicksPerMillisecond : -1;
                return new Measurement<long>(ageMs, Tag("group", h.Label));
            }),
            unit: "ms", description: "Milliseconds since last packet received per group");

        if (multiFeed is not null)
        {
            var mf = multiFeed;
            Meter.CreateObservableGauge("b3.umdf.feed.queue_depth",
                () => mf.GetChannelDepths().Select(d =>
                    new Measurement<int>(d.Depth, Tag("group", $"G{d.GroupId}"))),
                unit: "{packets}", description: "Pending packets in feed queue per group");
        }

        // ── Book counters ──

        Meter.CreateObservableCounter("b3.umdf.book.orders_added",
            () => PerGroupBook(bm => bm.OrderAdds),
            unit: "{orders}", description: "Orders added to books");

        Meter.CreateObservableCounter("b3.umdf.book.orders_updated",
            () => PerGroupBook(bm => bm.OrderUpdates),
            unit: "{orders}", description: "Order updates applied");

        Meter.CreateObservableCounter("b3.umdf.book.orders_deleted",
            () => PerGroupBook(bm => bm.OrderDeletes),
            unit: "{orders}", description: "Orders deleted from books");

        Meter.CreateObservableCounter("b3.umdf.book.trades",
            () => stats.TradeCount,
            unit: "{trades}", description: "Trades processed");

        Meter.CreateObservableCounter("b3.umdf.book.parse_errors",
            () => PerGroupBook(bm => bm.ParseErrors),
            description: "Book SBE parse errors");

        Meter.CreateObservableCounter("b3.umdf.book.crossings",
            () => PerGroupBook(bm => bm.CrossingTransitions),
            description: "Bid/ask crossing transitions");

        Meter.CreateObservableCounter("b3.umdf.book.delete_not_found",
            () => PerGroupBook(bm => bm.DeleteNotFound),
            description: "Delete operations on non-existent orders");

        Meter.CreateObservableCounter("b3.umdf.book.null_price_skips",
            () => PerGroupBook(bm => bm.NullPriceNewSkips),
            description: "New orders skipped due to null price");

        Meter.CreateObservableCounter("b3.umdf.book.null_price_deletes",
            () => PerGroupBook(bm => bm.NullPriceChangeDeletes),
            description: "Order updates with null price converted to deletes");

        // ── Book gauges ──

        Meter.CreateObservableGauge("b3.umdf.book.active",
            () => PerGroupBookInt(bm => bm.Books.Count),
            description: "Active order books per group");

        // ── Market data counters ──

        Meter.CreateObservableCounter("b3.umdf.market_data.updates",
            () => stats.MarketDataCount,
            unit: "{updates}", description: "Market data updates processed");

        Meter.CreateObservableCounter("b3.umdf.market_data.status_changes",
            () => stats.StatusChangeCount,
            description: "Security status changes");

        Meter.CreateObservableCounter("b3.umdf.market_data.forward_trades",
            () => stats.ForwardTradeCount,
            unit: "{trades}", description: "Forward trades processed");

        Meter.CreateObservableCounter("b3.umdf.market_data.trade_busts",
            () => stats.TradeBustCount,
            description: "Trade busts processed");

        Meter.CreateObservableCounter("b3.umdf.market_data.execution_summaries",
            () => stats.ExecSummaryCount,
            description: "Execution summaries processed");

        Meter.CreateObservableCounter("b3.umdf.market_data.parse_errors",
            () =>
            {
                var measurements = new List<Measurement<long>>();
                for (int i = 0; i < marketDataManagers.Count; i++)
                    measurements.Add(new Measurement<long>(marketDataManagers[i].ParseErrors,
                        Tag("group", $"G{groupIds[i]}")));
                return measurements;
            },
            description: "Market data SBE parse errors");

        // ── Market data gauges ──

        Meter.CreateObservableGauge("b3.umdf.instruments.active",
            () => PerGroupMdInt(m => m.InstrumentData.Count),
            description: "Active instruments per group");

        Meter.CreateObservableGauge("b3.umdf.symbols.registered",
            () => symbolRegistry.Count,
            description: "Total registered symbols");

        // ── Server / subscription metrics ──

        if (subscriptionManager is not null)
        {
            var sm = subscriptionManager;

            Meter.CreateObservableGauge("b3.umdf.server.clients",
                () => sm.ClientCount,
                description: "Connected WebSocket clients");

            Meter.CreateObservableGauge("b3.umdf.server.upstream_conflated",
                () => sm.UpstreamConflated,
                description: "Pending events in upstream conflation buffers");

            Meter.CreateObservableGauge("b3.umdf.server.client_queue_depth",
                () => sm.GetClientStats().Select(c =>
                    new Measurement<int>(c.QueueDepth, Tag("client", c.Id))),
                unit: "{messages}", description: "Client outbound queue depth");

            Meter.CreateObservableCounter("b3.umdf.server.messages_sent",
                () => sm.GetClientStats().Select(c =>
                    new Measurement<long>(c.MessagesSent, Tag("client", c.Id))),
                unit: "{messages}", description: "Messages sent to WebSocket clients");

            Meter.CreateObservableCounter("b3.umdf.server.bytes_sent",
                () => sm.GetClientStats().Select(c =>
                    new Measurement<long>(c.BytesSent, Tag("client", c.Id))),
                unit: "By", description: "Bytes sent to WebSocket clients");
        }

        if (groupHandlers is { Count: > 0 })
        {
            Meter.CreateObservableCounter("b3.umdf.server.events_received",
                () =>
                {
                    var measurements = new List<Measurement<long>>();
                    for (int i = 0; i < groupHandlers.Count; i++)
                        measurements.Add(new Measurement<long>(groupHandlers[i].EventsReceived,
                            Tag("group", $"G{groupIds[i]}")));
                    return measurements;
                },
                description: "Events received into conflation buffers");

            Meter.CreateObservableCounter("b3.umdf.server.events_flushed",
                () =>
                {
                    var measurements = new List<Measurement<long>>();
                    for (int i = 0; i < groupHandlers.Count; i++)
                        measurements.Add(new Measurement<long>(groupHandlers[i].EventsFlushed,
                            Tag("group", $"G{groupIds[i]}")));
                    return measurements;
                },
                description: "Events flushed from conflation to clients");
        }
    }
}
