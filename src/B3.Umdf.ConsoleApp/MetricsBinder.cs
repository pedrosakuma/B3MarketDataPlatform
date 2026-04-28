using System.Diagnostics.Metrics;
using B3.Umdf.Book;
using B3.Umdf.Feed;
using B3.Umdf.Server;
using B3.Umdf.Transport;

/// <summary>
/// Central OTEL-compatible metrics registration using System.Diagnostics.Metrics.
/// All instruments are observable (pull-based) — zero overhead on the hot path.
/// Monitor with: dotnet-counters monitor --counters B3.Umdf.Consumer
/// </summary>
static class MetricsBinder
{
    public static readonly Meter Meter = new("B3.Umdf.Consumer", "1.0.0");

    public static void Register(
        Stats stats,
        IReadOnlyList<BookManager> bookManagers,
        IReadOnlyList<MarketDataManager> marketDataManagers,
        IReadOnlyList<int> groupIds,
        MultiFeedManager? multiFeed,
        FeedHandler? singleFeed,
        MulticastChannelMerger? multicastMerger,
        SubscriptionManager? subscriptionManager,
        IReadOnlyList<GroupConflationHandler>? groupHandlers,
        SymbolRegistry symbolRegistry,
        IReadOnlyList<MulticastPacketSource>? multicastSources = null)
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

        IEnumerable<Measurement<long>> PerGroupMd(Func<MarketDataManager, long> selector)
        {
            for (int i = 0; i < marketDataManagers.Count; i++)
                yield return new Measurement<long>(selector(marketDataManagers[i]),
                    Tag("group", $"G{groupIds[i]}"));
        }

        IEnumerable<Measurement<long>> PerGroupRegistry(Func<SymbolStateRegistry, long> selector)
        {
            for (int i = 0; i < bookManagers.Count; i++)
            {
                var reg = bookManagers[i].StateRegistry;
                if (reg is null) continue;
                yield return new Measurement<long>(selector(reg),
                    Tag("group", $"G{groupIds[i]}"));
            }
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

        Meter.CreateObservableCounter("b3.umdf.feed.reorder_hits",
            () => FeedHandlers().Select(h =>
                new Measurement<long>(h.Handler.IncrementalHandler.ReorderHits, Tag("group", h.Label))),
            unit: "{packets}", description: "Out-of-order packets later drained from the A/B reorder buffer (avoided spurious recovery)");

        Meter.CreateObservableGauge("b3.umdf.feed.reorder_buffer_depth",
            () => FeedHandlers().Select(h =>
                new Measurement<int>(h.Handler.IncrementalHandler.ReorderBufferDepth, Tag("group", h.Label))),
            unit: "{packets}", description: "Current depth of the A/B reorder buffer");

        Meter.CreateObservableCounter("b3.umdf.feed.channel_gaps_absorbed",
            () => FeedHandlers().Select(h =>
                new Measurement<long>(h.Handler.PerSymbolGapsAbsorbed, Tag("group", h.Label))),
            unit: "{gaps}", description: "Channel-level gaps absorbed in Streaming (per-symbol routing healed affected instruments)");

        // ── Feed gauges ──

        Meter.CreateObservableGauge("b3.umdf.feed.state",
            () => FeedHandlers().Select(h =>
                new Measurement<int>((int)h.Handler.State, Tag("group", h.Label))),
            description: "Feed state per group (0=WaitInstrumentDefinition, 1=Streaming)");

        Meter.CreateObservableGauge("b3.umdf.feed.last_packet_age",
            () => FeedHandlers().Select(h =>
            {
                long ticks = h.Handler.LastPacketTicks;
                long ageMs = ticks > 0 ? Environment.TickCount64 - ticks : -1;
                return new Measurement<long>(ageMs, Tag("group", h.Label));
            }),
            unit: "ms", description: "Milliseconds since last packet received per group");

        // The legacy b3.umdf.feed.queue_depth / queue_dropped metrics were removed when the
        // per-group bounded Channel<UmdfPacket> was eliminated. Backpressure is now visible
        // via the per-group MPSC dispatch ring (b3.umdf.feed.ring.depth /
        // b3.umdf.feed.ring.dropped) and the sequence-gap / recovery counters.
        if (multiFeed is not null)
        {
            var feed = multiFeed;

            IEnumerable<Measurement<int>> RingDepth()
            {
                foreach (var s in feed.GetChannelStats())
                    yield return new Measurement<int>(s.Depth, Tag("group", $"G{s.GroupId}"));
            }

            IEnumerable<Measurement<long>> RingDropped()
            {
                foreach (var s in feed.GetChannelStats())
                    yield return new Measurement<long>(s.DroppedPackets, Tag("group", $"G{s.GroupId}"));
            }

            Meter.CreateObservableGauge("b3.umdf.feed.ring.depth",
                RingDepth,
                unit: "{packets}", description: "Pending packets in the per-group MPSC dispatch ring");

            Meter.CreateObservableCounter("b3.umdf.feed.ring.dropped",
                RingDropped,
                unit: "{packets}", description: "Packets dropped on per-group ring overflow (newest dropped)");

            // Per-channel drop attribution: distinguishes Inc A/B (~118 kpps peak)
            // from Snap / InstrumentDefinition (~1 kpps drip). A spike here is a
            // strong signal of which producer outpaced the dispatch thread.
            IEnumerable<Measurement<long>> RingDroppedByChannel()
            {
                foreach (var s in feed.GetChannelDropBreakdown())
                    yield return new Measurement<long>(
                        s.DroppedPackets,
                        Tag("group", $"G{s.GroupId}"),
                        Tag("channel", s.Channel.ToString()));
            }

            Meter.CreateObservableCounter("b3.umdf.feed.ring.dropped_by_channel",
                RingDroppedByChannel,
                unit: "{packets}",
                description: "Per-channel breakdown of per-group ring overflow drops (sums to b3.umdf.feed.ring.dropped per group)");
        }

        if (multicastMerger is not null)
        {
            var merger = multicastMerger;
            Meter.CreateObservableGauge("b3.umdf.transport.merge_queue_depth",
                () => merger.QueueDepth,
                unit: "{packets}", description: "Pending packets in the multicast merge queue");

            Meter.CreateObservableCounter("b3.umdf.transport.merge_dropped",
                () => merger.DroppedPackets,
                unit: "{packets}", description: "Packets dropped from the multicast merge queue on overflow");
        }

        if (multicastSources is { Count: > 0 })
        {
            var sources = multicastSources;

            IEnumerable<Measurement<long>> PerSource(Func<MulticastPacketSource, long> selector)
            {
                foreach (var s in sources)
                    yield return new Measurement<long>(selector(s),
                        Tag("group", $"G{s.ChannelGroup}"),
                        Tag("channel", s.ChannelType.ToString()));
            }

            IEnumerable<Measurement<int>> PerSourceInt(Func<MulticastPacketSource, int> selector)
            {
                foreach (var s in sources)
                    yield return new Measurement<int>(selector(s),
                        Tag("group", $"G{s.ChannelGroup}"),
                        Tag("channel", s.ChannelType.ToString()));
            }

            Meter.CreateObservableCounter("b3.umdf.transport.recvmmsg.syscalls",
                () => PerSource(s => s.BatchedSyscalls),
                unit: "{calls}", description: "recvmmsg(2) syscalls that returned at least one datagram");

            Meter.CreateObservableCounter("b3.umdf.transport.recvmmsg.datagrams",
                () => PerSource(s => s.BatchedDatagrams),
                unit: "{datagrams}", description: "Datagrams received via recvmmsg batched receive (avg batch = datagrams / syscalls)");

            Meter.CreateObservableCounter("b3.umdf.transport.membership.joins",
                () => PerSource(s => s.MembershipJoins),
                unit: "{events}", description: "IGMP joins on multicast sources (initial + recovery rejoins)");

            Meter.CreateObservableCounter("b3.umdf.transport.membership.leaves",
                () => PerSource(s => s.MembershipLeaves),
                unit: "{events}", description: "IGMP leaves on multicast sources (issued when group enters RealTime)");

            Meter.CreateObservableGauge("b3.umdf.transport.membership.joined",
                () => PerSourceInt(s => s.IsJoined ? 1 : 0),
                unit: "{bool}", description: "1 if the multicast source is currently joined to its group, else 0");
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

        Meter.CreateObservableGauge("b3.umdf.book.currently_crossed",
            () => PerGroupBook(bm => bm.CurrentlyCrossedBooks),
            description: "Number of books currently crossed (bid >= ask) while in OPEN trading phase — true anomalies");

        Meter.CreateObservableGauge("b3.umdf.book.currently_crossed_auction",
            () => PerGroupBook(bm => bm.CurrentlyCrossedAuction),
            description: "Number of books currently crossed/locked while in a non-OPEN phase (auction/halt/closed) — expected");

        Meter.CreateObservableGauge("b3.umdf.book.currently_locked",
            () => PerGroupBook(bm => bm.CurrentlyLockedBooks),
            description: "Number of books where bestBid == bestAsk (locked subset of crossed)");

        Meter.CreateObservableCounter("b3.umdf.book.delete_not_found",
            () => PerGroupBook(bm => bm.DeleteNotFound),
            description: "Delete operations on non-existent orders");

        Meter.CreateObservableCounter("b3.umdf.book.instruments_replaced",
            () => PerGroupBook(bm => bm.InstrumentsReplaced),
            unit: "{instruments}", description: "Per-symbol epoch resets triggered by SecurityID identity change (delisting reuse) — book cleared, stale buffer dropped, all baselines reset (P12-3)");

        // ── Per-symbol gap detection (Phase 0 — shadow only, no behavior change) ──
        // Counts gaps detected via per-security rptSeq comparison. Provides
        // production data to size the upcoming per-symbol recovery refactor.
        // Channel-level Recovery is unaffected.
        Meter.CreateObservableCounter("b3.umdf.symbol.gap.detected",
            () =>
            {
                var measurements = new List<Measurement<long>>();
                for (int i = 0; i < bookManagers.Count; i++)
                {
                    var grp = $"G{groupIds[i]}";
                    measurements.Add(new(bookManagers[i].GapTracker.GapCount(SymbolGapKind.Mbo),
                        Tag("group", grp), Tag("kind", "mbo")));
                    var mdmTracker = marketDataManagers[i].GapTracker;
                    foreach (SymbolGapKind k in Enum.GetValues<SymbolGapKind>())
                    {
                        if (k == SymbolGapKind.Mbo) continue;
                        measurements.Add(new(mdmTracker.GapCount(k),
                            Tag("group", grp), Tag("kind", k.ToString())));
                    }
                }
                return measurements;
            },
            unit: "{gaps}", description: "Per-symbol rptSeq gap events (shadow tracking, no behavior change)");

        Meter.CreateObservableCounter("b3.umdf.symbol.gap.size",
            () =>
            {
                var measurements = new List<Measurement<long>>();
                for (int i = 0; i < bookManagers.Count; i++)
                {
                    var grp = $"G{groupIds[i]}";
                    measurements.Add(new(bookManagers[i].GapTracker.GapSizeSum(SymbolGapKind.Mbo),
                        Tag("group", grp), Tag("kind", "mbo")));
                    var mdmTracker = marketDataManagers[i].GapTracker;
                    foreach (SymbolGapKind k in Enum.GetValues<SymbolGapKind>())
                    {
                        if (k == SymbolGapKind.Mbo) continue;
                        measurements.Add(new(mdmTracker.GapSizeSum(k),
                            Tag("group", grp), Tag("kind", k.ToString())));
                    }
                }
                return measurements;
            },
            unit: "{rptseqs}", description: "Sum of skipped rptSeq counts across all per-symbol gaps");

        Meter.CreateObservableGauge("b3.umdf.symbol.gap.affected",
            () =>
            {
                var measurements = new List<Measurement<int>>();
                for (int i = 0; i < bookManagers.Count; i++)
                {
                    var grp = $"G{groupIds[i]}";
                    measurements.Add(new(bookManagers[i].GapTracker.AffectedSymbolCount(SymbolGapKind.Mbo),
                        Tag("group", grp), Tag("kind", "mbo")));
                    var mdmTracker = marketDataManagers[i].GapTracker;
                    foreach (SymbolGapKind k in Enum.GetValues<SymbolGapKind>())
                    {
                        if (k == SymbolGapKind.Mbo) continue;
                        measurements.Add(new(mdmTracker.AffectedSymbolCount(k),
                            Tag("group", grp), Tag("kind", k.ToString())));
                    }
                }
                return measurements;
            },
            unit: "{symbols}", description: "Distinct symbols that experienced at least one rptSeq gap (per kind, per group)");

        Meter.CreateObservableCounter("b3.umdf.book.null_price_skips",
            () => PerGroupBook(bm => bm.NullPriceNewSkips),
            description: "New orders skipped due to null price");

        Meter.CreateObservableCounter("b3.umdf.book.null_price_deletes",
            () => PerGroupBook(bm => bm.NullPriceChangeDeletes),
            description: "Order updates with null price converted to deletes");

        Meter.CreateObservableCounter("b3.umdf.book.market_order_adds",
            () => PerGroupBook(bm => bm.MarketOrderAdds),
            description: "MOA/MOC null-price orders inserted into the market tier");

        Meter.CreateObservableCounter("b3.umdf.book.snapshot_market_order_adds",
            () => PerGroupBook(bm => bm.SnapshotMarketOrderAdds),
            description: "MOA/MOC null-price orders accepted from MBO snapshots");

        Meter.CreateObservableCounter("b3.umdf.book.market_order_updates",
            () => PerGroupBook(bm => bm.MarketOrderUpdates),
            description: "MOA/MOC null-price orders updated in the market tier");

        Meter.CreateObservableCounter("b3.umdf.book.market_order_deletes",
            () => PerGroupBook(bm => bm.MarketOrderDeletes),
            description: "MOA/MOC null-price orders removed from the market tier");

        Meter.CreateObservableCounter("b3.umdf.book.market_order_transitions_to_priced",
            () => PerGroupBook(bm => bm.MarketOrderTransitionsToPriced),
            description: "MOA/MOC orders that later received a concrete price");

        Meter.CreateObservableCounter("b3.umdf.book.market_order_transitions_to_market",
            () => PerGroupBook(bm => bm.MarketOrderTransitionsToMarket),
            description: "Priced orders that later moved into the MOA/MOC market tier");

        // ── Per-symbol recovery ──
        // These instruments report the per-symbol heal pipeline (Stale-symbol
        // counts, snapshot heal counters). They are always populated since
        // every BookManager / MarketDataManager owns a SymbolStateRegistry.

        Meter.CreateObservableGauge("b3.umdf.persymbol.stale_symbols",
            () =>
            {
                var m = new List<Measurement<int>>();
                for (int i = 0; i < bookManagers.Count; i++)
                {
                    var reg = bookManagers[i].StateRegistry;
                    if (reg is null) continue;
                    var grp = $"G{groupIds[i]}";
                    var snap = reg.GetAggregateSnapshot();
                    m.Add(new(snap.TotalStaleSymbols, Tag("group", grp), Tag("kind", "any")));
                    foreach (SymbolGapKind k in Enum.GetValues<SymbolGapKind>())
                        m.Add(new(snap.StaleOf(k), Tag("group", grp), Tag("kind", k.ToString())));
                }
                return m;
            },
            unit: "{symbols}", description: "Symbols currently in Stale state awaiting snapshot heal");

        Meter.CreateObservableGauge("b3.umdf.persymbol.tracked_symbols",
            () =>
            {
                var m = new List<Measurement<int>>();
                for (int i = 0; i < bookManagers.Count; i++)
                {
                    var reg = bookManagers[i].StateRegistry;
                    if (reg is null) continue;
                    m.Add(new(reg.GetAggregateSnapshot().TotalSymbols,
                        Tag("group", $"G{groupIds[i]}")));
                }
                return m;
            },
            unit: "{symbols}", description: "Symbols tracked by SymbolStateRegistry");

        Meter.CreateObservableCounter("b3.umdf.persymbol.lagging_snapshots",
            () =>
            {
                var m = new List<Measurement<long>>();
                for (int i = 0; i < bookManagers.Count; i++)
                {
                    var reg = bookManagers[i].StateRegistry;
                    if (reg is null) continue;
                    m.Add(new(reg.LaggingSnapshotCount, Tag("group", $"G{groupIds[i]}")));
                }
                return m;
            },
            unit: "{snapshots}", description: "Snapshot heals where snapshotRptSeq lagged the live high-water (ignored)");

        Meter.CreateObservableCounter("b3.umdf.persymbol.snapshots_healed",
            () => PerGroupBook(bm => bm.SnapshotsHealed),
            unit: "{snapshots}", description: "Per-symbol snapshots that successfully transitioned a symbol to Healthy");

        Meter.CreateObservableCounter("b3.umdf.persymbol.snapshots_missing_rptseq",
            () => PerGroupBook(bm => bm.SnapshotsMissingRptSeq),
            unit: "{snapshots}", description: "Snapshot Orders_71 received without a matching Header_30 LastRptSeq baseline");

        Meter.CreateObservableCounter("b3.umdf.persymbol.snapshot_chunks_orphaned",
            () => PerGroupBook(bm => bm.SnapshotChunksOrphaned),
            unit: "{chunks}", description: "Snapshot Orders_71 chunks dropped because no Header_30 was seen first for that securityID");

        Meter.CreateObservableCounter("b3.umdf.persymbol.snapshots_rejected_too_old",
            () => PerGroupBook(bm => bm.SnapshotsRejectedTooOld),
            unit: "{snapshots}", description: "Snapshots rejected because LastRptSeq is older than the symbol's MinHealRptSeq (would leave a hole)");

        Meter.CreateObservableCounter("b3.umdf.persymbol.snapshots_rejected_stale_version",
            () => PerGroupBook(bm => bm.SnapshotsRejectedStaleVersion),
            unit: "{snapshots}", description: "Snapshots rejected because they belong to an older SequenceVersion than the active epoch (post-rollover stragglers)");

        Meter.CreateObservableCounter("b3.umdf.persymbol.snapshots_abandoned",
            () => PerGroupBook(bm => bm.SnapshotsAbandoned),
            unit: "{snapshots}", description: "Snapshots abandoned mid-flight (e.g. truncated before CompleteSnapshot, replaced by a newer snapshot for the same symbol)");

        Meter.CreateObservableCounter("b3.umdf.persymbol.snapshots_aborted_by_epoch",
            () => PerGroupBook(bm => bm.SnapshotsAbortedByEpoch),
            unit: "{snapshots}", description: "Snapshots aborted because a per-symbol epoch reset (SequenceVersion bump or instrument replacement) invalidated the in-progress baseline");

        Meter.CreateObservableCounter("b3.umdf.persymbol.snapshots_authoritative_reset",
            () =>
            {
                var m = new List<Measurement<long>>();
                for (int i = 0; i < bookManagers.Count; i++)
                {
                    var reg = bookManagers[i].StateRegistry;
                    if (reg is null) continue;
                    m.Add(new(reg.StaleAuthoritativeResetCount, Tag("group", $"G{groupIds[i]}")));
                }
                return m;
            },
            unit: "{snapshots}", description: "Snapshots accepted as authoritative reset (stuck-Stale escape after StaleEscapeTimeoutMs); buffered tail discarded");

        // Severity instrumentation for authoritative reset.
        // unsafeDelta        = MinHeal - snap   (proven-unbridgeable gap, lower bound)
        // discardedTailDelta = highWater - snap (live tail abandoned by empty-drain)
        // Exposed as last/max/sum so dashboards can alert on magnitude.
        Meter.CreateObservableGauge("b3.umdf.persymbol.authoritative_reset_unsafe_delta_last",
            () => PerGroupRegistry(r => (long)r.LastAuthoritativeResetUnsafeDelta),
            unit: "{rptseq}", description: "Last authoritative-reset unsafe delta (MinHeal - snapRptSeq) per group");
        Meter.CreateObservableGauge("b3.umdf.persymbol.authoritative_reset_unsafe_delta_max",
            () => PerGroupRegistry(r => (long)r.MaxAuthoritativeResetUnsafeDelta),
            unit: "{rptseq}", description: "Max authoritative-reset unsafe delta observed since process start");
        Meter.CreateObservableCounter("b3.umdf.persymbol.authoritative_reset_unsafe_delta_sum",
            () => PerGroupRegistry(r => (long)r.SumAuthoritativeResetUnsafeDelta),
            unit: "{rptseq}", description: "Cumulative sum of authoritative-reset unsafe deltas (proven lost message count lower bound)");
        Meter.CreateObservableGauge("b3.umdf.persymbol.authoritative_reset_discarded_tail_delta_last",
            () => PerGroupRegistry(r => (long)r.LastAuthoritativeResetDiscardedTailDelta),
            unit: "{rptseq}", description: "Last authoritative-reset discarded-tail delta (priorHighWater - snapRptSeq)");
        Meter.CreateObservableGauge("b3.umdf.persymbol.authoritative_reset_discarded_tail_delta_max",
            () => PerGroupRegistry(r => (long)r.MaxAuthoritativeResetDiscardedTailDelta),
            unit: "{rptseq}", description: "Max authoritative-reset discarded-tail delta observed since process start");
        Meter.CreateObservableCounter("b3.umdf.persymbol.authoritative_reset_discarded_tail_delta_sum",
            () => PerGroupRegistry(r => (long)r.SumAuthoritativeResetDiscardedTailDelta),
            unit: "{rptseq}", description: "Cumulative sum of authoritative-reset discarded-tail deltas (live tail messages abandoned)");

        Meter.CreateObservableCounter("b3.umdf.persymbol.snapshots_skipped_healthy_ahead",
            () => PerGroupBook(bm => bm.SnapshotsSkippedHealthyAhead),
            unit: "{snapshots}", description: "Snapshots ignored because symbol is already Healthy with a more recent book.LastRptSeq than the snapshot baseline (always-on snapshot stream noise)");

        Meter.CreateObservableCounter("b3.umdf.persymbol.mbo_buffered",
            () => PerGroupBook(bm => bm.BufferedMboMessages),
            unit: "{messages}", description: "MBO messages enqueued into per-symbol stale buffer awaiting heal");

        Meter.CreateObservableCounter("b3.umdf.persymbol.mbo_replayed",
            () => PerGroupBook(bm => bm.ReplayedMboMessages),
            unit: "{messages}", description: "MBO messages replayed from stale buffer after snapshot heal");

        Meter.CreateObservableCounter("b3.umdf.persymbol.mbo_stale_transitions",
            () => PerGroupBook(bm => bm.MboStaleTransitions),
            unit: "{transitions}", description: "Healthy→Stale transitions observed by RouteMbo");

        Meter.CreateObservableCounter("b3.umdf.persymbol.mbo_stale_gap_size_sum",
            () => PerGroupBook(bm => bm.MboStaleGapSizeSum),
            unit: "{rptseq}", description: "Cumulative gap size of Healthy→Stale transitions (rptSeq distance)");

        Meter.CreateObservableGauge("b3.umdf.persymbol.mbo_stale_gap_size_max",
            () => PerGroupBook(bm => bm.MboStaleGapSizeMax),
            unit: "{rptseq}", description: "Largest gap size on any Healthy→Stale transition");

        Meter.CreateObservableCounter("b3.umdf.persymbol.epoch_resets",
            () => PerGroupBook(bm => bm.EpochResets),
            unit: "{events}", description: "ChannelReset/SequenceReset events that bumped the per-symbol epoch");

        Meter.CreateObservableCounter("b3.umdf.persymbol.epoch_reset_messages_dropped",
            () => PerGroupBook(bm => bm.EpochResetMessagesDropped),
            unit: "{messages}", description: "Stale-buffer messages dropped during per-symbol epoch reset");

        Meter.CreateObservableCounter("b3.umdf.persymbol.stats_dropped_duplicate",
            () =>
            {
                var m = new List<Measurement<long>>();
                for (int i = 0; i < marketDataManagers.Count; i++)
                    m.Add(new(marketDataManagers[i].DroppedDuplicateStats,
                        Tag("group", $"G{groupIds[i]}")));
                return m;
            },
            unit: "{messages}", description: "Per-symbol stat messages dropped as duplicates (rptSeq <= last seen)");

        Meter.CreateObservableCounter("b3.umdf.persymbol.stats_live_resyncs",
            () =>
            {
                var m = new List<Measurement<long>>();
                for (int i = 0; i < marketDataManagers.Count; i++)
                    m.Add(new(marketDataManagers[i].LiveResyncs,
                        Tag("group", $"G{groupIds[i]}")));
                return m;
            },
            unit: "{events}", description: "Per-symbol stat live resyncs (gap on Healthy applied via NextMessage policy)");

        Meter.CreateObservableGauge("b3.umdf.persymbol.stale_buffer_bytes",
            () =>
            {
                var m = new List<Measurement<long>>();
                for (int i = 0; i < bookManagers.Count; i++)
                {
                    var buf = bookManagers[i].StaleBuffer;
                    if (buf is null) continue;
                    m.Add(new(buf.TotalBytes, Tag("group", $"G{groupIds[i]}")));
                }
                return m;
            },
            unit: "By", description: "Bytes currently held by per-symbol stale MBO buffers (ArrayPool-backed)");

        Meter.CreateObservableCounter("b3.umdf.persymbol.stale_buffer_dropped_persymbol_cap",
            () =>
            {
                var m = new List<Measurement<long>>();
                for (int i = 0; i < bookManagers.Count; i++)
                {
                    var buf = bookManagers[i].StaleBuffer;
                    if (buf is null) continue;
                    m.Add(new(buf.DroppedPerSymbolCapCount, Tag("group", $"G{groupIds[i]}")));
                }
                return m;
            },
            unit: "{messages}", description: "Stale-buffer enqueues rejected because the per-symbol message cap was reached");

        Meter.CreateObservableCounter("b3.umdf.persymbol.stale_buffer_dropped_global_cap",
            () =>
            {
                var m = new List<Measurement<long>>();
                for (int i = 0; i < bookManagers.Count; i++)
                {
                    var buf = bookManagers[i].StaleBuffer;
                    if (buf is null) continue;
                    m.Add(new(buf.DroppedGlobalCapCount, Tag("group", $"G{groupIds[i]}")));
                }
                return m;
            },
            unit: "{messages}", description: "Stale-buffer enqueues rejected because the global byte cap was reached");

        Meter.CreateObservableCounter("b3.umdf.persymbol.stale_buffer_evicted_unsafe",
            () =>
            {
                var m = new List<Measurement<long>>();
                for (int i = 0; i < bookManagers.Count; i++)
                {
                    var buf = bookManagers[i].StaleBuffer;
                    if (buf is null) continue;
                    m.Add(new(buf.EvictedPerSymbolCapCount, Tag("group", $"G{groupIds[i]}")));
                }
                return m;
            },
            unit: "{messages}", description: "Hot-cap evictions that bumped MinHeal (no snapshot in flight, or evicted msg ≥ snapshot floor — each one makes the next heal harder)");

        Meter.CreateObservableCounter("b3.umdf.persymbol.stale_buffer_evicted_safe_below_floor",
            () =>
            {
                var m = new List<Measurement<long>>();
                for (int i = 0; i < bookManagers.Count; i++)
                {
                    var buf = bookManagers[i].StaleBuffer;
                    if (buf is null) continue;
                    m.Add(new(buf.SafeEvictedBelowFloorCount, Tag("group", $"G{groupIds[i]}")));
                }
                return m;
            },
            unit: "{messages}", description: "Hot-cap evictions absorbed safely below the protected snapshot floor (snapshot already covers — heal still viable)");

        Meter.CreateObservableCounter("b3.umdf.persymbol.stale_buffer_hot_promotions",
            () =>
            {
                var m = new List<Measurement<long>>();
                for (int i = 0; i < bookManagers.Count; i++)
                {
                    var buf = bookManagers[i].StaleBuffer;
                    if (buf is null) continue;
                    m.Add(new(buf.HotPromotionCount, Tag("group", $"G{groupIds[i]}")));
                }
                return m;
            },
            unit: "{symbols}", description: "Symbols promoted from normal cap to hot cap on first overflow (one-way ratchet)");

        Meter.CreateObservableCounter("b3.umdf.persymbol.stale_buffer_upper_tier_promotions",
            () =>
            {
                var m = new List<Measurement<long>>();
                for (int i = 0; i < bookManagers.Count; i++)
                {
                    var buf = bookManagers[i].StaleBuffer;
                    if (buf is null) continue;
                    var byLevel = buf.GetPromotionsByLevel();
                    for (int li = 2; li < byLevel.Length; li++)
                        m.Add(new(byLevel[li], Tag("group", $"G{groupIds[i]}"), Tag("level", li.ToString())));
                }
                return m;
            },
            unit: "{symbols}", description: "Symbols promoted to upper-tier caps (level 2+) — typically ultra-hot symbols during long heal windows");

        Meter.CreateObservableCounter("b3.umdf.persymbol.stale_buffer_promotions_refused",
            () =>
            {
                var m = new List<Measurement<long>>();
                for (int i = 0; i < bookManagers.Count; i++)
                {
                    var buf = bookManagers[i].StaleBuffer;
                    if (buf is null) continue;
                    m.Add(new(buf.PromotionsRefusedGlobalCapCount, Tag("group", $"G{groupIds[i]}")));
                }
                return m;
            },
            unit: "{events}", description: "Upper-tier promotion requests denied because global byte budget was above threshold (~70%)");

        Meter.CreateObservableCounter("b3.umdf.persymbol.channel_gaps_absorbed",
            () => FeedHandlers().Select(h =>
                new Measurement<long>(h.Handler.PerSymbolGapsAbsorbed, Tag("group", h.Label))),
            unit: "{gaps}", description: "Channel-level gaps absorbed without entering Recovery (per-symbol healing)");

        Meter.CreateObservableGauge("b3.umdf.persymbol.fanout_suppressed",
            () =>
            {
                var m = new List<Measurement<int>>(groupHandlers?.Count ?? 0);
                if (groupHandlers is null) return m;
                for (int i = 0; i < groupHandlers.Count; i++)
                    m.Add(new(groupHandlers[i].IsFanoutSuppressed ? 1 : 0,
                        Tag("group", $"G{groupIds[i]}")));
                return m;
            },
            description: "1 when broadcaster fanout is suppressed for the group (channel-state OR per-symbol stale-ratio gate)");

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

        Meter.CreateObservableCounter("b3.umdf.instruments.security_definitions_skipped",
            () => PerGroupMd(m => m.SecurityDefinitionsSkipped),
            unit: "{messages}", description: "SecurityDefinition messages skipped because their content was unchanged from the cached version (P11-2 skip-if-unchanged)");

        Meter.CreateObservableCounter("b3.umdf.instruments.security_definitions_timestamp_regressed",
            () => PerGroupMd(m => m.SecurityDefinitionsTimestampRegressed),
            unit: "{messages}", description: "SecurityDefinition messages dropped because their SecurityValidityTimestamp is older than the cached value — out-of-order arrivals that would silently roll metadata back (P12-3)");

        Meter.CreateObservableCounter("b3.umdf.instruments.identity_changed",
            () => PerGroupMd(m => m.InstrumentIdentityChanged),
            unit: "{instruments}", description: "SecurityDefinition arrivals where the canonical identity (Symbol/ISIN/MaturityDate/SecurityType) differs from the cached value — i.e. the exchange recycled the SecurityID for a different instrument (P12-3)");

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

            Meter.CreateObservableGauge("b3.umdf.server.client_pending_bytes",
                () => sm.GetClientStats().Select(c =>
                    new Measurement<long>(c.PendingBytes, Tag("client", c.Id))),
                unit: "By", description: "Bytes currently sitting in the client outbound ring");

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

            // ── Broadcaster decoupling path (per-group) ──

            Meter.CreateObservableCounter("b3.umdf.server.broadcast.batches_published",
                () =>
                {
                    var m = new List<Measurement<long>>();
                    for (int i = 0; i < groupHandlers.Count; i++)
                        m.Add(new Measurement<long>(groupHandlers[i].BroadcastBatchesPublished,
                            Tag("group", $"G{groupIds[i]}")));
                    return m;
                },
                unit: "{batches}", description: "Per-packet broadcast batches published from dispatch to broadcaster thread");

            Meter.CreateObservableCounter("b3.umdf.server.broadcast.batches_dropped",
                () =>
                {
                    var m = new List<Measurement<long>>();
                    for (int i = 0; i < groupHandlers.Count; i++)
                        m.Add(new Measurement<long>(groupHandlers[i].BroadcastBatchesDroppedFull,
                            Tag("group", $"G{groupIds[i]}")));
                    return m;
                },
                unit: "{batches}", description: "Broadcast batches dropped because the broadcaster ring was full");

            Meter.CreateObservableCounter("b3.umdf.server.broadcast.resync_requests",
                () =>
                {
                    var m = new List<Measurement<long>>();
                    for (int i = 0; i < groupHandlers.Count; i++)
                        m.Add(new Measurement<long>(groupHandlers[i].BroadcastResyncRequests,
                            Tag("group", $"G{groupIds[i]}")));
                    return m;
                },
                unit: "{requests}", description: "Resnapshot requests enqueued after broadcast drops");

            Meter.CreateObservableGauge("b3.umdf.server.broadcast.ring_depth",
                () =>
                {
                    var m = new List<Measurement<int>>();
                    for (int i = 0; i < groupHandlers.Count; i++)
                        m.Add(new Measurement<int>(groupHandlers[i].BroadcastRingDepth,
                            Tag("group", $"G{groupIds[i]}")));
                    return m;
                },
                unit: "{batches}", description: "Current broadcaster ring depth (per group)");
        }
    }
}
