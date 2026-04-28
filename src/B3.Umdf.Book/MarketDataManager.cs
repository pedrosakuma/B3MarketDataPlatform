using System.Collections.Concurrent;
using System.Collections.Frozen;
using System.Text;
using B3.Umdf.Feed;
using B3.Umdf.Mbo.Sbe.V16;
using B3.Umdf.Transport;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace B3.Umdf.Book;

public sealed class MarketDataManager : IFeedEventHandler
{
    private readonly ConcurrentDictionary<ulong, InstrumentInfo> _data = new(Environment.ProcessorCount, 4096);
    private volatile FrozenDictionary<ulong, InstrumentInfo>? _frozenData;
    private readonly ConcurrentDictionary<string, int> _groupStatus = new(StringComparer.Ordinal);
    /// <summary>
    /// Index of instruments by SecurityGroup for O(group-size) dispatch in
    /// <see cref="HandleSecurityGroupPhase"/>. Mutated only on the feed thread
    /// (single-writer, owned by this manager) when SecurityDefinition assigns
    /// or re-assigns a group; the inner lists are also feed-thread-only.
    /// </summary>
    private readonly Dictionary<string, List<KeyValuePair<ulong, InstrumentInfo>>> _groupMembers = new(StringComparer.Ordinal);
    private readonly IMarketDataEventHandler? _eventHandler;
    private readonly ILogger<MarketDataManager> _logger;
    private long _parseErrors;
    private long _tradingSessionResets;

    /// <summary>
    /// Thread-safe dictionary of all instrument data.
    /// Safe to read (Count, iterate) from any thread while the feed thread writes.
    /// </summary>
    public IReadOnlyDictionary<ulong, InstrumentInfo> InstrumentData => _data;

    /// <summary>Number of SBE parse errors encountered.</summary>
    public long ParseErrors => Volatile.Read(ref _parseErrors);

    /// <summary>
    /// Number of <c>SecurityDefinition_12</c> messages skipped because their
    /// <c>SecurityValidityTimestamp</c> matched the cached value on the
    /// instrument (P11-2 early-out). Under steady state this should track
    /// 99 %+ of all SecDef receptions, since the exchange re-broadcasts the
    /// same definition every few seconds.
    /// </summary>
    public long SecurityDefinitionsSkipped => Volatile.Read(ref _secDefSkippedCount);

    private long _secDefSkippedCount;

    /// <summary>
    /// Count of TRADING_SESSION_CHANGE (SecurityTradingEvent=4) resets applied
    /// to instruments — see B3 spec §14.3 (end-of-day stats reset).
    /// </summary>
    public long TradingSessionResets => Volatile.Read(ref _tradingSessionResets);

    public MarketDataManager(IMarketDataEventHandler? eventHandler = null, ILogger<MarketDataManager>? logger = null,
        SymbolStateRegistry? stateRegistry = null)
    {
        _eventHandler = eventHandler;
        _logger = logger ?? NullLogger<MarketDataManager>.Instance;
        GapTracker = new SymbolGapTracker(_logger);
        _stateRegistry = stateRegistry ?? throw new ArgumentNullException(nameof(stateRegistry),
            "SymbolStateRegistry is required.");
    }

    private readonly SymbolStateRegistry _stateRegistry;
    private long _droppedDuplicateStats;
    private long _liveResyncs;

    /// <summary>Per-symbol heal-state registry (one per group).</summary>
    public SymbolStateRegistry StateRegistry => _stateRegistry;

    /// <summary>Stat messages dropped because the registry detected a duplicate (lower-or-equal rptSeq).</summary>
    public long DroppedDuplicateStats => Volatile.Read(ref _droppedDuplicateStats);

    /// <summary>Stat messages applied across a registry-detected gap (LiveResyncPolicy.NextMessage).</summary>
    public long LiveResyncs => Volatile.Read(ref _liveResyncs);

    /// <summary>
    /// Per-stat-kind routing decision. Consults the registry: returns false
    /// if the message is a duplicate (Drop), true otherwise (Apply, including
    /// across a NextMessage live-resync gap).
    /// </summary>
    private bool RouteStat(ulong securityId, SymbolGapKind kind, uint receivedRptSeq, uint priorRptSeq)
    {
        if (receivedRptSeq == 0) return true; // schema absent; cannot gap-check
        var result = _stateRegistry.Observe(securityId, kind, receivedRptSeq);
        if (result.Action == SymbolStateRegistry.ObserveAction.Drop)
        {
            Interlocked.Increment(ref _droppedDuplicateStats);
            return false;
        }
        if (result.GapSize > 0)
            Interlocked.Increment(ref _liveResyncs);
        return true;
    }

    /// <summary>
    /// Per-symbol rptSeq gap shadow tracker (telemetry only). The registry is
    /// the source of truth for routing decisions; this counter remains useful
    /// for breakdown metrics.
    /// </summary>
    public SymbolGapTracker GapTracker { get; }

    public void FreezeData()
    {
        _frozenData = _data.ToFrozenDictionary();
    }

    public InstrumentInfo GetOrCreateInfo(ulong securityId)
    {
        if (_frozenData is { } frozen)
        {
            if (frozen.TryGetValue(securityId, out var info))
                return info;
        }

        return _data.GetOrAdd(securityId, static _ => new InstrumentInfo());
    }

    private bool TryLookupInfo(ulong securityId, out InstrumentInfo info)
    {
        if (_frozenData is { } frozen && frozen.TryGetValue(securityId, out info!))
            return true;
        return _data.TryGetValue(securityId, out info!);
    }

    public void OnPacket(in UmdfPacket packet, ReadOnlySpan<byte> sbePayload, ushort templateId)
    {
        try
        {
            var handler = new MarketDataSbeHandler { Owner = this };
            SbeDispatcher.Dispatch(sbePayload, ref handler);
        }
        catch (Exception ex)
        {
            Interlocked.Increment(ref _parseErrors);
            _logger.LogWarning(ex, "Error processing templateId={TemplateId}", templateId);
        }
    }

    /// <summary>
    /// Zero-cost devirtualized dispatcher for the market-data feed. See
    /// <c>BookManager.BookSbeHandler</c> for the same pattern. Methods left empty
    /// are templates this manager doesn't process (orders/trades/snapshots).
    /// </summary>
    private struct MarketDataSbeHandler : ISbeMessageHandler
    {
        public MarketDataManager Owner;

        public void OnSecurityDefinition_12(in SecurityDefinition_12DataReader reader, int blockLength, int version) => Owner.HandleSecurityDefinition(in reader);
        public void OnSecurityStatus_3(in SecurityStatus_3DataReader reader, int blockLength, int version) => Owner.HandleSecurityStatus(in reader);
        public void OnSecurityGroupPhase_10(in SecurityGroupPhase_10DataReader reader, int blockLength, int version) => Owner.HandleSecurityGroupPhase(in reader);
        public void OnOpeningPrice_15(in OpeningPrice_15DataReader reader, int blockLength, int version) => Owner.HandleOpeningPrice(in reader);
        public void OnTheoreticalOpeningPrice_16(in TheoreticalOpeningPrice_16DataReader reader, int blockLength, int version) => Owner.HandleTheoreticalOpeningPrice(in reader);
        public void OnClosingPrice_17(in ClosingPrice_17DataReader reader, int blockLength, int version) => Owner.HandleClosingPrice(in reader);
        public void OnAuctionImbalance_19(in AuctionImbalance_19DataReader reader, int blockLength, int version) => Owner.HandleAuctionImbalance(in reader);
        public void OnQuantityBand_21(in QuantityBand_21DataReader reader, int blockLength, int version) => Owner.HandleQuantityBand(in reader);
        public void OnPriceBand_22(in PriceBand_22DataReader reader, int blockLength, int version) => Owner.HandlePriceBand(in reader);
        public void OnHighPrice_24(in HighPrice_24DataReader reader, int blockLength, int version) => Owner.HandleHighPrice(in reader);
        public void OnLowPrice_25(in LowPrice_25DataReader reader, int blockLength, int version) => Owner.HandleLowPrice(in reader);
        public void OnLastTradePrice_27(in LastTradePrice_27DataReader reader, int blockLength, int version) => Owner.HandleLastTradePrice(in reader);
        public void OnSettlementPrice_28(in SettlementPrice_28DataReader reader, int blockLength, int version) => Owner.HandleSettlementPrice(in reader);
        public void OnOpenInterest_29(in OpenInterest_29DataReader reader, int blockLength, int version) => Owner.HandleOpenInterest(in reader);
        public void OnExecutionStatistics_56(in ExecutionStatistics_56DataReader reader, int blockLength, int version) => Owner.HandleExecutionStatistics(in reader);

        // Intentional no-ops: not consumed by the market-data manager (orders/trades/snapshots/news/etc).
        public void OnSequenceReset_1(in SequenceReset_1DataReader reader, int blockLength, int version) { }
        public void OnSequence_2(in Sequence_2DataReader reader, int blockLength, int version) { }
        public void OnEmptyBook_9(in EmptyBook_9DataReader reader, int blockLength, int version) { }
        public void OnChannelReset_11(in ChannelReset_11DataReader reader, int blockLength, int version) { }
        public void OnNews_5(in News_5DataReader reader, int blockLength, int version) { }
        public void OnSnapshotFullRefresh_Header_30(in SnapshotFullRefresh_Header_30DataReader reader, int blockLength, int version) { }
        public void OnOrder_MBO_50(in Order_MBO_50DataReader reader, int blockLength, int version) { }
        public void OnDeleteOrder_MBO_51(in DeleteOrder_MBO_51DataReader reader, int blockLength, int version) { }
        public void OnMassDeleteOrders_MBO_52(in MassDeleteOrders_MBO_52DataReader reader, int blockLength, int version) { }
        public void OnTrade_53(in Trade_53DataReader reader, int blockLength, int version) { }
        public void OnForwardTrade_54(in ForwardTrade_54DataReader reader, int blockLength, int version) { }
        public void OnExecutionSummary_55(in ExecutionSummary_55DataReader reader, int blockLength, int version) { }
        public void OnTradeBust_57(in TradeBust_57DataReader reader, int blockLength, int version) { }
        public void OnSnapshotFullRefresh_Orders_MBO_71(in SnapshotFullRefresh_Orders_MBO_71DataReader reader, int blockLength, int version) { }
        public void OnHeaderMessage_0(in HeaderMessage_0DataReader reader, int blockLength, int version) { }
        public void OnUnknownMessage(int templateId, int blockLength, int version, ReadOnlySpan<byte> payload) { }
    }

    public void OnSequenceReset()
    {
        // Snapshot recovery rebuilds order book state only. Instrument definitions and
        // incremental market-data fields are not replayed from the snapshot stream, so
        // clearing them here would blank the UI without any authoritative way to restore
        // them immediately. Keep the last known info until fresh updates arrive.
    }

    /// <summary>
    /// Spec §6.5.5.1 — SequenceVersion increment (weekly rollover / failover).
    /// Per-instrument rptSeq watermarks for each stat kind must be reset
    /// to 0 so the first post-version stat at rptSeq=1 is accepted by
    /// <see cref="SymbolStateRegistry"/>'s gap routing. Stat values
    /// themselves are preserved (UI keeps the last known value until the
    /// new session's first refresh arrives, mirroring
    /// <see cref="OnSequenceReset"/>'s rationale).
    /// </summary>
    public void OnSequenceVersionChanged(ushort newVersion)
    {
        foreach (var info in _data.Values)
        {
            info.LastRptSeqOpeningPrice = 0;
            info.LastRptSeqTheoreticalOpeningPrice = 0;
            info.LastRptSeqClosingPrice = 0;
            info.LastRptSeqAuctionImbalance = 0;
            info.LastRptSeqQuantityBand = 0;
            info.LastRptSeqPriceBand = 0;
            info.LastRptSeqHighPrice = 0;
            info.LastRptSeqLowPrice = 0;
            info.LastRptSeqLastTradePrice = 0;
            info.LastRptSeqSettlementPrice = 0;
            info.LastRptSeqOpenInterest = 0;
            info.LastRptSeqExecutionStatistics = 0;
            info.LastRptSeqSecurityStatus = 0;
        }
    }
    public void OnInstrumentDefinitionsComplete(int instrumentCount) { FreezeData(); }

    private void HandleSecurityDefinition(in SecurityDefinition_12DataReader reader)
    {
        ref readonly var msg = ref reader.Data;
        ulong securityId = (ulong)msg.SecurityID;
        var info = GetOrCreateInfo(securityId);

        // P11-2: skip-if-unchanged early-out. The InstrDef channel re-broadcasts
        // every SecurityDefinition every few seconds, but the exchange only
        // bumps SecurityValidityTimestamp when the definition actually changes
        // (corporate action / contract adjustment / new listing). Cache the
        // last-seen value on the instrument and bail before the expensive
        // parse + 6 string allocations + ReadGroups (4 handler delegates +
        // closure + lists). Profiling at 5x replay attributed ~1.6 GB / 600 s
        // (~31 % of total sampled allocations) to this method's hot body.
        // Defensive: if the wire has no validity timestamp (null sentinel),
        // fall through and parse — we have no key to compare against.
        long? newTsOpt = msg.SecurityValidityTimestamp.Time;
        if (newTsOpt is { } newTsSigned && newTsSigned > 0)
        {
            ulong newTs = (ulong)newTsSigned;
            if (info.LastSecurityValidityTimestamp == newTs)
            {
                Interlocked.Increment(ref _secDefSkippedCount);
                return;
            }
            info.LastSecurityValidityTimestamp = newTs;
        }

        string? oldGroup = info.SecurityGroup;
        string newGroup = Encoding.Latin1.GetString(msg.SecurityGroup.AsTrimmedSpan());
        info.SecurityGroup = newGroup;
        UpdateGroupMembership(securityId, info, oldGroup, newGroup);
        info.Symbol = Encoding.Latin1.GetString(msg.Symbol.AsTrimmedSpan());
        info.Asset = Encoding.Latin1.GetString(msg.Asset.AsTrimmedSpan());
        info.CfiCode = Encoding.Latin1.GetString(msg.CfiCode.AsTrimmedSpan());
        info.Currency = Encoding.Latin1.GetString(msg.Currency.AsTrimmedSpan());
        info.IsinNumber = Encoding.Latin1.GetString(msg.IsinNumber.AsTrimmedSpan());

        info.SecurityType = (int)msg.SecurityType;
        info.SecuritySubType = msg.SecuritySubType;
        info.Product = (int)msg.Product;
        info.MinPriceIncrement = msg.MinPriceIncrement.Mantissa;
        info.PriceDivisor = msg.PriceDivisor.Mantissa;
        info.ContractMultiplier = msg.ContractMultiplier.Mantissa;
        info.StrikePrice = msg.StrikePrice.Mantissa;
        info.MaturityDate = (int?)msg.MaturityDate;
        info.PutOrCall = msg.PutOrCall is { } poc ? (int)poc : null;
        info.ExerciseStyle = msg.ExerciseStyle is { } es ? (int)es : null;
        info.MarketSegmentID = msg.MarketSegmentID;
        info.TickSizeDenominator = msg.TickSizeDenominator;

        List<UnderlyingInfo>? underlyings = null;
        List<LegInfo>? legs = null;
        List<InstrAttribInfo>? attribs = null;
        string? secDesc = null;

        reader.ReadGroups(
            (in SecurityDefinition_12Data.NoUnderlyingsData u) =>
            {
                ulong uid = (ulong)u.UnderlyingSecurityID;
                if (uid == 0) return; // skip empty entries
                underlyings ??= new();
                underlyings.Add(new UnderlyingInfo
                {
                    SecurityId = uid,
                    Symbol = Encoding.Latin1.GetString(u.UnderlyingSymbol.AsTrimmedSpan()),
                });
            },
            (in SecurityDefinition_12Data.NoLegsData leg) =>
            {
                ulong lid = (ulong)leg.LegSecurityID;
                if (lid == 0) return; // skip empty entries
                legs ??= new();
                legs.Add(new LegInfo
                {
                    SecurityId = lid,
                    Symbol = Encoding.Latin1.GetString(leg.LegSymbol.AsTrimmedSpan()),
                    RatioQty = leg.LegRatioQty.Mantissa,
                    SecurityType = (int)leg.LegSecurityType,
                    Side = (int)leg.LegSide,
                });
            },
            (in SecurityDefinition_12Data.NoInstrAttribsData a) =>
            {
                attribs ??= new();
                attribs.Add(new InstrAttribInfo
                {
                    Type = (int)a.InstrAttribType,
                    Value = (int)a.InstrAttribValue,
                });
            },
            (TextEncoding desc) =>
            {
                if (desc.Length > 0)
                    secDesc = Encoding.UTF8.GetString(desc.VarData);
            }
        );

        info.Underlyings = underlyings;
        info.Legs = legs;
        info.InstrAttribs = attribs;
        info.SecurityDescription = secDesc;

        info.BumpVersion();
    }

    private void HandleSecurityStatus(in SecurityStatus_3DataReader reader)
    {
        ref readonly var msg = ref reader.Data;
        ulong securityId = (ulong)msg.SecurityID;
        var info = GetOrCreateInfo(securityId);

        if (msg.RptSeq is { } rs)
        {
            if (!RouteStat(securityId, SymbolGapKind.SecurityStatus, (uint)rs, info.LastRptSeqSecurityStatus))
                return;
            info.LastRptSeqSecurityStatus = (uint)rs;
        }

        int? eventCode = msg.SecurityTradingEvent is { } evt ? (int)evt : null;
        info.TradingEvent = eventCode;
        info.TradSesOpenTime = msg.TradSesOpenTime.Time;
        info.LastUpdateTimestamp = msg.TransactTime.Time ?? 0;

        // Per B3 spec §14.3: SecurityTradingEvent == 4 (TRADING_SESSION_CHANGE)
        // is the "end of day trading statistics reset" signal. Clear session
        // stats and stat-rptSeq watermarks so the next trading session starts
        // clean and post-reset stats at rptSeq=1 are accepted.
        if (eventCode == 4)
        {
            info.ResetSessionStatistics();
            Interlocked.Increment(ref _tradingSessionResets);
        }

        if (eventCode == 102) // SECURITY_REJOINS_SECURITY_GROUP_STATUS
        {
            info.FollowsGroupStatus = true;
            // Apply current group status if we already have it
            if (info.SecurityGroup is { } grp && _groupStatus.TryGetValue(grp, out int grpStatus))
                info.TradingStatus = grpStatus;
            else
                info.TradingStatus = (int)msg.SecurityTradingStatus;
        }
        else if (eventCode == 101) // SECURITY_STATUS_CHANGE (separate from group)
        {
            info.FollowsGroupStatus = false;
            info.TradingStatus = (int)msg.SecurityTradingStatus;
        }
        else
        {
            info.TradingStatus = (int)msg.SecurityTradingStatus;
        }

        info.BumpVersion();
        _eventHandler?.OnSecurityStatusChanged(securityId, info);
    }

    private void HandleSecurityGroupPhase(in SecurityGroupPhase_10DataReader reader)
    {
        ref readonly var msg = ref reader.Data;
        ReadOnlySpan<byte> groupBytes = msg.SecurityGroup.AsSpan();
        int status = (int)msg.TradingSessionSubID;

        // Intern the group string from the dictionary keys to avoid the
        // ToString().Trim() allocation on the hot path. New groups (cache
        // miss) fall back to the legacy normalization to preserve identity
        // with whatever HandleSecurityDefinition stored.
        string group = InternGroup(groupBytes) ?? Encoding.Latin1.GetString(msg.SecurityGroup.AsTrimmedSpan());

        _groupStatus[group] = status;

        // Propagate to instruments following this group — O(group-size)
        // instead of O(_data) by using the pre-built membership index.
        if (_groupMembers.TryGetValue(group, out var members))
        {
            foreach (var kv in members)
            {
                var info = kv.Value;
                if (info.FollowsGroupStatus)
                {
                    info.TradingStatus = status;
                    info.BumpVersion();
                    _eventHandler?.OnMarketDataUpdated(kv.Key, info);
                }
            }
        }
    }

    /// <summary>
    /// Lookup an existing interned group string by its raw SBE bytes
    /// (3-byte ASCII, possibly null/space padded). Returns null on miss
    /// so the caller can fall back to allocating + normalizing a new key.
    /// Linear scan is fine: there are typically &lt;50 distinct groups.
    /// </summary>
    private string? InternGroup(ReadOnlySpan<byte> bytes)
    {
        foreach (var key in _groupMembers.Keys)
        {
            if (KeyMatchesBytes(key, bytes)) return key;
        }
        foreach (var key in _groupStatus.Keys)
        {
            if (KeyMatchesBytes(key, bytes)) return key;
        }
        return null;
    }

    /// <summary>
    /// Compare an already-trimmed string key to raw SBE bytes. Mirrors the
    /// canonicalization used by <c>msg.SecurityGroup.ToString().Trim()</c>:
    /// trailing 0x00 (SBE pad) and 0x20 (ASCII space) are ignored.
    /// </summary>
    private static bool KeyMatchesBytes(string key, ReadOnlySpan<byte> bytes)
    {
        int len = bytes.Length;
        while (len > 0 && (bytes[len - 1] == 0 || bytes[len - 1] == (byte)' ')) len--;
        if (key.Length != len) return false;
        for (int i = 0; i < len; i++)
            if (key[i] != (char)bytes[i]) return false;
        return true;
    }

    /// <summary>
    /// Maintain <see cref="_groupMembers"/> when an instrument's
    /// SecurityGroup is assigned or re-assigned (cold path: instrument-def
    /// phase). Single-writer (feed thread).
    /// </summary>
    private void UpdateGroupMembership(ulong securityId, InstrumentInfo info, string? oldGroup, string newGroup)
    {
        if (oldGroup == newGroup) return;

        if (oldGroup is not null && _groupMembers.TryGetValue(oldGroup, out var oldList))
        {
            for (int i = 0; i < oldList.Count; i++)
            {
                if (oldList[i].Key == securityId)
                {
                    oldList.RemoveAt(i);
                    break;
                }
            }
        }

        if (!_groupMembers.TryGetValue(newGroup, out var newList))
        {
            newList = new List<KeyValuePair<ulong, InstrumentInfo>>();
            _groupMembers[newGroup] = newList;
        }
        // Defensive: avoid duplicate membership if HandleSecurityDefinition
        // re-fires with the same group for the same id.
        for (int i = 0; i < newList.Count; i++)
            if (newList[i].Key == securityId) return;
        newList.Add(new KeyValuePair<ulong, InstrumentInfo>(securityId, info));
    }

    private void HandleOpeningPrice(in OpeningPrice_15DataReader reader)
    {
        ref readonly var msg = ref reader.Data;
        ulong securityId = (ulong)msg.SecurityID;
        var info = GetOrCreateInfo(securityId);

        if (msg.RptSeq is { } rs)
        {
            if (!RouteStat(securityId, SymbolGapKind.OpeningPrice, (uint)rs, info.LastRptSeqOpeningPrice))
                return;
            info.LastRptSeqOpeningPrice = (uint)rs;
        }

        info.OpeningPrice = msg.MDEntryPx.Mantissa;
        info.NetChangeFromPrevDay = msg.NetChgPrevDay.Mantissa;
        info.LastUpdateTimestamp = msg.MDEntryTimestamp.Time ?? 0;
        info.BumpVersion();

        _eventHandler?.OnMarketDataUpdated(securityId, info);
    }

    private void HandleTheoreticalOpeningPrice(in TheoreticalOpeningPrice_16DataReader reader)
    {
        ref readonly var msg = ref reader.Data;
        ulong securityId = (ulong)msg.SecurityID;
        var info = GetOrCreateInfo(securityId);

        if (msg.RptSeq is { } rs)
        {
            if (!RouteStat(securityId, SymbolGapKind.TheoreticalOpeningPrice, (uint)rs, info.LastRptSeqTheoreticalOpeningPrice))
                return;
            info.LastRptSeqTheoreticalOpeningPrice = (uint)rs;
        }

        info.TheoreticalOpeningPrice = msg.MDEntryPx.Mantissa;
        info.TheoreticalOpeningSize = msg.MDEntrySize;
        info.LastUpdateTimestamp = msg.MDEntryTimestamp.Time ?? 0;
        info.BumpVersion();

        _eventHandler?.OnMarketDataUpdated(securityId, info);
    }

    private void HandleClosingPrice(in ClosingPrice_17DataReader reader)
    {
        ref readonly var msg = ref reader.Data;
        ulong securityId = (ulong)msg.SecurityID;
        var info = GetOrCreateInfo(securityId);

        if (msg.RptSeq is { } rs)
        {
            if (!RouteStat(securityId, SymbolGapKind.ClosingPrice, (uint)rs, info.LastRptSeqClosingPrice))
                return;
            info.LastRptSeqClosingPrice = (uint)rs;
        }

        info.ClosingPrice = msg.MDEntryPx.Mantissa;
        info.LastUpdateTimestamp = msg.MDEntryTimestamp.Time ?? 0;
        info.BumpVersion();

        _eventHandler?.OnMarketDataUpdated(securityId, info);
    }

    private void HandleAuctionImbalance(in AuctionImbalance_19DataReader reader)
    {
        ref readonly var msg = ref reader.Data;
        ulong securityId = (ulong)msg.SecurityID;
        var info = GetOrCreateInfo(securityId);

        if (msg.RptSeq is { } rs)
        {
            if (!RouteStat(securityId, SymbolGapKind.AuctionImbalance, (uint)rs, info.LastRptSeqAuctionImbalance))
                return;
            info.LastRptSeqAuctionImbalance = (uint)rs;
        }

        info.AuctionImbalanceSize = msg.MDEntrySize;
        info.LastUpdateTimestamp = msg.MDEntryTimestamp.Time ?? 0;
        info.BumpVersion();

        _eventHandler?.OnMarketDataUpdated(securityId, info);
    }

    private void HandleQuantityBand(in QuantityBand_21DataReader reader)
    {
        ref readonly var msg = ref reader.Data;
        ulong securityId = (ulong)msg.SecurityID;
        var info = GetOrCreateInfo(securityId);

        if (msg.RptSeq is { } rs)
        {
            if (!RouteStat(securityId, SymbolGapKind.QuantityBand, (uint)rs, info.LastRptSeqQuantityBand))
                return;
            info.LastRptSeqQuantityBand = (uint)rs;
        }

        info.AvgDailyTradedQty = msg.AvgDailyTradedQty;
        info.MaxTradeVol = msg.MaxTradeVol;
        info.LastUpdateTimestamp = msg.MDEntryTimestamp.Time ?? 0;
        info.BumpVersion();

        _eventHandler?.OnMarketDataUpdated(securityId, info);
    }

    private void HandlePriceBand(in PriceBand_22DataReader reader)
    {
        ref readonly var msg = ref reader.Data;
        ulong securityId = (ulong)msg.SecurityID;
        var info = GetOrCreateInfo(securityId);

        if (msg.RptSeq is { } rs)
        {
            if (!RouteStat(securityId, SymbolGapKind.PriceBand, (uint)rs, info.LastRptSeqPriceBand))
                return;
            info.LastRptSeqPriceBand = (uint)rs;
        }

        info.PriceBandLow = msg.LowLimitPrice.Mantissa;
        info.PriceBandHigh = msg.HighLimitPrice.Mantissa;
        // PriceLimitType is required for the consumer to interpret the band values
        // (PRICE_UNIT vs TICKS vs PERCENTAGE). For futures/percentages B3 sends e.g.
        // ±1.0000 as PERCENTAGE, which is meaningless without this discriminator.
        info.PriceLimitType = msg.PriceLimitType is { } plt ? (byte)plt : (byte?)null;
        info.TradingReferencePrice = msg.TradingReferencePrice.Mantissa;
        info.LastUpdateTimestamp = msg.MDEntryTimestamp.Time ?? 0;
        info.BumpVersion();

        _eventHandler?.OnMarketDataUpdated(securityId, info);
    }

    private void HandleHighPrice(in HighPrice_24DataReader reader)
    {
        ref readonly var msg = ref reader.Data;
        ulong securityId = (ulong)msg.SecurityID;
        var info = GetOrCreateInfo(securityId);

        if (msg.RptSeq is { } rs)
        {
            if (!RouteStat(securityId, SymbolGapKind.HighPrice, (uint)rs, info.LastRptSeqHighPrice))
                return;
            info.LastRptSeqHighPrice = (uint)rs;
        }

        info.HighPrice = msg.MDEntryPx.Mantissa;
        info.LastUpdateTimestamp = msg.MDEntryTimestamp.Time ?? 0;
        info.BumpVersion();

        _eventHandler?.OnMarketDataUpdated(securityId, info);
    }

    private void HandleLowPrice(in LowPrice_25DataReader reader)
    {
        ref readonly var msg = ref reader.Data;
        ulong securityId = (ulong)msg.SecurityID;
        var info = GetOrCreateInfo(securityId);

        if (msg.RptSeq is { } rs)
        {
            if (!RouteStat(securityId, SymbolGapKind.LowPrice, (uint)rs, info.LastRptSeqLowPrice))
                return;
            info.LastRptSeqLowPrice = (uint)rs;
        }

        info.LowPrice = msg.MDEntryPx.Mantissa;
        info.LastUpdateTimestamp = msg.MDEntryTimestamp.Time ?? 0;
        info.BumpVersion();

        _eventHandler?.OnMarketDataUpdated(securityId, info);
    }

    private void HandleLastTradePrice(in LastTradePrice_27DataReader reader)
    {
        ref readonly var msg = ref reader.Data;
        ulong securityId = (ulong)msg.SecurityID;
        var info = GetOrCreateInfo(securityId);

        if (msg.RptSeq is { } rs)
        {
            if (!RouteStat(securityId, SymbolGapKind.LastTradePrice, (uint)rs, info.LastRptSeqLastTradePrice))
                return;
            info.LastRptSeqLastTradePrice = (uint)rs;
        }

        info.LastTradePrice = msg.MDEntryPx.Mantissa;
        info.LastTradeSize = (long)msg.MDEntrySize;
        info.LastUpdateTimestamp = msg.MDEntryTimestamp.Time ?? 0;
        info.BumpVersion();

        _eventHandler?.OnMarketDataUpdated(securityId, info);
    }

    private void HandleSettlementPrice(in SettlementPrice_28DataReader reader)
    {
        ref readonly var msg = ref reader.Data;
        ulong securityId = (ulong)msg.SecurityID;
        var info = GetOrCreateInfo(securityId);

        if (msg.RptSeq is { } rs)
        {
            if (!RouteStat(securityId, SymbolGapKind.SettlementPrice, (uint)rs, info.LastRptSeqSettlementPrice))
                return;
            info.LastRptSeqSettlementPrice = (uint)rs;
        }

        info.SettlementPrice = msg.MDEntryPx.Mantissa;
        info.LastUpdateTimestamp = msg.MDEntryTimestamp.Time ?? 0;
        info.BumpVersion();

        _eventHandler?.OnMarketDataUpdated(securityId, info);
    }

    private void HandleOpenInterest(in OpenInterest_29DataReader reader)
    {
        ref readonly var msg = ref reader.Data;
        ulong securityId = (ulong)msg.SecurityID;
        var info = GetOrCreateInfo(securityId);

        if (msg.RptSeq is { } rs)
        {
            if (!RouteStat(securityId, SymbolGapKind.OpenInterest, (uint)rs, info.LastRptSeqOpenInterest))
                return;
            info.LastRptSeqOpenInterest = (uint)rs;
        }

        info.OpenInterest = (long)msg.MDEntrySize;
        info.LastUpdateTimestamp = msg.MDEntryTimestamp.Time ?? 0;
        info.BumpVersion();

        _eventHandler?.OnMarketDataUpdated(securityId, info);
    }

    private void HandleExecutionStatistics(in ExecutionStatistics_56DataReader reader)
    {
        ref readonly var msg = ref reader.Data;
        ulong securityId = (ulong)msg.SecurityID;
        var info = GetOrCreateInfo(securityId);

        if (msg.RptSeq is { } rs)
        {
            if (!RouteStat(securityId, SymbolGapKind.ExecutionStatistics, (uint)rs, info.LastRptSeqExecutionStatistics))
                return;
            info.LastRptSeqExecutionStatistics = (uint)rs;
        }

        info.TradeVolume = (long)msg.TradeVolume;
        info.VwapPrice = msg.VwapPx.Mantissa;
        info.NetChangeFromPrevDay = msg.NetChgPrevDay.Mantissa;
        info.NumberOfTrades = (long)(uint)msg.NumberOfTrades;
        info.LastUpdateTimestamp = msg.MDEntryTimestamp.Time ?? 0;
        info.BumpVersion();

        _eventHandler?.OnMarketDataUpdated(securityId, info);
    }
}
