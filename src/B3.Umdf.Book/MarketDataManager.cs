using System.Collections.Concurrent;
using System.Collections.Frozen;
using System.Text;
using B3.Umdf.Feed;
using B3.Umdf.Mbo.Sbe.V16;
using B3.Umdf.Transport;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

using LastTradePrice_27Data = B3.Umdf.Mbo.Sbe.V16.V6.LastTradePrice_27Data;

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

    /// <summary>
    /// Thread-safe dictionary of all instrument data.
    /// Safe to read (Count, iterate) from any thread while the feed thread writes.
    /// </summary>
    public IReadOnlyDictionary<ulong, InstrumentInfo> InstrumentData => _data;

    /// <summary>Number of SBE parse errors encountered.</summary>
    public long ParseErrors => Volatile.Read(ref _parseErrors);

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

    /// <summary>Symbol state registry (PerSymbol recovery is the only supported mode).</summary>
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
        if (!MessageHeader.TryParse(sbePayload, out var header, out _))
            return;

        var body = sbePayload[MessageHeader.MESSAGE_SIZE..];

        try
        {
            switch (templateId)
            {
                case SecurityDefinition_12Data.MESSAGE_ID:
                    HandleSecurityDefinition(body, header.BlockLength);
                    break;
                case SecurityStatus_3Data.MESSAGE_ID:
                    HandleSecurityStatus(body);
                    break;
                case SecurityGroupPhase_10Data.MESSAGE_ID:
                    HandleSecurityGroupPhase(body);
                    break;
                case OpeningPrice_15Data.MESSAGE_ID:
                    HandleOpeningPrice(body);
                    break;
                case TheoreticalOpeningPrice_16Data.MESSAGE_ID:
                    HandleTheoreticalOpeningPrice(body);
                    break;
                case ClosingPrice_17Data.MESSAGE_ID:
                    HandleClosingPrice(body);
                    break;
                case AuctionImbalance_19Data.MESSAGE_ID:
                    HandleAuctionImbalance(body);
                    break;
                case QuantityBand_21Data.MESSAGE_ID:
                    HandleQuantityBand(body);
                    break;
                case PriceBand_22Data.MESSAGE_ID:
                    HandlePriceBand(body);
                    break;
                case HighPrice_24Data.MESSAGE_ID:
                    HandleHighPrice(body);
                    break;
                case LowPrice_25Data.MESSAGE_ID:
                    HandleLowPrice(body);
                    break;
                case LastTradePrice_27Data.MESSAGE_ID:
                    HandleLastTradePrice(body);
                    break;
                case SettlementPrice_28Data.MESSAGE_ID:
                    HandleSettlementPrice(body);
                    break;
                case OpenInterest_29Data.MESSAGE_ID:
                    HandleOpenInterest(body);
                    break;
                case ExecutionStatistics_56Data.MESSAGE_ID:
                    HandleExecutionStatistics(body);
                    break;
            }
        }
        catch (Exception ex)
        {
            Interlocked.Increment(ref _parseErrors);
            _logger.LogWarning(ex, "Error processing templateId={TemplateId}", templateId);
        }
    }

    public void OnSequenceReset()
    {
        // Snapshot recovery rebuilds order book state only. Instrument definitions and
        // incremental market-data fields are not replayed from the snapshot stream, so
        // clearing them here would blank the UI without any authoritative way to restore
        // them immediately. Keep the last known info until fresh updates arrive.
    }
    public void OnInstrumentDefinitionsComplete(int instrumentCount) { FreezeData(); }

    private void HandleSecurityDefinition(ReadOnlySpan<byte> body, int blockLength)
    {
        if (!SecurityDefinition_12Data.TryParse(body, blockLength, out var reader))
            return;

        ref readonly var msg = ref reader.Data;
        ulong securityId = (ulong)msg.SecurityID;
        var info = GetOrCreateInfo(securityId);

        string? oldGroup = info.SecurityGroup;
        string newGroup = msg.SecurityGroup.ToString().Trim();
        info.SecurityGroup = newGroup;
        UpdateGroupMembership(securityId, info, oldGroup, newGroup);
        info.Symbol = msg.Symbol.ToString().Trim();
        info.Asset = msg.Asset.ToString().Trim();
        info.CfiCode = msg.CfiCode.ToString().Trim();
        info.Currency = msg.Currency.ToString().Trim();
        info.IsinNumber = msg.IsinNumber.ToString().Trim();

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
                    Symbol = u.UnderlyingSymbol.ToString().Trim(),
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
                    Symbol = leg.LegSymbol.ToString().Trim(),
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

    private void HandleSecurityStatus(ReadOnlySpan<byte> body)
    {
        if (!SecurityStatus_3Data.TryParse(body, out var reader))
            return;

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

    private void HandleSecurityGroupPhase(ReadOnlySpan<byte> body)
    {
        if (!SecurityGroupPhase_10Data.TryParse(body, out var reader))
            return;

        ref readonly var msg = ref reader.Data;
        ReadOnlySpan<byte> groupBytes = msg.SecurityGroup.AsSpan();
        int status = (int)msg.TradingSessionSubID;

        // Intern the group string from the dictionary keys to avoid the
        // ToString().Trim() allocation on the hot path. New groups (cache
        // miss) fall back to the legacy normalization to preserve identity
        // with whatever HandleSecurityDefinition stored.
        string group = InternGroup(groupBytes) ?? msg.SecurityGroup.ToString().Trim();

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

    private void HandleOpeningPrice(ReadOnlySpan<byte> body)
    {
        if (!OpeningPrice_15Data.TryParse(body, out var reader))
            return;

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

    private void HandleTheoreticalOpeningPrice(ReadOnlySpan<byte> body)
    {
        if (!TheoreticalOpeningPrice_16Data.TryParse(body, out var reader))
            return;

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

    private void HandleClosingPrice(ReadOnlySpan<byte> body)
    {
        if (!ClosingPrice_17Data.TryParse(body, out var reader))
            return;

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

    private void HandleAuctionImbalance(ReadOnlySpan<byte> body)
    {
        if (!AuctionImbalance_19Data.TryParse(body, out var reader))
            return;

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

    private void HandleQuantityBand(ReadOnlySpan<byte> body)
    {
        if (!QuantityBand_21Data.TryParse(body, out var reader))
            return;

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

    private void HandlePriceBand(ReadOnlySpan<byte> body)
    {
        if (!PriceBand_22Data.TryParse(body, out var reader))
            return;

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

    private void HandleHighPrice(ReadOnlySpan<byte> body)
    {
        if (!HighPrice_24Data.TryParse(body, out var reader))
            return;

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

    private void HandleLowPrice(ReadOnlySpan<byte> body)
    {
        if (!LowPrice_25Data.TryParse(body, out var reader))
            return;

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

    private void HandleLastTradePrice(ReadOnlySpan<byte> body)
    {
        if (!LastTradePrice_27Data.TryParse(body, out var reader))
            return;

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

    private void HandleSettlementPrice(ReadOnlySpan<byte> body)
    {
        if (!SettlementPrice_28Data.TryParse(body, out var reader))
            return;

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

    private void HandleOpenInterest(ReadOnlySpan<byte> body)
    {
        if (!OpenInterest_29Data.TryParse(body, out var reader))
            return;

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

    private void HandleExecutionStatistics(ReadOnlySpan<byte> body)
    {
        if (!ExecutionStatistics_56Data.TryParse(body, out var reader))
            return;

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
