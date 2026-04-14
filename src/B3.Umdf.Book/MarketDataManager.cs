using System.Collections.Concurrent;
using System.Collections.Frozen;
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

    public MarketDataManager(IMarketDataEventHandler? eventHandler = null, ILogger<MarketDataManager>? logger = null)
    {
        _eventHandler = eventHandler;
        _logger = logger ?? NullLogger<MarketDataManager>.Instance;
    }

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
        if (sbePayload.Length < MessageHeader.MESSAGE_SIZE)
            return;

        var body = sbePayload[MessageHeader.MESSAGE_SIZE..];

        try
        {
            switch (templateId)
            {
                case SecurityDefinition_12Data.MESSAGE_ID:
                    HandleSecurityDefinition(body);
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

    public void OnGapDetected(uint expected, uint received) { }
    public void OnSequenceReset() { ClearAllInfo(); }
    public void OnSnapshotStart() { }
    public void OnSnapshotComplete(uint lastRptSeq) { FreezeData(); }
    public void OnInstrumentDefinitionsComplete(int instrumentCount) { }

    private void HandleSecurityDefinition(ReadOnlySpan<byte> body)
    {
        if (!SecurityDefinition_12Data.TryParse(body, out var reader))
            return;

        ref readonly var msg = ref reader.Data;
        ulong securityId = (ulong)msg.SecurityID;
        var info = GetOrCreateInfo(securityId);
        info.SecurityGroup = msg.SecurityGroup.ToString().Trim();
        info.BumpVersion();
    }

    private void HandleSecurityStatus(ReadOnlySpan<byte> body)
    {
        if (!SecurityStatus_3Data.TryParse(body, out var reader))
            return;

        ref readonly var msg = ref reader.Data;
        ulong securityId = (ulong)msg.SecurityID;
        var info = GetOrCreateInfo(securityId);

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
        string group = msg.SecurityGroup.ToString().Trim();
        int status = (int)msg.TradingSessionSubID;

        _groupStatus[group] = status;

        // Propagate to all instruments following this group
        foreach (var kvp in _data)
        {
            var info = kvp.Value;
            if (info.FollowsGroupStatus && info.SecurityGroup == group)
            {
                info.TradingStatus = status;
                info.BumpVersion();
                _eventHandler?.OnMarketDataUpdated(kvp.Key, info);
            }
        }
    }

    private void HandleOpeningPrice(ReadOnlySpan<byte> body)
    {
        if (!OpeningPrice_15Data.TryParse(body, out var reader))
            return;

        ref readonly var msg = ref reader.Data;
        ulong securityId = (ulong)msg.SecurityID;
        var info = GetOrCreateInfo(securityId);

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

        info.PriceBandLow = msg.LowLimitPrice.Mantissa;
        info.PriceBandHigh = msg.HighLimitPrice.Mantissa;
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

        info.TradeVolume = (long)msg.TradeVolume;
        info.VwapPrice = msg.VwapPx.Mantissa;
        info.NetChangeFromPrevDay = msg.NetChgPrevDay.Mantissa;
        info.NumberOfTrades = (long)(uint)msg.NumberOfTrades;
        info.LastUpdateTimestamp = msg.MDEntryTimestamp.Time ?? 0;
        info.BumpVersion();

        _eventHandler?.OnMarketDataUpdated(securityId, info);
    }

    private void ClearAllInfo()
    {
        foreach (var (_, info) in _data)
        {
            info.Reset();
        }
    }
}
