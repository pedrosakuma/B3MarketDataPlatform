using System.Collections.Frozen;
using B3.Umdf.Feed;
using B3.Umdf.Mbo.Sbe.V16;
using B3.Umdf.Transport;

using LastTradePrice_27Data = B3.Umdf.Mbo.Sbe.V16.V6.LastTradePrice_27Data;

namespace B3.Umdf.Book;

public sealed class MarketDataManager : IFeedEventHandler
{
    private Dictionary<ulong, InstrumentInfo> _mutableData = new();
    private FrozenDictionary<ulong, InstrumentInfo>? _frozenData;
    private readonly IMarketDataEventHandler? _eventHandler;

    /// <summary>
    /// Returns FrozenDictionary after instrument definitions (optimized lookups),
    /// falls back to mutable dictionary during setup.
    /// </summary>
    public IReadOnlyDictionary<ulong, InstrumentInfo> InstrumentData =>
        (IReadOnlyDictionary<ulong, InstrumentInfo>?)_frozenData ?? _mutableData;

    public MarketDataManager(IMarketDataEventHandler? eventHandler = null)
    {
        _eventHandler = eventHandler;
    }

    public void EnsureCapacity(int instrumentCount)
    {
        _mutableData.EnsureCapacity(instrumentCount);
    }

    public void FreezeData()
    {
        _frozenData = _mutableData.ToFrozenDictionary();
    }

    public InstrumentInfo GetOrCreateInfo(ulong securityId)
    {
        if (_frozenData is not null)
        {
            if (_frozenData.TryGetValue(securityId, out var info))
                return info;
            // New instrument after freeze — fall back to mutable without re-freezing on hot path
            if (_mutableData.TryGetValue(securityId, out info))
                return info;
            info = new InstrumentInfo();
            _mutableData[securityId] = info;
            return info;
        }

        if (!_mutableData.TryGetValue(securityId, out var mInfo))
        {
            mInfo = new InstrumentInfo();
            _mutableData[securityId] = mInfo;
        }
        return mInfo;
    }

    private bool TryLookupInfo(ulong securityId, out InstrumentInfo info)
    {
        if (_frozenData is not null)
            return _frozenData.TryGetValue(securityId, out info!);
        return _mutableData.TryGetValue(securityId, out info!);
    }

    public void OnPacket(in UmdfPacket packet, ReadOnlySpan<byte> sbePayload, ushort templateId)
    {
        if (sbePayload.Length < MessageHeader.MESSAGE_SIZE)
            return;

        var body = sbePayload[MessageHeader.MESSAGE_SIZE..];

        switch (templateId)
        {
            case SecurityStatus_3Data.MESSAGE_ID:
                HandleSecurityStatus(body);
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

    public void OnGapDetected(uint expected, uint received) { }
    public void OnSequenceReset() { ClearAllInfo(); }
    public void OnSnapshotStart() { }
    public void OnSnapshotComplete(uint lastRptSeq) { FreezeData(); }
    public void OnInstrumentDefinitionsComplete(int instrumentCount)
    {
        EnsureCapacity(instrumentCount);
    }

    private void HandleSecurityStatus(ReadOnlySpan<byte> body)
    {
        if (!SecurityStatus_3Data.TryParse(body, out var reader))
            return;

        ref readonly var msg = ref reader.Data;
        ulong securityId = (ulong)msg.SecurityID;
        var info = GetOrCreateInfo(securityId);

        info.TradingStatus = (int)msg.SecurityTradingStatus;
        info.TradingEvent = msg.SecurityTradingEvent is { } evt ? (int)evt : null;
        info.TradSesOpenTime = msg.TradSesOpenTime.Time;
        info.LastUpdateTimestamp = msg.TransactTime.Time ?? 0;

        _eventHandler?.OnSecurityStatusChanged(securityId, info);
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

        _eventHandler?.OnMarketDataUpdated(securityId, info);
    }

    private void ClearAllInfo()
    {
        foreach (var (_, info) in _frozenData is not null
            ? (IEnumerable<KeyValuePair<ulong, InstrumentInfo>>)_frozenData
            : _mutableData)
        {
            info.Reset();
        }
    }
}
