using B3.Umdf.Book;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;

namespace B3.Umdf.Server.Hosting;

internal sealed class InstrumentEndpointMapper
{
    private readonly SubscriptionManager _subscriptionManager;

    public InstrumentEndpointMapper(SubscriptionManager subscriptionManager)
    {
        _subscriptionManager = subscriptionManager;
    }

    public void Map(WebApplication app)
    {
        app.MapGet("/instrument/{symbol}", (string symbol) =>
        {
            var registry = _subscriptionManager.SymbolRegistry;
            if (registry is null)
                return Results.StatusCode(503);

            symbol = symbol.Trim().ToUpperInvariant();
            if (!registry.TryResolve(symbol, out var securityId))
                return Results.NotFound();
            var info = _subscriptionManager.FindInstrumentInfo(securityId);
            if (info is null)
                return Results.NotFound();

            var resp = BuildInstrumentInfoResponse(securityId, info);
            return Results.Json(resp, AppJsonContext.Default.InstrumentInfoResponse);
        });
    }

    /// <summary>
    /// Pure mapping helper: copy every field from the internal <see cref="InstrumentInfo"/>
    /// snapshot into the public DTO. Mechanical and large by necessity (~50 fields covering
    /// reference data + statistics + collections); kept separate so the endpoint registration
    /// stays scannable.
    /// </summary>
    internal static InstrumentInfoResponse BuildInstrumentInfoResponse(ulong securityId, InstrumentInfo info)
    {
        return new InstrumentInfoResponse
        {
            SecurityId = securityId,
            Symbol = info.Symbol,
            Asset = info.Asset,
            IsinNumber = info.IsinNumber,
            Currency = info.Currency,
            CfiCode = info.CfiCode,
            SecurityGroup = info.SecurityGroup,
            SecurityDescription = info.SecurityDescription,
            SecurityType = info.SecurityType,
            SecuritySubType = info.SecuritySubType,
            Product = info.Product,
            MinPriceIncrement = info.MinPriceIncrement,
            PriceDivisor = info.PriceDivisor,
            ContractMultiplier = info.ContractMultiplier,
            StrikePrice = info.StrikePrice,
            MaturityDate = info.MaturityDate,
            PutOrCall = info.PutOrCall,
            ExerciseStyle = info.ExerciseStyle,
            MarketSegmentID = info.MarketSegmentID,
            TickSizeDenominator = info.TickSizeDenominator,
            TradingStatus = info.TradingStatus,
            TradingEvent = info.TradingEvent,
            OpeningPrice = info.OpeningPrice,
            ClosingPrice = info.ClosingPrice,
            HighPrice = info.HighPrice,
            LowPrice = info.LowPrice,
            LastTradePrice = info.LastTradePrice,
            LastTradeSize = info.LastTradeSize,
            SettlementPrice = info.SettlementPrice,
            TheoreticalOpeningPrice = info.TheoreticalOpeningPrice,
            TheoreticalOpeningSize = info.TheoreticalOpeningSize,
            AuctionImbalanceSize = info.AuctionImbalanceSize,
            PriceBandLow = info.PriceBandLow,
            PriceBandHigh = info.PriceBandHigh,
            PriceLimitType = info.PriceLimitType,
            TradingReferencePrice = info.TradingReferencePrice,
            AvgDailyTradedQty = info.AvgDailyTradedQty,
            MaxTradeVol = info.MaxTradeVol,
            TradeVolume = info.TradeVolume,
            VwapPrice = info.VwapPrice,
            NetChangeFromPrevDay = info.NetChangeFromPrevDay,
            NumberOfTrades = info.NumberOfTrades,
            OpenInterest = info.OpenInterest,
            LastUpdateTimestamp = info.LastUpdateTimestamp,
            Underlyings = info.Underlyings?.Select(u => new UnderlyingResponse
            {
                SecurityId = u.SecurityId,
                Symbol = u.Symbol,
            }).ToList(),
            Legs = info.Legs?.Select(l => new LegResponse
            {
                SecurityId = l.SecurityId,
                Symbol = l.Symbol,
                RatioQty = l.RatioQty,
                SecurityType = l.SecurityType,
                Side = l.Side,
            }).ToList(),
            InstrAttribs = info.InstrAttribs?.Select(a => new InstrAttribResponse
            {
                Type = a.Type,
                Value = a.Value,
            }).ToList(),
        };
    }
}
