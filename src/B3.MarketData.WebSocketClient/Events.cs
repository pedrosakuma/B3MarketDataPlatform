namespace B3.MarketData.WebSocketClient;

/// <summary>
/// Mirror of the server's <c>ServerCapabilities</c> bitfield. Surfaced so client
/// callers can inspect <see cref="ServerHelloEvent.Capabilities"/> without
/// hard-coding bit positions. Unknown bits MUST be ignored — never gate behaviour
/// on the absence of a known bit (that's how forward-compat breaks).
/// </summary>
[Flags]
public enum ServerCapabilities : uint
{
    None = 0,
    SnapshotOnSubscribe = 0x0001,
    SymbolDelistedNotification = 0x0002,
}

/// <summary>Connection state surfaced via <see cref="MarketDataClient.ConnectionStateChanged"/>.</summary>
public enum ConnectionState
{
    Disconnected = 0,
    Connecting = 1,
    Connected = 2,
    Reconnecting = 3,
    Faulted = 4,
}

/// <summary>
/// A live trade print. Price has the SBE 4-decimal exponent already
/// applied (the raw wire value is <c>price × 10_000</c>).
/// </summary>
/// <param name="SecurityId">B3 security id.</param>
/// <param name="Symbol">Resolved symbol (from the client-side
/// <c>SubscribeOk</c> cache).</param>
/// <param name="Price">Trade price, scaled. <c>raw / 10_000m</c>.</param>
/// <param name="Qty">Trade quantity (raw, unscaled).</param>
/// <param name="TradeId">Server-assigned trade id (for correlation with
/// a future <see cref="TradeBustEvent"/>).</param>
/// <param name="ReceivedUtc">UTC timestamp at which the SDK received
/// the frame from the WebSocket. Not the exchange match time.</param>
public readonly record struct TradeEvent(
    ulong SecurityId,
    string Symbol,
    decimal Price,
    long Qty,
    long TradeId,
    DateTime ReceivedUtc);

/// <summary>
/// Cancellation of a previously-broadcast trade. Risk consumers usually
/// ignore this (the live tape has already moved on); audit consumers
/// should remove the print from their trade history by
/// <see cref="TradeId"/>.
/// </summary>
public readonly record struct TradeBustEvent(
    ulong SecurityId,
    string Symbol,
    long TradeId,
    DateTime ReceivedUtc);

/// <summary>
/// Per-symbol info snapshot. Only fields actually present in the frame
/// are populated; absent fields are <c>null</c>. Prices are scaled with
/// the SBE 4-decimal exponent.
/// </summary>
public sealed class InfoSnapshotEvent
{
    public ulong SecurityId { get; init; }
    public string Symbol { get; init; } = "";
    public DateTime ReceivedUtc { get; init; }

    public decimal? OpeningPrice { get; init; }
    public decimal? ClosingPrice { get; init; }
    public decimal? HighPrice { get; init; }
    public decimal? LowPrice { get; init; }
    public decimal? LastTradePrice { get; init; }
    public long? LastTradeSize { get; init; }
    public decimal? SettlementPrice { get; init; }
    public decimal? VwapPrice { get; init; }
    public long? NumberOfTrades { get; init; }
    public long? OpenInterest { get; init; }
    public decimal? PriceBandLow { get; init; }
    public decimal? PriceBandHigh { get; init; }
    public decimal? TradingReferencePrice { get; init; }
    public long? TradeVolume { get; init; }
    public long? TradingStatus { get; init; }
    public long? TradingEvent { get; init; }
}

/// <summary>
/// Server feed status. <see cref="Ready"/> = <c>true</c> once every
/// upstream feed group is in <c>RealTime</c>. The server emits one on
/// connect and one on each transition; treat <c>Ready=false</c> as
/// "do not subscribe yet".
/// </summary>
public readonly record struct ServerStatusEvent(bool Ready, DateTime ReceivedUtc);

/// <summary>
/// First server-initiated frame on a fresh connection. Carries the server-side
/// protocol version, advertised capabilities bitfield, and build version string.
/// Surfaced both as the <c>MarketDataClient.ServerHello</c> event and snapshotted
/// on the <c>MarketDataClient.LastServerHello</c> property so late subscribers can
/// still inspect the negotiation result.
/// </summary>
/// <param name="ProtocolVersion">Server's wire-protocol version. The SDK treats
/// any value &gt;= 1 as compatible today; bumps signal a breaking change.</param>
/// <param name="Capabilities">Bitfield of optional features the server advertises.
/// Unknown bits MUST be treated as reserved; do not gate behaviour on their absence.</param>
/// <param name="BuildVersion">Free-form server build identifier (assembly version,
/// semver, or git SHA). Surface as-is to operators; do not parse.</param>
/// <param name="ReceivedUtc">UTC timestamp at which the SDK received the frame.</param>
public readonly record struct ServerHelloEvent(
    uint ProtocolVersion,
    ServerCapabilities Capabilities,
    string BuildVersion,
    DateTime ReceivedUtc);

/// <summary>
/// Terminal notification for a previously-subscribed security. Risk consumers
/// MUST stop expecting further data for <see cref="SecurityId"/>; UI consumers
/// SHOULD remove the symbol from their listings. Server has already torn down
/// its per-symbol subscription map by the time this fires, so the SDK does NOT
/// auto-unsubscribe (no Unsubscribe frame is sent).
/// </summary>
public readonly record struct SymbolDelistedEvent(
    ulong SecurityId,
    string Symbol,
    DateTime ReceivedUtc);

/// <summary>
/// Returned by the server when a <c>Subscribe</c> can't be satisfied.
/// </summary>
public enum SubscribeErrorCode : byte
{
    Unknown = 0,
    UnknownSymbol = 0x01,
    NotReady = 0x02,
}

public readonly record struct SubscribeErrorEvent(
    string Symbol,
    SubscribeErrorCode ErrorCode,
    DateTime ReceivedUtc);

public readonly record struct ConnectionStateChangedEvent(
    ConnectionState State,
    Exception? Error,
    DateTime ChangedUtc);
