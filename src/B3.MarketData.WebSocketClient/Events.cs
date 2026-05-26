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
/// Per-trade flag bitset. Currently signals whether the print was executed
/// during an auction phase (opening cross, pre-open, or final-closing-call).
/// Reserved bits are kept 0 for forward compatibility; mask with the
/// individual values rather than equality-checking the whole byte.
/// </summary>
[System.Flags]
public enum TradeFlags : byte
{
    /// <summary>No flags set — regular open-phase print.</summary>
    None = 0,
    /// <summary>
    /// Trade is an auction print: either the SBE
    /// <c>TradeCondition.OpeningPrice</c> bit was set on the source message
    /// (opening / reopening cross), or the security's trading status was in
    /// an auction phase (<c>RESERVED</c> / <c>FINAL_CLOSING_CALL</c>) at the
    /// time of the trade.
    /// </summary>
    AuctionPrint = 1,
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
/// <param name="Flags">Per-trade flag bitset — see <see cref="TradeFlags"/>.
/// <see cref="TradeFlags.None"/> when the server pre-dates the flag byte
/// (the SDK auto-detects payload length and defaults to <c>None</c>).
/// Trades replayed from the server's recent-trades snapshot preserve the
/// per-trade flags captured at ingest time.</param>
public readonly record struct TradeEvent(
    ulong SecurityId,
    string Symbol,
    decimal Price,
    long Qty,
    long TradeId,
    DateTime ReceivedUtc,
    TradeFlags Flags = TradeFlags.None);

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
/// Decoded auction-imbalance side from <c>AuctionImbalance_19</c> (SBE schema
/// 2.2.0 §AuctionImbalance). <see cref="Balanced"/> = both bits clear (no
/// pending side); <see cref="MoreBuyers"/> / <see cref="MoreSellers"/> mirror
/// the SBE <c>ImbalanceMoreBuyers</c> / <c>ImbalanceMoreSellers</c> flags.
/// <see cref="Unknown"/> covers reserved combinations (both bits set) — kept
/// distinct so forward-compat additions in the schema do not silently collapse
/// to a known value.
/// </summary>
public enum AuctionImbalanceCondition : byte
{
    Balanced = 0,
    MoreBuyers = 1,
    MoreSellers = 2,
    Unknown = 3,
}

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

    // ── Auction ─────────────────────────────────────────────────────
    /// <summary>
    /// Latest theoretical opening price from <c>TheoreticalOpeningPrice_16</c>.
    /// Calculated and updated by the venue during every auction (pre-opening,
    /// pre-closing, intra-day reopen). Scaled with the SBE 4-decimal exponent
    /// (raw / 10_000). <c>null</c> when the venue has not yet published a TOP
    /// for this security, or when the field is not present in this frame.
    /// </summary>
    public decimal? TheoreticalOpeningPrice { get; init; }

    /// <summary>
    /// Theoretical opening quantity matched at <see cref="TheoreticalOpeningPrice"/>
    /// (raw, unscaled).
    /// </summary>
    public long? TheoreticalOpeningSize { get; init; }

    /// <summary>
    /// Remaining auction quantity from <c>AuctionImbalance_19</c> — the size
    /// of the pending side (or 0 when balanced). Raw, unscaled. Pair with
    /// <see cref="AuctionImbalanceCondition"/> to know which side is pending.
    /// </summary>
    public long? AuctionImbalanceSize { get; init; }

    /// <summary>
    /// Pending side of the auction imbalance. <c>null</c> when the venue
    /// has not yet published an <c>AuctionImbalance</c> for this security
    /// or when the field is not present in this frame.
    /// </summary>
    public AuctionImbalanceCondition? AuctionImbalanceCondition { get; init; }
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
/// Client-side notification raised after <see cref="MarketDataClient.UnsubscribeAsync"/>
/// drops a symbol from the active subscription set. <see cref="SecurityId"/>
/// is zero when no <c>SubscribeOk</c> had been observed yet.
/// </summary>
public readonly record struct UnsubscribedEvent(
    string Symbol,
    ulong SecurityId,
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

/// <summary>
/// Order book side. Matches the server's internal <c>BookSideType</c> /
/// SBE <c>MDEntryType</c> wire byte (0 = Bid, 1 = Ask).
/// </summary>
public enum BookSide : byte
{
    Bid = 0,
    Ask = 1,
}

/// <summary>
/// Side(s) cleared by a <see cref="BookClearedEvent"/>. Matches the server's
/// <c>BookClearSide</c> wire byte (0 = Both, 1 = Bid, 2 = Ask).
/// </summary>
public enum BookClearSide : byte
{
    Both = 0,
    Bid = 1,
    Ask = 2,
}

// ── L3 / Order-by-order book (MBO) ──────────────────────────────────

/// <summary>One MBO order inside a <see cref="BookSnapshotEvent"/>.
/// Price is scaled with the SBE 4-decimal exponent already applied.</summary>
public readonly record struct BookSnapshotOrder(
    ulong OrderId,
    decimal Price,
    long Qty);

/// <summary>
/// L3 / order-by-order snapshot phase boundary. The server currently
/// emits this as an empty marker frame (zero entries on both sides)
/// followed by one <see cref="OrderAddedEvent"/> per live order on the
/// book — so consumers SHOULD clear any prior state for
/// <see cref="SecurityId"/> when this event fires and then rebuild from
/// the OrderAdded stream that follows in the same WebSocket packet.
/// <para>The wire format also allows aggregated price-level entries
/// inside the snapshot body (price/totalQty/orderCount), which are
/// surfaced via <see cref="Bids"/> / <see cref="Asks"/> with
/// <see cref="BookSnapshotOrder.OrderId"/> set to 0 — present for
/// forward-compatibility but empty in production traffic today.</para>
/// </summary>
public sealed class BookSnapshotEvent
{
    public ulong SecurityId { get; init; }
    public string Symbol { get; init; } = "";
    /// <summary>Per-symbol RptSeq at snapshot publish time; matches the value
    /// the next incremental will carry on the wire.</summary>
    public uint RptSeq { get; init; }
    public IReadOnlyList<BookSnapshotOrder> Bids { get; init; } = Array.Empty<BookSnapshotOrder>();
    public IReadOnlyList<BookSnapshotOrder> Asks { get; init; } = Array.Empty<BookSnapshotOrder>();
    public DateTime ReceivedUtc { get; init; }
}

/// <summary>Per-order Add. Price already scaled. Fires for both
/// <c>OrderAdded</c> (0x0030) opcodes.</summary>
public readonly record struct OrderAddedEvent(
    ulong SecurityId,
    string Symbol,
    ulong OrderId,
    BookSide Side,
    decimal Price,
    long Qty,
    DateTime ReceivedUtc);

/// <summary>Per-order Update (qty / price change). Price already scaled.</summary>
public readonly record struct OrderUpdatedEvent(
    ulong SecurityId,
    string Symbol,
    ulong OrderId,
    BookSide Side,
    decimal Price,
    long Qty,
    DateTime ReceivedUtc);

/// <summary>Per-order Delete. Consumers MUST drop the matching
/// <see cref="OrderId"/> on <see cref="Side"/> from their book.</summary>
public readonly record struct OrderDeletedEvent(
    ulong SecurityId,
    string Symbol,
    ulong OrderId,
    BookSide Side,
    DateTime ReceivedUtc);

/// <summary>Mass-delete of one or both sides of the book. Consumers MUST
/// drop every order on the affected side(s) — a follow-up
/// <see cref="BookSnapshotEvent"/> is NOT guaranteed.</summary>
public readonly record struct BookClearedEvent(
    ulong SecurityId,
    string Symbol,
    BookClearSide ClearSide,
    DateTime ReceivedUtc);

/// <summary>
/// Aggregate null-price MOA/MOC market tier for one side. Carries the
/// total quantity and order count; deliberately separate from
/// <see cref="OrderAddedEvent"/> so no sentinel price is needed.
/// </summary>
public readonly record struct MarketTierUpdateEvent(
    ulong SecurityId,
    string Symbol,
    BookSide Side,
    long TotalQty,
    int OrderCount,
    DateTime ReceivedUtc);

// ── MBP / Aggregated price levels ───────────────────────────────────

/// <summary>One aggregated level inside a <see cref="LevelSnapshotEvent"/>.
/// Price already scaled.</summary>
public readonly record struct PriceLevel(
    decimal Price,
    long TotalQty,
    int OrderCount);

/// <summary>
/// Full MBP price-level snapshot. Emitted on initial
/// <see cref="SubscribeFlags.Mbp"/> subscribe. Consumers SHOULD replace
/// any prior per-symbol level map when this fires.
/// </summary>
public sealed class LevelSnapshotEvent
{
    public ulong SecurityId { get; init; }
    public string Symbol { get; init; } = "";
    public IReadOnlyList<PriceLevel> Bids { get; init; } = Array.Empty<PriceLevel>();
    public IReadOnlyList<PriceLevel> Asks { get; init; } = Array.Empty<PriceLevel>();
    public DateTime ReceivedUtc { get; init; }
}

/// <summary>Incremental MBP level (re)write. Conflated server-side per
/// <c>(secId, side, price)</c> — at most one per packet per level.</summary>
public readonly record struct LevelUpdateEvent(
    ulong SecurityId,
    string Symbol,
    BookSide Side,
    decimal Price,
    long TotalQty,
    int OrderCount,
    DateTime ReceivedUtc);

/// <summary>Incremental MBP level removal. Consumers MUST drop the
/// matching <c>(side, price)</c> from their per-symbol map.</summary>
public readonly record struct LevelDeletedEvent(
    ulong SecurityId,
    string Symbol,
    BookSide Side,
    decimal Price,
    DateTime ReceivedUtc);

// ── Per-symbol stale + recovery aggregate ───────────────────────────

/// <summary>Per-symbol stale-status transition. Coalesced server-side
/// (last value within a packet wins). UI consumers SHOULD dim rows
/// when <see cref="IsStale"/> is <c>true</c>.</summary>
public readonly record struct SymbolStaleStatusEvent(
    ulong SecurityId,
    string Symbol,
    bool IsStale,
    DateTime ReceivedUtc);

/// <summary>One per-kind stale breakdown row inside
/// <see cref="RecoveryProgressEvent"/>. <see cref="Kind"/> is the raw
/// <c>SymbolGapKind</c> byte from the server — kept as a byte so that
/// new kinds added server-side are forward-compatible.</summary>
public readonly record struct RecoveryProgressKind(byte Kind, uint StaleCount);

/// <summary>
/// Aggregate recovery progress (broadcast ~250 ms). Stops after the
/// final "all healthy" frame (<see cref="TotalStaleSymbols"/> == 0) so
/// the UI can clear its banner.
/// </summary>
public sealed class RecoveryProgressEvent
{
    public uint TotalSymbols { get; init; }
    public uint TotalStaleSymbols { get; init; }
    public IReadOnlyList<RecoveryProgressKind> StaleByKind { get; init; } = Array.Empty<RecoveryProgressKind>();
    public DateTime ReceivedUtc { get; init; }
}

// ── Candles ─────────────────────────────────────────────────────────

/// <summary>
/// One OHLCV+VWAP candle. Prices already scaled with the SBE 4-decimal
/// exponent; <see cref="TimeNanos"/> is exchange epoch nanoseconds.
/// </summary>
public readonly record struct Candle(
    long TimeNanos,
    decimal Open,
    decimal High,
    decimal Low,
    decimal Close,
    long Volume,
    decimal Avg);

/// <summary>
/// Historical candle batch. The server may send multiple batches per
/// snapshot (e.g. one per resolution); each frame is self-contained.
/// </summary>
public sealed class CandleSnapshotEvent
{
    public ulong SecurityId { get; init; }
    public string Symbol { get; init; } = "";
    /// <summary>Candle resolution as a raw server-side identifier
    /// (e.g. seconds-per-bucket). Kept as <c>int</c> to stay
    /// forward-compatible with new resolutions.</summary>
    public int Resolution { get; init; }
    /// <summary>True on the first batch of a snapshot — consumers SHOULD
    /// replace their cached history for <c>(SecurityId, Resolution)</c>.</summary>
    public bool IsFirst { get; init; }
    /// <summary>True on the final batch of a snapshot.</summary>
    public bool IsLast { get; init; }
    public IReadOnlyList<Candle> Candles { get; init; } = Array.Empty<Candle>();
    public DateTime ReceivedUtc { get; init; }
}

/// <summary>Single candle change (latest candle updated or a new bucket
/// opened). Consumers SHOULD upsert by <see cref="Candle.TimeNanos"/>.</summary>
public readonly record struct CandleUpdateEvent(
    ulong SecurityId,
    string Symbol,
    int Resolution,
    Candle Candle,
    DateTime ReceivedUtc);

// ── Rankings ────────────────────────────────────────────────────────

/// <summary>One row inside a <see cref="RankingsUpdateEvent"/>. <see cref="Value"/>
/// is the raw server-side metric (volume in shares, gainers/losers in
/// basis-point percentage * 100, etc.) — kept as <c>long</c> so the
/// SDK does not have to interpret category-specific scaling.</summary>
public readonly record struct RankingEntry(
    ulong SecurityId,
    string Symbol,
    long Value);

/// <summary>
/// Three top-N rankings tables (most-traded by volume, top gainers,
/// top losers) emitted periodically by the server. Each list is at
/// most 10 entries on the current wire.
/// </summary>
public sealed class RankingsUpdateEvent
{
    public IReadOnlyList<RankingEntry> Volume { get; init; } = Array.Empty<RankingEntry>();
    public IReadOnlyList<RankingEntry> Gainers { get; init; } = Array.Empty<RankingEntry>();
    public IReadOnlyList<RankingEntry> Losers { get; init; } = Array.Empty<RankingEntry>();
    public DateTime ReceivedUtc { get; init; }
}

// ── News ────────────────────────────────────────────────────────────

/// <summary>Optional source classification for a <see cref="NewsEvent"/>.
/// Kept as a raw byte so that new sources added server-side are
/// forward-compatible.</summary>
public enum NewsSource : byte
{
    Unknown = 0,
}

/// <summary>
/// A fully-reassembled news delivery. The SDK buffers
/// <c>NewsBegin/Chunk/End</c> frames internally and raises a single
/// <see cref="MarketDataClient.News"/> event with the joined payloads.
/// </summary>
public sealed class NewsEvent
{
    /// <summary>0 = global news (no instrument scope).</summary>
    public ulong SecurityIdOrZero { get; init; }
    /// <summary>Resolved symbol when <see cref="SecurityIdOrZero"/> matches
    /// a previously-subscribed security; empty otherwise.</summary>
    public string Symbol { get; init; } = "";
    public ulong NewsId { get; init; }
    public byte SourceRaw { get; init; }
    /// <summary>Source classification when known; otherwise
    /// <see cref="NewsSource.Unknown"/>. Use <see cref="SourceRaw"/>
    /// for the wire byte.</summary>
    public NewsSource Source => Enum.IsDefined(typeof(NewsSource), SourceRaw) ? (NewsSource)SourceRaw : NewsSource.Unknown;
    /// <summary>Raw language code from the wire (ISO-style or vendor-specific).</summary>
    public ushort LanguageRaw { get; init; }
    /// <summary>Original publish time in nanoseconds from epoch (exchange time).</summary>
    public long OrigTimeNanos { get; init; }
    public string Headline { get; init; } = "";
    public string Text { get; init; } = "";
    public string Url { get; init; } = "";
    public DateTime ReceivedUtc { get; init; }
}

/// <summary>
/// Static per-security metadata sourced from B3's <c>SecurityDefinition_12</c>
/// (UMDF instrument definition). Pushed over the WebSocket on subscribe (when
/// the client opts in via <see cref="SubscribeFlags.SecurityDefinition"/>) and
/// again on every real definition change — idempotent re-broadcasts upstream
/// are suppressed (the server short-circuits on unchanged
/// <c>SecurityValidityTimestamp</c>), so this event fires only on true deltas.
/// Consumers should treat this as the authoritative source of tick size and
/// lot size for pre-trade guards: no REST refetch needed.
/// </summary>
public sealed class SecurityDefinitionEvent
{
    public ulong SecurityId { get; init; }
    /// <summary>Resolved symbol (embedded directly in the wire frame, not
    /// looked up from the secId→symbol cache).</summary>
    public string Symbol { get; init; } = "";
    public DateTime ReceivedUtc { get; init; }

    /// <summary>
    /// Minimum price increment (tick size). Scaled with the SBE
    /// <c>Fixed8</c> exponent: <c>raw / 100_000_000m</c>. Pre-trade
    /// guards should round/snap order prices to a multiple of this value.
    /// </summary>
    public decimal? MinPriceIncrement { get; init; }

    /// <summary>
    /// Minimum trade volume (lot size), unscaled, from
    /// <c>SecurityDefinition_12.MinTradeVol</c>. Order quantities must be
    /// integer multiples of this value to be accepted by the venue.
    /// </summary>
    public long? MinTradeVolume { get; init; }

    public long? PriceDivisor { get; init; }
    public long? ContractMultiplier { get; init; }
    public long? StrikePrice { get; init; }
    public long? MaturityDate { get; init; }
    public long? PutOrCall { get; init; }
    public long? ExerciseStyle { get; init; }
    public long? SecurityType { get; init; }
    public long? SecuritySubType { get; init; }
    public long? Product { get; init; }
    public long? MarketSegmentID { get; init; }
    public long? TickSizeDenominator { get; init; }

    public string? IsinNumber { get; init; }
    public string? Currency { get; init; }
    public string? Asset { get; init; }
    public string? CfiCode { get; init; }
    public string? SecurityGroup { get; init; }
    public string? SecurityDescription { get; init; }
}

/// <summary>
/// Dynamic per-symbol price band ("túnel de preço") sourced from B3 UMDF
/// <c>PriceBand_22</c>. Pushed on subscribe (when the server has already
/// observed the band) and on every subsequent real change to low/high
/// limits, limit-type, midpoint-type, or trading reference price.
/// Idempotent re-broadcasts are short-circuited upstream, so this event
/// fires only on true deltas. Consumers should treat this as the
/// authoritative pre-trade fat-finger band: no REST refetch / static
/// config needed.
/// </summary>
public sealed class PriceBandEvent
{
    public ulong SecurityId { get; init; }
    /// <summary>Resolved symbol (embedded directly in the wire frame, not
    /// looked up from the secId→symbol cache).</summary>
    public string Symbol { get; init; } = "";
    public DateTime ReceivedUtc { get; init; }

    /// <summary>Lower price-band limit, pre-scaled to a <see cref="decimal"/>
    /// (raw / 10_000). Null when the venue did not specify a lower limit.
    /// Interpretation depends on <see cref="PriceLimitType"/> — see remarks
    /// on that property.</summary>
    public decimal? LowerBand { get; init; }

    /// <summary>Upper price-band limit, pre-scaled to a <see cref="decimal"/>
    /// (raw / 10_000). Null when the venue did not specify an upper limit.
    /// Interpretation depends on <see cref="PriceLimitType"/> — see remarks
    /// on that property.</summary>
    public decimal? UpperBand { get; init; }

    /// <summary>Trading reference price (anchor for percentage / tick bands),
    /// pre-scaled to a <see cref="decimal"/> (raw / 100_000_000 — SBE
    /// <c>Fixed8</c> exponent). Null when not provided.</summary>
    public decimal? TradingReferencePrice { get; init; }

    /// <summary>SBE PriceLimitType (tag 1306) — REQUIRED to interpret
    /// <see cref="LowerBand"/> / <see cref="UpperBand"/>:
    /// 0 = PRICE_UNIT (absolute prices), 1 = TICKS (offsets vs.
    /// <see cref="TradingReferencePrice"/> combined with the security's
    /// tick size), 2 = PERCENTAGE (offsets vs.
    /// <see cref="TradingReferencePrice"/>; e.g. 1.0000 = ±100%).</summary>
    public byte? PriceLimitType { get; init; }

    /// <summary>SBE PriceBandType (tag 1305). Discriminator for the band
    /// classification (hard / soft / continuous / auction).</summary>
    public byte? PriceBandType { get; init; }

    /// <summary>SBE PriceBandMidpointPriceType. Only meaningful when
    /// <see cref="PriceLimitType"/> = PERCENTAGE on rejection / auction
    /// bands; null otherwise.</summary>
    public byte? PriceBandMidpointPriceType { get; init; }

    /// <summary>UMDF <c>MDEntryTimestamp</c> (UTC nanoseconds since epoch)
    /// of the band emission. Lets consumers age out stale bands.</summary>
    public long? AsOfTimestamp { get; init; }

    /// <summary>UMDF <c>RptSeq</c> of the band emission (widened to
    /// <see cref="long"/>). Null when the venue did not provide it on this
    /// emission.</summary>
    public long? RptSeq { get; init; }
}

/// <summary>
/// Indicates which side has the auction imbalance.
/// Derived from the SBE <c>ImbalanceCondition</c> bit-field:
/// bit 8 (0x0100) = MoreBuyers, bit 9 (0x0200) = MoreSellers.
/// When neither bit is set, the imbalance is Balanced.
/// </summary>
public enum ImbalanceSide
{
    /// <summary>Neither more buyers nor more sellers — balanced.</summary>
    Balanced = 0,
    /// <summary>More buyers than sellers (bit 8 set in SBE).</summary>
    MoreBuyers = 1,
    /// <summary>More sellers than buyers (bit 9 set in SBE).</summary>
    MoreSellers = 2,
}

/// <summary>
/// Aggregated auction state from two UMDF templates:
/// <c>AuctionImbalance_19</c> (imbalance qty/side) and
/// <c>SecurityGroupPhase_10</c> (trading status/pre-open time).
/// Pushed on subscribe (when the server has already observed data)
/// and on every subsequent real change. Both sources can independently
/// trigger updates — each bump yields a push with whatever is currently
/// populated. Null fields mean "not yet received from UMDF" or "not
/// applicable to the current auction phase".
/// </summary>
public sealed class AuctionEvent
{
    public ulong SecurityId { get; init; }

    /// <summary>Resolved symbol (embedded directly in the wire frame).</summary>
    public string Symbol { get; init; } = "";

    public DateTime ReceivedUtc { get; init; }

    // ── AuctionImbalance_19 fields ──

    /// <summary>Remaining quantity to match (MDEntrySize). Zero or positive.
    /// Null when no imbalance message was received yet.</summary>
    public long? ImbalanceQty { get; init; }

    /// <summary>Which side has excess quantity: <see cref="ImbalanceSide.MoreBuyers"/>,
    /// <see cref="ImbalanceSide.MoreSellers"/>, or <see cref="ImbalanceSide.Balanced"/>.
    /// Null when no imbalance message was received yet.</summary>
    public ImbalanceSide ImbalanceSide { get; init; }

    /// <summary>Raw SBE <c>ImbalanceCondition</c> bit-field (uint16). Exposed
    /// for advanced consumers who need flags beyond buyer/seller side.</summary>
    public ushort? ImbalanceConditionRaw { get; init; }

    // ── SecurityGroupPhase_10 fields ──

    /// <summary>SBE <c>TradingSessionSubID</c> — the trading phase for this
    /// security/group. Common values: 2=Pre-Open, 4=Call, 17=Continuous.
    /// Null when no phase message was received yet.</summary>
    public int? TradingStatus { get; init; }

    /// <summary>UMDF <c>TradSesOpenTime</c> (UTC epoch nanos) — the scheduled
    /// opening time for Pre-Open phase (tag 342). Only populated when the
    /// status is Pre-Open and B3 publishes it; null otherwise.</summary>
    public long? TradSesOpenTime { get; init; }

    // ── Common envelope fields ──

    /// <summary>Latest <c>MDEntryTimestamp</c> (UTC nanos since epoch) from
    /// either AuctionImbalance or SecurityGroupPhase.</summary>
    public long? AsOfTimestamp { get; init; }

    /// <summary>Latest <c>RptSeq</c> (widened to <see cref="long"/>) from
    /// either source. Null when not provided.</summary>
    public long? RptSeq { get; init; }
}
