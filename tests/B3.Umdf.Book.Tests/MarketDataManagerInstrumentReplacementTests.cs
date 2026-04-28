using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using B3.Umdf.Book;
using B3.Umdf.Mbo.Sbe.V16;
using B3.Umdf.Transport;
using Microsoft.Extensions.Logging.Abstractions;

namespace B3.Umdf.Book.Tests;

/// <summary>
/// Coverage for SecurityID reuse / re-listing scenarios on the
/// <see cref="MarketDataManager.HandleSecurityDefinition"/> path.
///
/// Three behaviors are pinned:
///   1. <b>Identity change detection</b> — when a SecDef arrives for an
///      existing SecurityID with a different Symbol/ISIN/MaturityDate/SecurityType,
///      the manager treats it as instrument replacement: emits
///      <see cref="IMarketDataEventHandler.OnInstrumentReplaced"/>, increments
///      <c>InstrumentIdentityChanged</c>, and the receiver is expected to
///      clear book + reset registry baselines.
///   2. <b>Timestamp regression guard</b> — a SecDef whose
///      <c>SecurityValidityTimestamp</c> is older than the cached value is
///      dropped (<c>SecurityDefinitionsTimestampRegressed</c> increments).
///   3. <b>Same-identity rebroadcast</b> — different timestamp, same identity
///      tuple → no replacement event; just metadata update.
/// </summary>
public class MarketDataManagerInstrumentReplacementTests
{
    [Fact]
    public void IdentityChange_DifferentSymbol_FiresOnInstrumentReplacedAndClearsBook()
    {
        const ulong sec = 1001;
        var registry = new SymbolStateRegistry(NullLogger.Instance);
        var sink = new ReplacementSink();
        var mdm = new MarketDataManager(eventHandler: sink, stateRegistry: registry);

        mdm.OnPacket(in EmptyPacket,
            BuildSecDefPacket(sec, validityTs: 1000, symbol: "PETR4", isin: "BRPETRACNOR9",
                maturityDate: 0, securityType: SecurityType.OPT),
            SecurityDefinition_12Data.MESSAGE_ID);
        Assert.Empty(sink.ReplacementEvents);
        Assert.Equal(0L, mdm.InstrumentIdentityChanged);

        // Same timestamp re-broadcast — short-circuited; no replacement.
        mdm.OnPacket(in EmptyPacket,
            BuildSecDefPacket(sec, validityTs: 1000, symbol: "PETR4", isin: "BRPETRACNOR9",
                maturityDate: 0, securityType: SecurityType.OPT),
            SecurityDefinition_12Data.MESSAGE_ID);
        Assert.Empty(sink.ReplacementEvents);

        // Identity changed (different Symbol + ISIN) under a NEW timestamp.
        mdm.OnPacket(in EmptyPacket,
            BuildSecDefPacket(sec, validityTs: 2000, symbol: "VALE3", isin: "BRVALEACNOR0",
                maturityDate: 0, securityType: SecurityType.OPT),
            SecurityDefinition_12Data.MESSAGE_ID);

        Assert.Equal(1L, mdm.InstrumentIdentityChanged);
        var ev = Assert.Single(sink.ReplacementEvents);
        Assert.Equal(sec, ev.SecurityId);
        Assert.Equal("PETR4", ev.OldSymbol);
        Assert.Equal("VALE3", ev.NewSymbol);

        // Cached info reflects the new identity; old fields are gone.
        Assert.True(mdm.InstrumentData.TryGetValue(sec, out var info));
        Assert.Equal("VALE3", info!.Symbol);
        Assert.Equal("BRVALEACNOR0", info.IsinNumber);
    }

    [Fact]
    public void IdentityChange_DifferentMaturityDate_FiresOnInstrumentReplaced()
    {
        const ulong sec = 1002;
        var registry = new SymbolStateRegistry(NullLogger.Instance);
        var sink = new ReplacementSink();
        var mdm = new MarketDataManager(eventHandler: sink, stateRegistry: registry);

        mdm.OnPacket(in EmptyPacket,
            BuildSecDefPacket(sec, validityTs: 1000, symbol: "WINQ25", isin: "",
                maturityDate: 20250819, securityType: SecurityType.OPT),
            SecurityDefinition_12Data.MESSAGE_ID);
        mdm.OnPacket(in EmptyPacket,
            BuildSecDefPacket(sec, validityTs: 2000, symbol: "WINQ25", isin: "",
                maturityDate: 20260819, securityType: SecurityType.OPT),
            SecurityDefinition_12Data.MESSAGE_ID);

        Assert.Equal(1L, mdm.InstrumentIdentityChanged);
    }

    [Fact]
    public void TimestampRegression_DropsSecDef_AndIncrementsCounter()
    {
        const ulong sec = 1003;
        var registry = new SymbolStateRegistry(NullLogger.Instance);
        var sink = new ReplacementSink();
        var mdm = new MarketDataManager(eventHandler: sink, stateRegistry: registry);

        mdm.OnPacket(in EmptyPacket,
            BuildSecDefPacket(sec, validityTs: 5000, symbol: "PETR4", isin: "BRPETRACNOR9",
                maturityDate: 0, securityType: SecurityType.OPT),
            SecurityDefinition_12Data.MESSAGE_ID);

        // Out-of-order older SecDef (e.g. late arrival on InstrDef channel) —
        // must be dropped to avoid silently rolling metadata back.
        mdm.OnPacket(in EmptyPacket,
            BuildSecDefPacket(sec, validityTs: 4000, symbol: "STALE9", isin: "ZZSTALE00000",
                maturityDate: 19990101, securityType: SecurityType.FUT),
            SecurityDefinition_12Data.MESSAGE_ID);

        Assert.Equal(1L, mdm.SecurityDefinitionsTimestampRegressed);
        Assert.Equal(0L, mdm.InstrumentIdentityChanged);

        Assert.True(mdm.InstrumentData.TryGetValue(sec, out var info));
        Assert.Equal("PETR4", info!.Symbol); // unchanged
        Assert.Equal((ulong)5000, info.LastSecurityValidityTimestamp);
        Assert.Empty(sink.ReplacementEvents);
    }

    [Fact]
    public void SameIdentity_DifferentTimestamp_DoesNotFireReplacement()
    {
        // Corporate action / contract adjustment scenario: timestamp bumps
        // (so the early-out is bypassed) but the canonical identity stays
        // the same. The path must NOT be misclassified as reuse.
        const ulong sec = 1004;
        var registry = new SymbolStateRegistry(NullLogger.Instance);
        var sink = new ReplacementSink();
        var mdm = new MarketDataManager(eventHandler: sink, stateRegistry: registry);

        mdm.OnPacket(in EmptyPacket,
            BuildSecDefPacket(sec, validityTs: 1000, symbol: "PETR4", isin: "BRPETRACNOR9",
                maturityDate: 0, securityType: SecurityType.OPT),
            SecurityDefinition_12Data.MESSAGE_ID);
        mdm.OnPacket(in EmptyPacket,
            BuildSecDefPacket(sec, validityTs: 2000, symbol: "PETR4", isin: "BRPETRACNOR9",
                maturityDate: 0, securityType: SecurityType.OPT),
            SecurityDefinition_12Data.MESSAGE_ID);

        Assert.Equal(0L, mdm.InstrumentIdentityChanged);
        Assert.Empty(sink.ReplacementEvents);
    }

    [Fact]
    public void FirstSecDef_NeverFiresReplacement()
    {
        // No prior identity — the first SecDef for a SecurityID is always a
        // creation, never a replacement. Counter and event must stay clean.
        const ulong sec = 1005;
        var registry = new SymbolStateRegistry(NullLogger.Instance);
        var sink = new ReplacementSink();
        var mdm = new MarketDataManager(eventHandler: sink, stateRegistry: registry);

        mdm.OnPacket(in EmptyPacket,
            BuildSecDefPacket(sec, validityTs: 1000, symbol: "WHATEVER", isin: "BRX",
                maturityDate: 20300101, securityType: SecurityType.FUT),
            SecurityDefinition_12Data.MESSAGE_ID);

        Assert.Equal(0L, mdm.InstrumentIdentityChanged);
        Assert.Empty(sink.ReplacementEvents);
    }

    private sealed class ReplacementSink : IMarketDataEventHandler
    {
        public readonly List<(ulong SecurityId, string? OldSymbol, string NewSymbol)> ReplacementEvents = new();
        public void OnInstrumentReplaced(ulong securityId, string? oldSymbol, string newSymbol)
            => ReplacementEvents.Add((securityId, oldSymbol, newSymbol));
    }

    private static readonly UmdfPacket EmptyPacket = new()
    {
        Data = ReadOnlyMemory<byte>.Empty,
        Channel = ChannelType.InstrumentDefinition,
        ChannelGroup = 1,
        ReceivedTimestampTicks = 0,
    };

    /// <summary>
    /// Builds a SecurityDefinition_12 SBE message with custom Symbol/ISIN/
    /// MaturityDate/SecurityType. Patches the TryEncode-generated wire bytes
    /// at known FieldOffset positions (Symbol@16, SecurityType@37,
    /// MaturityDate@140, IsinNumber@164) — the generated struct exposes
    /// these as InlineArray composites without convenient setters.
    /// </summary>
    private static byte[] BuildSecDefPacket(ulong securityId, long validityTs,
        string symbol, string isin, int maturityDate, SecurityType securityType)
    {
        const int sbeHeaderSize = 8;
        UTCTimestampSeconds ts = Unsafe.As<long, UTCTimestampSeconds>(ref validityTs);
        var payload = new SecurityDefinition_12Data
        {
            SecurityID = (SecurityID)securityId,
            SecurityValidityTimestamp = ts,
        };

        var buf = new byte[1024];
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(0), (ushort)SecurityDefinition_12Data.MESSAGE_SIZE);
        BinaryPrimitives.WriteUInt16LittleEndian(buf.AsSpan(2), SecurityDefinition_12Data.MESSAGE_ID);

        bool ok = SecurityDefinition_12Data.TryEncode(
            payload,
            buf.AsSpan(sbeHeaderSize),
            ReadOnlySpan<SecurityDefinition_12Data.NoUnderlyingsData>.Empty,
            ReadOnlySpan<SecurityDefinition_12Data.NoLegsData>.Empty,
            ReadOnlySpan<SecurityDefinition_12Data.NoInstrAttribsData>.Empty,
            ReadOnlySpan<byte>.Empty,
            out int bytesWritten);
        Assert.True(ok, "TryEncode failed");

        // Patch identity fields directly into the encoded body. Offsets are
        // from FieldOffset attributes in SecurityDefinition_12.cs (within the
        // body, not counting the 8-byte SBE header).
        var body = buf.AsSpan(sbeHeaderSize);
        if (!string.IsNullOrEmpty(symbol))
        {
            var src = Encoding.Latin1.GetBytes(symbol);
            body.Slice(16, 20).Clear();
            src.AsSpan(0, Math.Min(src.Length, 20)).CopyTo(body.Slice(16));
        }
        body[37] = (byte)securityType;
        BinaryPrimitives.WriteInt32LittleEndian(body.Slice(140, 4), maturityDate);
        if (!string.IsNullOrEmpty(isin))
        {
            var src = Encoding.Latin1.GetBytes(isin);
            body.Slice(164, 12).Clear();
            src.AsSpan(0, Math.Min(src.Length, 12)).CopyTo(body.Slice(164));
        }

        var result = new byte[sbeHeaderSize + bytesWritten];
        buf.AsSpan(0, result.Length).CopyTo(result);
        return result;
    }
}
