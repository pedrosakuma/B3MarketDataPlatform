using System.Collections.Concurrent;
using System.Collections.Frozen;
using B3.Umdf.Feed;
using B3.Umdf.Mbo.Sbe.V16;
using B3.Umdf.Transport;

namespace B3.Umdf.Book;

/// <summary>
/// Bidirectional symbol↔securityId mapping built from SecurityDefinition messages.
/// Thread-safe: multiple group workers may call OnPacket concurrently.
/// </summary>
public sealed class SymbolRegistry : IFeedEventHandler
{
    private readonly ConcurrentDictionary<string, ulong> _bySymbol = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<ulong, string> _byId = new();
    private volatile FrozenDictionary<string, ulong>? _frozenBySymbol;
    private volatile FrozenDictionary<ulong, string>? _frozenById;
    private int _lastFrozenCount;

    public IReadOnlyDictionary<string, ulong> BySymbol =>
        (IReadOnlyDictionary<string, ulong>?)_frozenBySymbol ?? _bySymbol;

    public IReadOnlyDictionary<ulong, string> ById =>
        (IReadOnlyDictionary<ulong, string>?)_frozenById ?? _byId;

    public int Count => _frozenBySymbol?.Count ?? _bySymbol.Count;

    public bool TryResolve(string symbol, out ulong securityId)
    {
        if (_frozenBySymbol is { } frozen && frozen.TryGetValue(symbol, out securityId))
            return true;
        return _bySymbol.TryGetValue(symbol, out securityId);
    }

    public bool TryGetSymbol(ulong securityId, out string symbol)
    {
        if (_frozenById is { } frozen && frozen.TryGetValue(securityId, out symbol!))
            return true;
        return _byId.TryGetValue(securityId, out symbol!);
    }

    /// <summary>
    /// Promotes live ConcurrentDictionary entries to FrozenDictionary if new symbols
    /// have been added since the last freeze. Lock-free: the volatile reference swap
    /// is atomic. Safe to call from any thread (e.g. a background timer).
    /// </summary>
    /// <returns>True if a new snapshot was created.</returns>
    public bool TryPromote()
    {
        int liveCount = _byId.Count;
        if (liveCount <= _lastFrozenCount)
            return false;

        _frozenBySymbol = _bySymbol.ToFrozenDictionary(StringComparer.OrdinalIgnoreCase);
        _frozenById = _byId.ToFrozenDictionary();
        _lastFrozenCount = liveCount;
        return true;
    }

    public void OnPacket(in UmdfPacket packet, ReadOnlySpan<byte> sbePayload, ushort templateId)
    {
        if (templateId != SecurityDefinition_12Data.MESSAGE_ID) return;
        if (sbePayload.Length < MessageHeader.MESSAGE_SIZE) return;

        var body = sbePayload[MessageHeader.MESSAGE_SIZE..];
        if (!SecurityDefinition_12Data.TryParse(body, out var reader)) return;

        ref readonly var msg = ref reader.Data;
        ulong securityId = (ulong)msg.SecurityID;
        string symbol = msg.Symbol.ToString().Trim();

        if (!string.IsNullOrEmpty(symbol))
        {
            _bySymbol[symbol] = securityId;
            _byId[securityId] = symbol;
        }
    }

    public void OnGapDetected(uint expected, uint received) { }
    public void OnSequenceReset() { }
    public void OnSnapshotStart() { }

    public void OnSnapshotComplete(uint lastRptSeq) { }

    public void OnInstrumentDefinitionsComplete(int instrumentCount)
    {
        TryPromote();
    }
}
