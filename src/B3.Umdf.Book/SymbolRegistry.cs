using System.Collections.Frozen;
using B3.Umdf.Feed;
using B3.Umdf.Mbo.Sbe.V16;
using B3.Umdf.Transport;

namespace B3.Umdf.Book;

/// <summary>
/// Bidirectional symbol↔securityId mapping built from SecurityDefinition messages.
/// </summary>
public sealed class SymbolRegistry : IFeedEventHandler
{
    private Dictionary<string, ulong> _mutableBySymbol = new(StringComparer.OrdinalIgnoreCase);
    private Dictionary<ulong, string> _mutableById = new();
    private FrozenDictionary<string, ulong>? _frozenBySymbol;
    private FrozenDictionary<ulong, string>? _frozenById;

    public IReadOnlyDictionary<string, ulong> BySymbol =>
        (IReadOnlyDictionary<string, ulong>?)_frozenBySymbol ?? _mutableBySymbol;

    public IReadOnlyDictionary<ulong, string> ById =>
        (IReadOnlyDictionary<ulong, string>?)_frozenById ?? _mutableById;

    public int Count => _frozenBySymbol?.Count ?? _mutableBySymbol.Count;

    public bool TryResolve(string symbol, out ulong securityId)
    {
        if (_frozenBySymbol is not null)
            return _frozenBySymbol.TryGetValue(symbol, out securityId);
        return _mutableBySymbol.TryGetValue(symbol, out securityId);
    }

    public bool TryGetSymbol(ulong securityId, out string symbol)
    {
        if (_frozenById is not null)
            return _frozenById.TryGetValue(securityId, out symbol!);
        return _mutableById.TryGetValue(securityId, out symbol!);
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
            _mutableBySymbol[symbol] = securityId;
            _mutableById[securityId] = symbol;
        }
    }

    public void OnGapDetected(uint expected, uint received) { }
    public void OnSequenceReset() { }
    public void OnSnapshotStart() { }

    public void OnSnapshotComplete(uint lastRptSeq) { }

    public void OnInstrumentDefinitionsComplete(int instrumentCount)
    {
        _frozenBySymbol = _mutableBySymbol.ToFrozenDictionary(StringComparer.OrdinalIgnoreCase);
        _frozenById = _mutableById.ToFrozenDictionary();
    }
}
