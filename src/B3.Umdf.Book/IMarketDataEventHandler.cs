namespace B3.Umdf.Book;

public interface IMarketDataEventHandler
{
    void OnSecurityStatusChanged(ulong securityId, InstrumentInfo info) { }
    void OnMarketDataUpdated(ulong securityId, InstrumentInfo info) { }

    /// <summary>
    /// Fired when a SecurityDefinition arrives for an existing SecurityID
    /// whose canonical identity (Symbol/ISIN/MaturityDate/SecurityType)
    /// differs from the cached value — i.e. the exchange is reusing the
    /// SecurityID for a different instrument (post-delisting recycle, or
    /// rare definition error). Receivers MUST treat this as a per-symbol
    /// epoch reset: clear book, drop stale buffers, reset registry baselines.
    /// </summary>
    void OnInstrumentReplaced(ulong securityId, string? oldSymbol, string newSymbol) { }

    /// <summary>
    /// Fired once per fully-reassembled <c>News_5</c> delivery (P13). The
    /// <paramref name="securityIdOrZero"/> is 0 for global market-wide news
    /// and non-zero for instrument-scoped news.
    /// <para>
    /// CONTRACT: <paramref name="headline"/>, <paramref name="text"/> and
    /// <paramref name="url"/> spans are valid ONLY for the duration of this
    /// call. Implementations MUST consume them synchronously (copy or write to
    /// the wire) before returning. Backing buffers are returned to
    /// <c>ArrayPool</c> immediately after this method returns.
    /// </para>
    /// </summary>
    void OnNews(
        ulong securityIdOrZero,
        ulong newsId,
        byte source,
        ushort language,
        long origTimeNanos,
        ReadOnlySpan<byte> headline,
        ReadOnlySpan<byte> text,
        ReadOnlySpan<byte> url) { }
}
