namespace B3.Umdf.Book;

/// <summary>
/// A market order with no price (MOA — Market On Auction, MOC — Market On Close).
/// Per B3 BinaryUMDF v2.2.0 spec §12.1, these orders form a virtual price level
/// with priority above all priced levels during reserved / pre-opening / final
/// closing call phases. Tracked separately from the priced <see cref="BookSide"/>
/// so BBO and crossing-detection logic on real prices remain unpolluted.
/// </summary>
public struct MarketOrder
{
    public ulong OrderId;
    public BookSideType Side;
    public long Quantity;
    public uint EnteringFirm;
    public ulong SecurityId;
}
