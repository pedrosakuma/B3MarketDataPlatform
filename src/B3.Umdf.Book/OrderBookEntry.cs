namespace B3.Umdf.Book;

/// <summary>
/// Order book entry stored by value in <see cref="BookSide"/> dictionaries and lists.
/// Declared as a mutable struct so that order adds do not allocate on the heap; mutations
/// must be performed via ref accessors (e.g. <see cref="System.Runtime.InteropServices.CollectionsMarshal"/>)
/// to avoid editing detached copies.
/// </summary>
public struct OrderBookEntry
{
    public ulong OrderId;
    public long Price;
    public long Quantity;
    public uint EnteringFirm;
    public ulong SecurityId;
    public BookSideType Side;

    /// <summary>
    /// Index within the price level's order list. Used for O(1) swap-remove.
    /// Managed internally by BookSide.
    /// </summary>
    internal int PriceLevelIndex;
}

public enum BookSideType : byte
{
    Bid,
    Ask
}
