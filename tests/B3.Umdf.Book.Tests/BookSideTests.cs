using B3.Umdf.Book;

namespace B3.Umdf.Book.Tests;

public class BookSideTests
{
    private static void AssertValid(BookSide side)
    {
        var errors = side.Validate();
        Assert.Empty(errors);
    }

    [Fact]
    public void AddOrder_IncreasesCount()
    {
        var side = new BookSide(BookSideType.Bid);
        side.AddOrUpdate(new OrderBookEntry { OrderId = 1, Price = 100, Quantity = 10 });

        Assert.Equal(1, side.OrderCount);
        AssertValid(side);
    }

    [Fact]
    public void RemoveOrder_DecreasesCount()
    {
        var side = new BookSide(BookSideType.Bid);
        side.AddOrUpdate(new OrderBookEntry { OrderId = 1, Price = 100, Quantity = 10 });
        side.Remove(1);

        Assert.Equal(0, side.OrderCount);
        AssertValid(side);
    }

    [Fact]
    public void BestBidPrice_IsHighest()
    {
        var side = new BookSide(BookSideType.Bid);
        side.AddOrUpdate(new OrderBookEntry { OrderId = 1, Price = 100, Quantity = 10 });
        side.AddOrUpdate(new OrderBookEntry { OrderId = 2, Price = 200, Quantity = 20 });
        side.AddOrUpdate(new OrderBookEntry { OrderId = 3, Price = 150, Quantity = 15 });

        var best = side.BestPrice();
        Assert.NotNull(best);
        Assert.Equal(200, best.Value.Price);
        Assert.Equal(20, best.Value.TotalQty);
        AssertValid(side);
    }

    [Fact]
    public void BestAskPrice_IsLowest()
    {
        var side = new BookSide(BookSideType.Ask);
        side.AddOrUpdate(new OrderBookEntry { OrderId = 1, Price = 100, Quantity = 10 });
        side.AddOrUpdate(new OrderBookEntry { OrderId = 2, Price = 200, Quantity = 20 });
        side.AddOrUpdate(new OrderBookEntry { OrderId = 3, Price = 150, Quantity = 15 });

        var best = side.BestPrice();
        Assert.NotNull(best);
        Assert.Equal(100, best.Value.Price);
        Assert.Equal(10, best.Value.TotalQty);
        AssertValid(side);
    }

    [Fact]
    public void UpdateOrder_MovesToNewPriceLevel()
    {
        var side = new BookSide(BookSideType.Bid);
        side.AddOrUpdate(new OrderBookEntry { OrderId = 1, Price = 100, Quantity = 10 });
        side.AddOrUpdate(new OrderBookEntry { OrderId = 1, Price = 200, Quantity = 20 });

        Assert.Equal(1, side.OrderCount);
        var best = side.BestPrice();
        Assert.Equal(200, best!.Value.Price);
        AssertValid(side);
    }

    [Fact]
    public void Clear_RemovesAllOrders()
    {
        var side = new BookSide(BookSideType.Bid);
        side.AddOrUpdate(new OrderBookEntry { OrderId = 1, Price = 100, Quantity = 10 });
        side.AddOrUpdate(new OrderBookEntry { OrderId = 2, Price = 200, Quantity = 20 });
        side.Clear();

        Assert.Equal(0, side.OrderCount);
        Assert.Null(side.BestPrice());
        AssertValid(side);
    }

    [Fact]
    public void MultiplOrdersAtSamePrice_AggregatedInBestPrice()
    {
        var side = new BookSide(BookSideType.Bid);
        side.AddOrUpdate(new OrderBookEntry { OrderId = 1, Price = 100, Quantity = 10 });
        side.AddOrUpdate(new OrderBookEntry { OrderId = 2, Price = 100, Quantity = 20 });

        var best = side.BestPrice();
        Assert.Equal(100, best!.Value.Price);
        Assert.Equal(30, best.Value.TotalQty);
        AssertValid(side);
    }

    [Fact]
    public void RemoveNonExistentOrder_ReturnsFalse_RemainsValid()
    {
        var side = new BookSide(BookSideType.Bid);
        side.AddOrUpdate(new OrderBookEntry { OrderId = 1, Price = 100, Quantity = 10 });

        Assert.False(side.Remove(999));
        Assert.Equal(1, side.OrderCount);
        AssertValid(side);
    }

    [Fact]
    public void UpdateOrder_SamePrice_ReplacesEntry()
    {
        var side = new BookSide(BookSideType.Ask);
        side.AddOrUpdate(new OrderBookEntry { OrderId = 1, Price = 100, Quantity = 10 });
        side.AddOrUpdate(new OrderBookEntry { OrderId = 1, Price = 100, Quantity = 50 });

        Assert.Equal(1, side.OrderCount);
        Assert.Single(side.PriceLevels[100]);
        Assert.Equal(50, side.PriceLevels[100][0].Quantity);
        AssertValid(side);
    }

    [Fact]
    public void AddRemoveManyOrders_RemainsValid()
    {
        var side = new BookSide(BookSideType.Bid);
        for (ulong i = 1; i <= 100; i++)
            side.AddOrUpdate(new OrderBookEntry { OrderId = i, Price = (long)(i % 10) * 100, Quantity = (long)i });

        AssertValid(side);
        Assert.Equal(100, side.OrderCount);

        // Remove even orders
        for (ulong i = 2; i <= 100; i += 2)
            side.Remove(i);

        AssertValid(side);
        Assert.Equal(50, side.OrderCount);

        // Update remaining orders to new prices
        for (ulong i = 1; i <= 99; i += 2)
            side.AddOrUpdate(new OrderBookEntry { OrderId = i, Price = 9999, Quantity = 1 });

        AssertValid(side);
        Assert.Equal(50, side.OrderCount);
        Assert.Single(side.PriceLevels); // all at same price
    }

    [Fact]
    public void RemoveAllOrders_LeavesNoPriceLevels()
    {
        var side = new BookSide(BookSideType.Ask);
        side.AddOrUpdate(new OrderBookEntry { OrderId = 1, Price = 100, Quantity = 10 });
        side.AddOrUpdate(new OrderBookEntry { OrderId = 2, Price = 100, Quantity = 20 });
        side.AddOrUpdate(new OrderBookEntry { OrderId = 3, Price = 200, Quantity = 30 });

        side.Remove(1);
        AssertValid(side);
        side.Remove(2);
        AssertValid(side);
        side.Remove(3);
        AssertValid(side);

        Assert.Empty(side.PriceLevels);
        Assert.Equal(0, side.OrderCount);
    }
}

public class OrderBookValidationTests
{
    private static void AssertValid(OrderBook book)
    {
        var errors = book.Validate();
        Assert.Empty(errors);
    }

    [Fact]
    public void EmptyBook_IsValid()
    {
        var book = new OrderBook(1);
        AssertValid(book);
    }

    [Fact]
    public void BidsAndAsks_IndependentlyValid()
    {
        var book = new OrderBook(1);
        book.Bids.AddOrUpdate(new OrderBookEntry { OrderId = 1, Price = 100, Quantity = 10, Side = BookSideType.Bid });
        book.Asks.AddOrUpdate(new OrderBookEntry { OrderId = 2, Price = 200, Quantity = 20, Side = BookSideType.Ask });

        AssertValid(book);
    }

    [Fact]
    public void ClearBook_IsValid()
    {
        var book = new OrderBook(1);
        book.Bids.AddOrUpdate(new OrderBookEntry { OrderId = 1, Price = 100, Quantity = 10 });
        book.Asks.AddOrUpdate(new OrderBookEntry { OrderId = 2, Price = 200, Quantity = 20 });
        book.Clear();

        AssertValid(book);
        Assert.Equal(0, book.Bids.OrderCount);
        Assert.Equal(0, book.Asks.OrderCount);
    }

    [Fact]
    public void FullLifecycle_RemainsValid()
    {
        var book = new OrderBook(42);

        // Add bids
        book.Bids.AddOrUpdate(new OrderBookEntry { OrderId = 1, Price = 1000, Quantity = 100, Side = BookSideType.Bid });
        book.Bids.AddOrUpdate(new OrderBookEntry { OrderId = 2, Price = 900, Quantity = 200, Side = BookSideType.Bid });
        book.Bids.AddOrUpdate(new OrderBookEntry { OrderId = 3, Price = 1000, Quantity = 50, Side = BookSideType.Bid });
        AssertValid(book);

        // Add asks
        book.Asks.AddOrUpdate(new OrderBookEntry { OrderId = 10, Price = 1100, Quantity = 75, Side = BookSideType.Ask });
        book.Asks.AddOrUpdate(new OrderBookEntry { OrderId = 11, Price = 1200, Quantity = 150, Side = BookSideType.Ask });
        AssertValid(book);

        // Update a bid (price change)
        book.Bids.AddOrUpdate(new OrderBookEntry { OrderId = 1, Price = 950, Quantity = 80, Side = BookSideType.Bid });
        AssertValid(book);

        // Delete orders
        book.Bids.Remove(2);
        book.Asks.Remove(10);
        AssertValid(book);

        Assert.Equal(2, book.Bids.OrderCount);
        Assert.Equal(1, book.Asks.OrderCount);
    }
}
