using B3.Umdf.Book;

namespace B3.Umdf.Book.Tests;

public class BookSideTests
{
    [Fact]
    public void AddOrder_IncreasesCount()
    {
        var side = new BookSide(BookSideType.Bid);
        side.AddOrUpdate(new OrderBookEntry { OrderId = 1, Price = 100, Quantity = 10 });

        Assert.Equal(1, side.OrderCount);
    }

    [Fact]
    public void RemoveOrder_DecreasesCount()
    {
        var side = new BookSide(BookSideType.Bid);
        side.AddOrUpdate(new OrderBookEntry { OrderId = 1, Price = 100, Quantity = 10 });
        side.Remove(1);

        Assert.Equal(0, side.OrderCount);
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
    }
}
