using B3.Umdf.Book;

namespace B3.Umdf.Book.Tests;

public class MarketDataManagerTests
{
    [Fact]
    public void OnSequenceReset_PreservesInstrumentInfo()
    {
        var manager = new MarketDataManager();
        var info = manager.GetOrCreateInfo(123);

        info.Symbol = "PETR4";
        info.SecurityGroup = "EQT";
        info.LastTradePrice = 123450;
        info.TradeVolume = 42;
        info.BumpVersion();

        manager.OnSequenceReset();

        Assert.True(manager.InstrumentData.TryGetValue(123, out var current));
        Assert.Same(info, current);
        Assert.Equal("PETR4", current.Symbol);
        Assert.Equal("EQT", current.SecurityGroup);
        Assert.Equal(123450, current.LastTradePrice);
        Assert.Equal(42, current.TradeVolume);
    }
}
