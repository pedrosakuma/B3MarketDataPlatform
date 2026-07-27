using System.Collections.Concurrent;
using System.Reflection;
using B3.Umdf.Book;
using Microsoft.Extensions.Logging.Abstractions;

namespace B3.Umdf.Server.Tests;

public class InstrumentStatusBootstrapTests
{
    private const ulong SecurityId = 42;
    private const string Symbol = "PETR4";

    [Fact]
    public async Task InfoSubscribe_EmitsCachedAdministrativeStatus()
    {
        using var manager = new SubscriptionManager();
        var group = manager.CreateGroupHandler();
        var registry = new SymbolStateRegistry(NullLogger.Instance);
        var bookManager = new BookManager(
            stateRegistry: registry,
            staleBuffer: new StaleMboBuffer(NullLogger.Instance));
        var marketDataManager = new MarketDataManager(stateRegistry: registry);
        group.SetBookManager(bookManager);

        var info = marketDataManager.GetOrCreateInfo(SecurityId);
        info.Symbol = Symbol;
        info.TradingStatus = 17;
        info.AdministrativeStatus = new InstrumentStatusUpdate(
            PreviousStatus: 17,
            NewStatus: 17,
            TransitionCode: InstrumentStatusDecoder.InstrumentHaltedTransitionCode,
            HaltReasonCode: 4,
            SourceTimestampNanos: 123,
            RptSeq: 9,
            AdministrativeStateCode: InstrumentStatusDecoder.AdministrativeHaltedStateCode,
            TradingSessionId: 1);

        var symbols = new SymbolRegistry();
        RegisterSymbol(symbols, Symbol, SecurityId);
        manager.SetDataSources(
            new[] { bookManager },
            new[] { marketDataManager },
            symbols,
            new[] { group });
        manager.SetReady();

        var socket = new RecordingWebSocket();
        using var session = new ClientSession(socket, channelCapacity: 64);
        manager.RegisterClient(session);
        var writeTask = session.RunWriteLoopAsync();

        manager.HandleSubscribe(
            session.Id, Symbol, DataFlags.Info, bookManager, group,
            bookBatchCutoffSequence: 0);

        await WaitUntil(
            () => socket.HasMessageType(MessageType.InstrumentStatus),
            TimeSpan.FromSeconds(2));
        Assert.Equal(1, socket.CountByType(MessageType.InstrumentStatus));
        var frame = socket.LastFrame(MessageType.InstrumentStatus);
        Assert.NotNull(frame);
        Assert.Equal(
            WireProtocol.InstrumentStatusDeliverySnapshot,
            frame![^3]);
        Assert.Equal(InstrumentStatusDecoder.AdministrativeHaltedStateCode, frame[^2]);
        Assert.Equal(1, frame[^1]);

        session.Dispose();
        await writeTask.WaitAsync(TimeSpan.FromSeconds(2));
    }

    private static void RegisterSymbol(
        SymbolRegistry registry,
        string symbol,
        ulong securityId)
    {
        var bySymbol = (ConcurrentDictionary<string, ulong>)typeof(SymbolRegistry)
            .GetField("_bySymbol", BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetValue(registry)!;
        var byId = (ConcurrentDictionary<ulong, string>)typeof(SymbolRegistry)
            .GetField("_byId", BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetValue(registry)!;
        bySymbol[symbol] = securityId;
        byId[securityId] = symbol;
    }

    private static async Task WaitUntil(Func<bool> predicate, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (predicate()) return;
            await Task.Delay(20);
        }
        Assert.True(predicate(), "Timed out waiting for instrument status snapshot.");
    }
}
