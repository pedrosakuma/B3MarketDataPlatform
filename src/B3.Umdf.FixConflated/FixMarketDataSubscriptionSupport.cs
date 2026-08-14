using System.Collections.Concurrent;
using System.Globalization;
using B3.Umdf.Book;

namespace B3.Umdf.FixConflated;

public sealed class FixSessionSubscriptionState
{
    private readonly object _gate = new();
    private HashSet<ulong> _subscribedSecurityIds = [];
    private bool _hasExplicitRequest;

    public bool HasExplicitRequest
    {
        get
        {
            lock (_gate)
                return _hasExplicitRequest;
        }
    }

    public bool IsSubscribedTo(ulong securityId)
    {
        lock (_gate)
        {
            if (!_hasExplicitRequest)
                return true;

            return _subscribedSecurityIds.Contains(securityId);
        }
    }

    public void Apply(FixMarketDataRequestAction action)
    {
        lock (_gate)
        {
            _hasExplicitRequest = true;
            switch (action.SubscriptionRequestType)
            {
                case '0':
                    break;
                case '1':
                    foreach (ulong securityId in action.SecurityIds)
                        _subscribedSecurityIds.Add(securityId);
                    break;
                case '2':
                    foreach (ulong securityId in action.SecurityIds)
                        _subscribedSecurityIds.Remove(securityId);
                    break;
                default:
                    throw new InvalidOperationException($"Unsupported SubscriptionRequestType {action.SubscriptionRequestType}.");
            }
        }
    }
}

public readonly record struct FixMarketDataRequestAction(
    string MdReqId,
    char SubscriptionRequestType,
    IReadOnlyList<ulong> SecurityIds);

public readonly record struct FixApplicationDispatch(FixMessage Message, ulong? SecurityId = null);

public static class FixApplicationMessageClassifier
{
    public static ulong? TryGetSecurityId(FixMessage message)
    {
        ArgumentNullException.ThrowIfNull(message);
        if (!message.TryGetString(FixTags.MsgType, out string? msgType) || string.IsNullOrEmpty(msgType))
            return null;

        return msgType switch
        {
            FixMsgTypes.MarketDataSnapshotFullRefresh => TryParseSecurityId(message, FixTags.SecurityId),
            FixMsgTypes.MarketDataIncrementalRefresh => TryParseSecurityId(message, FixTags.SecurityId),
            _ => null,
        };
    }

    private static ulong? TryParseSecurityId(FixMessage message, int tag)
    {
        if (!message.TryGetString(tag, out string? raw) ||
            !ulong.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out ulong securityId))
            return null;

        return securityId;
    }
}

public sealed class FixInitialSnapshotProvider
{
    private readonly IReadOnlyList<BookManager> _bookManagers;
    private readonly IFixMarketDataInstrumentResolver _instrumentResolver;
    private readonly IFixClock _clock;

    public FixInitialSnapshotProvider(
        IReadOnlyList<BookManager> bookManagers,
        IFixMarketDataInstrumentResolver instrumentResolver,
        IFixClock? clock = null)
    {
        _bookManagers = bookManagers ?? throw new ArgumentNullException(nameof(bookManagers));
        _instrumentResolver = instrumentResolver ?? throw new ArgumentNullException(nameof(instrumentResolver));
        _clock = clock ?? SystemFixClock.Instance;
    }

    public IEnumerable<FixMessage> CreateMessages()
        => CreateMessages(static _ => true, static securityId => $"SNAP-{securityId.ToString(CultureInfo.InvariantCulture)}");

    public IEnumerable<FixMessage> CreateMessages(IEnumerable<ulong> securityIds, string mdReqId)
    {
        ArgumentNullException.ThrowIfNull(securityIds);
        ArgumentException.ThrowIfNullOrEmpty(mdReqId);
        var requested = new HashSet<ulong>(securityIds);
        return CreateMessages(requested.Contains, _ => mdReqId);
    }

    private IEnumerable<FixMessage> CreateMessages(Func<ulong, bool> shouldInclude, Func<ulong, string> mdReqIdFactory)
    {
        var emitted = new HashSet<ulong>();
        DateTimeOffset snapshotTime = _clock.UtcNow;

        foreach (BookManager manager in _bookManagers)
        {
            foreach (KeyValuePair<ulong, OrderBook> entry in manager.Books)
            {
                if (!shouldInclude(entry.Key) || !emitted.Add(entry.Key))
                    continue;
                if (!_instrumentResolver.TryResolve(entry.Key, out FixMarketDataInstrument instrument))
                    continue;

                yield return FixSnapshotMessageBuilder.Build(
                    new FixMarketDataSnapshotRequest(mdReqIdFactory(entry.Key), instrument),
                    entry.Value,
                    snapshotTime);
            }
        }
    }
}

public sealed class FixMarketDataRequestHandler
{
    private static readonly HashSet<string> SupportedSecurityIdSources = ["8"];
    private static readonly HashSet<string> SupportedSecurityExchanges = ["BVMF"];

    private readonly FixInitialSnapshotProvider _snapshotProvider;
    private readonly IFixMarketDataInstrumentResolver _instrumentResolver;
    private readonly ConcurrentDictionary<ulong, byte> _knownSecurityIds = new();

    public FixMarketDataRequestHandler(FixInitialSnapshotProvider snapshotProvider, IFixMarketDataInstrumentResolver instrumentResolver)
    {
        _snapshotProvider = snapshotProvider ?? throw new ArgumentNullException(nameof(snapshotProvider));
        _instrumentResolver = instrumentResolver ?? throw new ArgumentNullException(nameof(instrumentResolver));
    }

    public FixMarketDataRequestResult Handle(FixMessage message, FixSessionSubscriptionState subscriptionState)
    {
        ArgumentNullException.ThrowIfNull(message);
        ArgumentNullException.ThrowIfNull(subscriptionState);

        if (!TryParse(message, out FixMarketDataRequestAction action, out FixMessage? reject))
            return new FixMarketDataRequestResult(reject is null ? [] : [reject]);

        subscriptionState.Apply(action);
        IReadOnlyList<FixMessage> snapshots = action.SubscriptionRequestType is '0' or '1'
            ? _snapshotProvider.CreateMessages(action.SecurityIds, action.MdReqId).ToArray()
            : [];
        return new FixMarketDataRequestResult(snapshots);
    }

    private bool TryParse(FixMessage message, out FixMarketDataRequestAction action, out FixMessage? reject)
    {
        action = default;
        reject = null;

        if (!TryGetRequiredString(message, FixTags.MDReqId, out string? mdReqId))
        {
            reject = CreateReject("UNKNOWN", '1', "Missing MDReqID.");
            return false;
        }

        if (!TryGetRequiredString(message, FixApplicationTags.SubscriptionRequestType, out string? subscriptionRaw) || subscriptionRaw!.Length != 1)
        {
            reject = CreateReject(mdReqId!, '1', "Missing or invalid SubscriptionRequestType.");
            return false;
        }

        char subscriptionRequestType = subscriptionRaw[0];
        if (subscriptionRequestType is not ('0' or '1' or '2'))
        {
            reject = CreateReject(mdReqId!, '1', $"Unsupported SubscriptionRequestType {subscriptionRequestType}.");
            return false;
        }

        List<ulong> securityIds = [];
        bool insideRelatedSym = false;
        ulong? currentSecurityId = null;
        string? currentSecurityIdSource = null;
        string? currentSecurityExchange = null;

        foreach (FixField field in message.Fields)
        {
            if (field.Tag == FixApplicationTags.NoRelatedSym)
            {
                insideRelatedSym = true;
                continue;
            }

            if (!insideRelatedSym || field.Tag == FixTags.CheckSum)
                continue;

            switch (field.Tag)
            {
                case FixTags.SecurityId:
                    if (currentSecurityId is not null)
                    {
                        if (!TryFinalizeGroup(mdReqId!, currentSecurityId, currentSecurityIdSource, currentSecurityExchange, securityIds, out reject))
                            return false;
                        currentSecurityIdSource = null;
                        currentSecurityExchange = null;
                    }

                    if (!ulong.TryParse(field.Value, NumberStyles.Integer, CultureInfo.InvariantCulture, out ulong parsedSecurityId))
                    {
                        reject = CreateReject(mdReqId!, '1', $"Invalid SecurityID '{field.Value}'.");
                        return false;
                    }

                    currentSecurityId = parsedSecurityId;
                    break;
                case FixTags.SecurityIdSource:
                    currentSecurityIdSource = field.Value;
                    break;
                case FixTags.SecurityExchange:
                    currentSecurityExchange = field.Value;
                    break;
            }
        }

        if (insideRelatedSym)
        {
            if (!TryFinalizeGroup(mdReqId!, currentSecurityId, currentSecurityIdSource, currentSecurityExchange, securityIds, out reject))
                return false;
        }

        if (securityIds.Count == 0)
        {
            reject = CreateReject(mdReqId!, '1', "MarketDataRequest must include at least one NoRelatedSym SecurityID.");
            return false;
        }

        action = new FixMarketDataRequestAction(mdReqId!, subscriptionRequestType, securityIds);
        return true;
    }

    private bool TryFinalizeGroup(
        string mdReqId,
        ulong? securityId,
        string? securityIdSource,
        string? securityExchange,
        List<ulong> securityIds,
        out FixMessage? reject)
    {
        reject = null;
        if (securityId is null || string.IsNullOrEmpty(securityIdSource) || string.IsNullOrEmpty(securityExchange))
        {
            reject = CreateReject(mdReqId, '1', "Each NoRelatedSym entry must include SecurityID, SecurityIDSource, and SecurityExchange.");
            return false;
        }

        if (!SupportedSecurityIdSources.Contains(securityIdSource!))
        {
            reject = CreateReject(mdReqId, '1', $"Unsupported SecurityIDSource {securityIdSource}. Supported value is 8.");
            return false;
        }

        if (!SupportedSecurityExchanges.Contains(securityExchange!))
        {
            reject = CreateReject(mdReqId, '1', $"Unsupported SecurityExchange {securityExchange}. Supported value is BVMF.");
            return false;
        }

        ulong resolvedSecurityId = securityId.Value;
        if (!_knownSecurityIds.ContainsKey(resolvedSecurityId))
        {
            if (!_instrumentResolver.TryResolve(resolvedSecurityId, out _))
            {
                reject = CreateReject(mdReqId, '0', $"Unknown SecurityID {resolvedSecurityId.ToString(CultureInfo.InvariantCulture)}.");
                return false;
            }

            _knownSecurityIds.TryAdd(resolvedSecurityId, 0);
        }

        securityIds.Add(resolvedSecurityId);
        return true;
    }

    private static FixMessage CreateReject(string mdReqId, char reason, string text)
    {
        var reject = new FixMessage();
        reject.Add(FixTags.MsgType, FixMsgTypes.MarketDataRequestReject);
        reject.Add(FixTags.MDReqId, mdReqId);
        reject.Add(FixApplicationTags.MdReqRejReason, reason.ToString(CultureInfo.InvariantCulture));
        reject.Add(FixTags.Text, text);
        return reject;
    }

    private static bool TryGetRequiredString(FixMessage message, int tag, out string? value)
    {
        if (message.TryGetString(tag, out value) && !string.IsNullOrEmpty(value))
            return true;

        value = null;
        return false;
    }
}

public readonly record struct FixMarketDataRequestResult(IReadOnlyList<FixMessage> OutboundMessages);
