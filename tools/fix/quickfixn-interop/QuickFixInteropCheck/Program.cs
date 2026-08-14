using System;
using QuickFix;

namespace QuickFixInteropCheck;

/// <summary>
/// Minimal QuickFIX/n application callback set. Its only job is to log every
/// admin/application message it sends and receives so a human (or a script
/// scanning its stdout) can confirm the sandbox's FIX 4.4 session and
/// application messages are genuinely parseable by an independent,
/// third-party FIX engine — not just by this repo's own
/// tools/fix/fix-validate.mjs.
/// </summary>
public sealed class LoggingApplication : IApplication
{
    public void OnCreate(SessionID sessionID) => Console.WriteLine($"[app] OnCreate {sessionID}");

    public void OnLogon(SessionID sessionID) => Console.WriteLine($"[app] OnLogon {sessionID}");

    public void OnLogout(SessionID sessionID) => Console.WriteLine($"[app] OnLogout {sessionID}");

    public void ToAdmin(Message message, SessionID sessionID)
        => Console.WriteLine($"[app] ToAdmin: {Readable(message)}");

    public void FromAdmin(Message message, SessionID sessionID)
        => Console.WriteLine($"[app] FromAdmin: {Readable(message)}");

    public void ToApp(Message message, SessionID sessionID)
        => Console.WriteLine($"[app] ToApp: {Readable(message)}");

    public void FromApp(Message message, SessionID sessionID)
        => Console.WriteLine($"[app] FromApp (MsgType={message.Header.GetString(35)}): {Readable(message)}");

    private static string Readable(Message message) => message.ToString().Replace('\u0001', '|');
}

public static class Program
{
    public static void Main(string[] args)
    {
        string cfgPath = args.Length > 0 ? args[0] : "quickfixn-session.cfg";
        var settings = new SessionSettings(cfgPath);
        var application = new LoggingApplication();

        // In-memory store/log: this is a throwaway interop probe, not a
        // persistent session — there is nothing worth keeping across runs.
        var storeFactory = new QuickFix.Store.MemoryStoreFactory();
        var logFactory = new QuickFix.Logger.ScreenLogFactory(settings);

        using var initiator = new QuickFix.Transport.SocketInitiator(application, storeFactory, settings, logFactory);
        initiator.Start();

        Console.WriteLine("[main] initiator started, press Enter to stop...");
        Console.ReadLine();

        initiator.Stop();
    }
}
