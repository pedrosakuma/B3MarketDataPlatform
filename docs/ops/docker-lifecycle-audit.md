# Docker lifecycle audit — issue #18

Investigation closing GitHub issue #18 (signal handling, SIGTERM drain, OOM
recovery, PID 1 reaping, healthcheck timing). Findings are organised per
question in the original issue; each one ends with **status** (`fixed in
this PR`, `no issues found`, or `recommendation logged`) and the relevant
`file:line` citations.

## Summary

| # | Question | Finding | Status |
|---|---|---|---|
| 1 | SIGTERM drain end-to-end | Drain budget can exceed `stop_grace_period`; healthcheck too eager | `fixed in this PR` |
| 2 | PID 1 reaping | dotnet is PID 1 directly via exec-form ENTRYPOINT; no shell, no zombies | `no issues found` |
| 3 | OOM recovery | State is purely in-memory; restart heals via snapshot policy; Kestrel sets SO_REUSEADDR | `no issues found` |
| 4 | Frontend lifecycle | `depends_on: service_healthy` is correct but coupled to finding #1 healthcheck fix | `fixed in this PR` |
| 5 | Healthcheck timing | `--health-check` probed `/live` (always 200); now probes `/health` (gates on `IsReady` + stale window) | `fixed in this PR` |

Bonus: `docker-entrypoint.sh` is dead code (never COPYed into the image)
because `AppSettings.ApplyEnvironment` already folds the legacy
`PCAP_DIR`/`PCAP_PREFIX`/`REPLAY_SPEED` env vars. Removed.

## 1. SIGTERM drain end-to-end

**(a) Shell trap forwards SIGTERM to the .NET process.**
Not applicable — `Dockerfile:37` uses exec-form `ENTRYPOINT
["/app/B3.Umdf.ConsoleApp"]`, so dotnet is PID 1 directly and the runtime
receives SIGTERM without any shell intermediary.

**(b) .NET cancellation propagates through the pipelines.**
`src/B3.Umdf.ConsoleApp/Program.cs:90-109` registers `PosixSignalRegistration`
for SIGTERM/SIGINT/SIGHUP/SIGQUIT, each calling `TriggerShutdown(...)`
(`Program.cs:57-79`) which cancels the shared CTS. The CTS feeds
`multiFeed.StartAsync(cts.Token)` so the dispatch loops exit cooperatively;
`MultiFeedManager.StopAsync()` (`Program.cs:687`) joins the broadcaster
group and `WebSocketHost.StopAsync(...)` (`Program.cs:696`,
`src/B3.Umdf.Server/WebSocketHost.cs:168-176`) sends a clean WS 1001
EndpointUnavailable to every connected client via `ShutdownCoordinator.
DrainClientsAsync`. **No gap.**

**(c) The drain is bounded against the orchestrator grace.**
This is the one real bug.

`Program.cs:693-698` runs:

```csharp
await Task.Delay(TimeSpan.FromSeconds(shutdownDrainSeconds));      // up to N s
await wsHost.StopAsync(TimeSpan.FromSeconds(shutdownDrainSeconds)); // up to N s
```

With `ShutdownDrainSeconds = 5` (default, `AppSettings.cs:133`) the WS
phase alone burns up to **10 s**, and that's on top of
`multiFeed.StopAsync()` and the broadcaster joins (`Program.cs:705`). The
old `docker-compose.yml` set `stop_grace_period: 10s` — i.e. SIGKILL
arrives before the close handshake completes and clients see TCP RST.

**Fix in this PR.** `docker-compose.yml` `stop_grace_period: 10s → 30s`.
This is a one-line change that gives the documented drain budget room to
breathe, without changing the .NET-side shutdown logic. A future cleanup
could fold the redundant `Task.Delay(N)` into `WebSocketHost.StopAsync`
itself (the coordinator there already bounds the close handshake), but
that's a behavioural change for another PR.

**(d) Clients see a clean WS close.**
Verified by reading `ShutdownCoordinator.DrainClientsAsync` — it issues
`CloseAsync(WebSocketCloseStatus.EndpointUnavailable, ...)` per session
within the `closeHandshakeBudget`. Provided #c above is honoured, clients
see WS 1001 and not RST.

## 2. PID 1 reaping

`Dockerfile:37` uses exec-form `ENTRYPOINT
["/app/B3.Umdf.ConsoleApp"]`. dotnet is PID 1 directly. The .NET 10 runtime
handles PID 1 signal delivery natively and we do not spawn external
processes from `Program.cs` (verified by `grep -r "Process.Start\|fork"
src/` — no hits). **No zombie reaping required; no shell-init like `tini`
needed.**

The previously-shipped `docker-entrypoint.sh` was a vestige: never COPYed
into the image (the Dockerfile copies only `/app` and `/tools`), so any
deploy that ran it would have had to add it manually. The env-var
fallback in `AppSettings.ApplyEnvironment` (`src/B3.Umdf.Server/
AppSettings.cs:377-401`) already reads `PCAP_DIR`, `PCAP_PREFIX`, `WS_PORT`,
`REPLAY_SPEED`, so the script duplicates work that already happens
in-process. **Removed in this PR** to eliminate the confusion about which
entrypoint is authoritative.

## 3. OOM recovery

- **`StaleMboBuffer` state.** Per stored memory + `src/B3.Umdf.Book/
  StaleMboBuffer.cs`, state is purely in-memory — no lock files, no Unix
  sockets created. Restart starts from a clean slate.
- **Book healing.** `SymbolStateRegistry` uses `RequireSnapshot` policy for
  the `Mbo` kind (`SymbolStateRegistry.DefaultPolicies`); the next snapshot
  rebuilds books deterministically. Test coverage in
  `tests/B3.Umdf.Book.Tests/` (237 passing).
- **WS port rebind.** Kestrel on Linux enables `SO_REUSEADDR` by default,
  so the next process binding `:8080` after a SIGKILL does not hit
  `EADDRINUSE` from `TIME_WAIT`. Verified empirically by:

  ```sh
  docker compose up -d backend
  docker compose kill -s KILL backend  # bypass drain
  docker compose up -d backend         # comes up clean
  ```

**No issues found.**

## 4. Frontend container lifecycle

`docker-compose.yml:29-31` gates the frontend on
`depends_on: backend: condition: service_healthy`. This is correct.

The coupled question — does the frontend reconnect after a backend
restart? — is a frontend concern; the backend's contract is to expose
`/live` (always 200 while alive) and `/health` (503 while stale). The
frontend's WS client should retry on disconnect, which is a separate
audit item not in scope here.

## 5. Healthcheck timing

`Dockerfile:34-35` (before this PR):

```dockerfile
HEALTHCHECK --interval=10s --timeout=3s --start-period=30s --retries=3 \
    CMD ["/app/B3.Umdf.ConsoleApp", "--health-check"]
```

`--health-check` resolves to `mode=live` in `Bootstrap.cs:158`, which probes
`/live` — always 200 as soon as Kestrel is listening
(`HealthEndpointMapper.cs:87`). This means Compose marks the container
"healthy" within ~1 s of Kestrel binding, **before any PCAP packet has
been parsed**. The frontend's `depends_on: service_healthy` consequently
fires while the subscription set is still empty.

**Fix in this PR.** Switch CMD to `--health-check=health`, which probes
`/health` (`Bootstrap.cs:159`, `HealthEndpointMapper.cs:39-82`). `/health`
returns 503 while `_subscriptionManager.IsReady == false` or any feed
group is stuck in non-Streaming state past `MaxStaleSeconds`. The Compose
healthcheck now genuinely reflects "ready to serve subscriptions".

`start-period: 30s` is preserved — the consumer typically reaches Ready
within 5–15 s once the SnapshotRecovery channel completes its first
sweep, well within budget.

## Smoke-test commands

The behaviour above can be re-verified locally without K8s:

```sh
# Build + start fresh
docker compose up -d --build

# 1. Healthcheck honours feed state now (returns "starting" until ready,
#    then "healthy" once /health passes)
docker inspect --format='{{.State.Health.Status}}' $(docker compose ps -q backend)

# 2. Graceful SIGTERM drain. Should complete within ~20s on a healthy box;
#    container logs should show "Shutdown requested via PosixSignal SIGTERM",
#    "Shutting down gracefully...", and "ProcessExit reached after Xms".
time docker compose stop backend     # 30s grace

# 3. OOM-style hard kill — verify clean restart with no port-bind issue
docker compose kill -s KILL backend
docker compose up -d backend
docker compose logs --tail=20 backend
```

A client-side observer (`tools/BookFeedLoadHarness/`) can confirm clients
see WS 1001 EndpointUnavailable on graceful stop and RST on KILL.

## Out of scope (separate tickets if/when needed)

- **Fold the redundant `Task.Delay` into `WebSocketHost.StopAsync`.** The
  coordinator there already bounds the close handshake; the two-step
  drain is leftover from before the coordinator existed. Pure cleanup.
- **Tunable `MaxStaleSeconds` from env var.** Currently driven by
  `UMDF_HEALTH_MAX_STALE_SECONDS` per `HealthEndpointMapper`; already
  works but undocumented in `README.md`.
- **Frontend reconnect-on-disconnect audit.** Frontend repo concern.
