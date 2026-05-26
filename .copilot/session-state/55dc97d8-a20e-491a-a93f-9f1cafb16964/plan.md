# Plano: Canal Auction (#57) + Expansão Bands (#58)

## Roadmap
- **v0.7.0** — #57 Canal Auction (`AuctionImbalance_19` + `SecurityGroupPhase_10`)
- **v0.8.0** — #58 Expansão Bands (`QuantityBand_21` no canal existente)

---

## #57 — Canal Auction (v0.7.0)

### Dados agregados

| Campo | Tipo | Fonte | Observação |
|-------|------|-------|------------|
| `ImbalanceQty` | long? | AuctionImbalance_19.MDEntrySize | Opcional no schema |
| `ImbalanceCondition` | ushort | AuctionImbalance_19.ImbalanceCondition | 0x0100=MoreBuyers, 0x0200=MoreSellers, 0=Balanced |
| `GroupPhase` | byte | SecurityGroupPhase_10.TradingSessionSubID | Enum TradingSessionSubID |
| `TradSesOpenTime` | ulong? | SecurityGroupPhase_10.TradSesOpenTime | Opcional (só Pre-open) |

### Wire format
- `MessageType.Auction = 0x00B2`
- `DataFlags.Auction = 0x80` (último bit disponível)
- Layout: header(4) + securityId(8) + symLen(1) + symbol(≤255) + numericMask(1) + slots

### Field IDs (numeric mask, 8 bits)
- 0: ImbalanceQty (i64)
- 1: ImbalanceCondition (i16 widened to i64)
- 2: GroupPhase (byte widened to i64)
- 3: TradSesOpenTime (i64 UTC nanos)
- 4: AsOfTimestamp (i64 UTC nanos)
- 5: RptSeq (i64)

### Tarefas

1. **InstrumentInfo** — adicionar `_auctionVersion`, `BumpAuctionVersion()`, campo `AuctionTimestamp`
2. **IMarketDataEventHandler** — adicionar `OnAuctionChanged`
3. **MarketDataManager** — delta-detection em `HandleAuctionImbalance` + `HandleSecurityGroupPhase`, fire hook
4. **WireProtocol** — `MessageType.Auction`, `DataFlags.Auction`, `WriteAuction`
5. **SnapshotEmitter** — `SendAuctionSnapshot`
6. **SubscriptionState** — `WantsAuction`
7. **MpscOutboundRing** — `AuctionWake/Add/Remove` kinds
8. **ClientSession** — `_auctionVersions` dict, wake/drain/append
9. **SubscriptionManager** — bootstrap + activation + rollback + notify
10. **GroupConflationHandler** — `OnAuctionChanged`
11. **SDK WireFormat** — `ReadAuction`
12. **SDK Events** — `AuctionEvent`
13. **SDK MarketDataClient** — `Auction` event + dispatch
14. **SDK SubscribeFlags** — `Auction = 0x80`
15. **Testes** — wire-format pins + roundtrip
16. **Docs** — WEBSOCKET-PROTOCOL.md + CLIENT-SDK.md

---

## Status

- [x] Plano criado
- [ ] InstrumentInfo
- [ ] IMarketDataEventHandler
- [ ] MarketDataManager
- [ ] WireProtocol
- [ ] SnapshotEmitter
- [ ] SubscriptionState
- [ ] MpscOutboundRing
- [ ] ClientSession
- [ ] SubscriptionManager
- [ ] GroupConflationHandler
- [ ] SDK WireFormat
- [ ] SDK Events
- [ ] SDK MarketDataClient
- [ ] SDK SubscribeFlags
- [ ] Testes
- [ ] Docs
- [ ] Commit + tag v0.7.0
