# Protocol contract gate

This repository decodes official B3 Binary UMDF/SBE packets and also exposes a
separate MarketData WebSocket protocol for downstream clients. Keep those
contracts distinct.

## Durable rules

- Cross-repo wire dependencies must cite the merged upstream PR URL and merge
  commit SHA that produced the contract. Issue proposals, draft PRs, and local
  branches are useful context, but they are not implementation evidence.
- Protocol implementation requests must also cite the exact official schema
  family/version, SBE template ID/name, fields/tags/offsets used, and enum values
  being decoded or projected.
- If that contract evidence is absent, file the work as protocol research or a
  blocker instead of requesting an implementation.
- The MarketData WebSocket protocol may define its own frames/messages derived
  from supported data, but those frames must be described as MarketData WebSocket
  messages, never as official UMDF templates.

## Issue routes

Use **Protocol implementation (contract proven)** only when the merged upstream
wire contract and official schema details are known. Use **Protocol research /
blocker (contract absent)** for feasibility work, missing schema evidence, or
blocked downstream requests. Use **General issue (non-protocol)** for ordinary
bugs, docs, operations, UI, and performance work that does not change a wire
contract.
