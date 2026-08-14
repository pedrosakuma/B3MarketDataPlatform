#!/usr/bin/env node
// Standalone FIX Conflated validator for the local sandbox.
// Usage:
//   HTTP_BASE=http://localhost:8080 node tools/fix/fix-validate.mjs localhost 9200 PETR4
//
// Example with a replay-driven local run (future issue #103 item 2 can wrap this
// together with the WS validator during a live PCAP replay):
//   ./tools/pcap/download-pcaps.sh
//   UMDF_FIX_CONFLATED_ENABLED=true UMDF_FIX_CONFLATED_PORT=9200 \
//   dotnet run --project src/B3.Umdf.ConsoleApp -- \
//     --pcap-prefix pcap/20250331_MBO_084_EQT --ws-port 8080 --speed 1
//   HTTP_BASE=http://localhost:8080 node tools/fix/fix-validate.mjs localhost 9200 PETR4
//
// The validator behaves like a real FIX 4.4 client: it opens a raw TCP socket,
// performs Logon, rebuilds local book state from the automatic full snapshot plus
// later incremental refreshes, and periodically compares the result with
// GET /book/{symbol} on the HTTP side used by the existing WS validator.

import net from 'node:net';
import zlib from 'node:zlib';

const SOH = 0x01;
const BEGIN_STRING = 'FIX.4.4';

const TAG = {
  BeginString: 8,
  BodyLength: 9,
  MsgType: 35,
  OrderId: 37,
  SecurityId: 48,
  SenderCompId: 49,
  Symbol: 55,
  Text: 58,
  TargetCompId: 56,
  MsgSeqNum: 34,
  SendingTime: 52,
  EncryptMethod: 98,
  HeartBtInt: 108,
  TestReqId: 112,
  CheckSum: 10,
  MDReqId: 262,
  SubscriptionRequestType: 263,
  NoRelatedSym: 146,
  NoMDEntries: 268,
  MDEntryType: 269,
  MDEntryPx: 270,
  MDEntrySize: 271,
  MDEntryDate: 272,
  MDEntryTime: 273,
  MDUpdateAction: 279,
  TradeId: 1003,
};

const MSG = {
  Heartbeat: '0',
  TestRequest: '1',
  Logout: '5',
  Logon: 'A',
  MarketDataRequest: 'V',
  MarketDataSnapshotFullRefresh: 'W',
  MarketDataRequestReject: 'Y',
  MarketDataIncrementalRefresh: 'X',
};

const HOST = process.argv[2] || '127.0.0.1';
const PORT = parseRequiredPort(process.argv[3] || '9200');
const REQUESTED_SYMBOL = normalizeSymbol(process.argv[4] || '') || null;
const HTTP_BASE = process.argv[5] || process.env.HTTP_BASE || `http://${HOST}:8080`;
const CHECK_INTERVAL = parsePositiveInt(process.env.CHECK_INTERVAL_MS || '5000', 5000);
const HTTP_TIMEOUT_MS = parsePositiveInt(process.env.HTTP_TIMEOUT_MS || '3000', 3000);
const INITIAL_HEARTBEAT_SECONDS = parsePositiveInt(process.env.FIX_HEARTBEAT_SEC || '30', 30);
const RUN_SECONDS = parseNonNegativeInt(process.env.RUN_SECONDS || '0', 0);
const WS_SNAPSHOT_COMPARE_URL = process.env.WS_SNAPSHOT_COMPARE_URL || '';
const WS_SNAPSHOT_TIMEOUT_MS = parsePositiveInt(process.env.WS_SNAPSHOT_TIMEOUT_MS || '5000', 5000);
const SENDER_COMP_ID = process.env.FIX_SENDER_COMP_ID || `FIX-VALIDATOR-${process.pid}-${Date.now().toString(36)}`;
const TARGET_COMP_ID = process.env.FIX_TARGET_COMP_ID || 'SANDBOX';

let inboundBuffer = Buffer.alloc(0);
let inflatedBuffer = Buffer.alloc(0);
let nextOutboundSeqNum = 1;
let heartbeatIntervalSeconds = INITIAL_HEARTBEAT_SECONDS;
let trackedSymbol = REQUESTED_SYMBOL;
let trackedSecurityId = null;
let lastSentAt = 0;
let lastReceivedAt = 0;
let sessionActive = false;
let snapshotSeen = false;
let shutdownRequested = false;
let lastBookError = null;

const orders = new Map();
const counters = {
  messagesIn: 0,
  logons: 0,
  snapshots: 0,
  incrementals: 0,
  bookEntries: 0,
  tradeEntries: 0,
  heartbeatsIn: 0,
  heartbeatsOut: 0,
  testRequestsIn: 0,
  divergences: 0,
  unknownMessages: 0,
};

let checkTimer = null;
let heartbeatTimer = null;
let runTimer = null;
let shuttingDown = false;
let shutdownPromise = null;

console.log(`Connecting FIX validator to ${HOST}:${PORT} sender=${SENDER_COMP_ID} target=${TARGET_COMP_ID} symbol=${trackedSymbol ?? '(first snapshot)'} http=${HTTP_BASE}`);

const socket = net.createConnection({ host: HOST, port: PORT });
socket.setNoDelay(true);
const inflater = zlib.createInflate();

inflater.on('data', chunk => {
  lastReceivedAt = Date.now();
  inflatedBuffer = Buffer.concat([inflatedBuffer, chunk]);
  drainInbound(socket);
});

inflater.on('error', error => {
  console.error(`Inbound zlib inflate failed: ${error.message}`);
  socket.destroy(error);
});

socket.on('connect', () => {
  console.log('TCP connected; sending Logon');
  sendSessionMessage(socket, MSG.Logon, [
    [TAG.EncryptMethod, '0'],
    [TAG.HeartBtInt, String(heartbeatIntervalSeconds)],
  ]);

  heartbeatTimer = setInterval(() => {
    if (!sessionActive || socket.destroyed)
      return;

    const now = Date.now();
    if (now - lastSentAt < heartbeatIntervalSeconds * 1000)
      return;

    counters.heartbeatsOut++;
    sendSessionMessage(socket, MSG.Heartbeat, []);
  }, 1000);

  checkTimer = setInterval(() => {
    void checkServer();
  }, CHECK_INTERVAL);

  if (RUN_SECONDS > 0)
  {
    runTimer = setTimeout(() => {
      console.log(`RUN_SECONDS=${RUN_SECONDS} reached; requesting graceful shutdown.`);
      void beginShutdown('timer');
    }, RUN_SECONDS * 1000);
  }
});

socket.on('data', chunk => {
  inboundBuffer = Buffer.concat([inboundBuffer, chunk]);
  inflater.write(chunk);
});

socket.on('close', hadError => {
  clearTimers();
  printSummary(hadError ? 'socket closed after error' : 'socket closed');
  if (!shutdownRequested && lastBookError)
    console.error(`Last server-check issue: ${lastBookError}`);
  process.exit(counters.divergences > 0 ? 1 : 0);
});

socket.on('error', error => {
  console.error(`Socket error: ${error.message}`);
});

process.on('SIGINT', () => void beginShutdown('SIGINT'));
process.on('SIGTERM', () => void beginShutdown('SIGTERM'));

async function checkServer() {
  const local = computeBookState();
  const prefix = `[check] ${trackedSymbol ?? '(waiting-symbol)'}`;

  if (!snapshotSeen || !trackedSymbol) {
    console.log(`${prefix} waiting for initial snapshot... msgs=${counters.messagesIn} orders=${orders.size}`);
    return;
  }

  try {
    const response = await fetch(`${HTTP_BASE}/book/${encodeURIComponent(trackedSymbol)}`, {
      signal: AbortSignal.timeout(HTTP_TIMEOUT_MS),
    });

    if (!response.ok) {
      lastBookError = `GET /book/${trackedSymbol} -> HTTP ${response.status}`;
      console.log(`${prefix} local=${formatLocalSummary(local)} | server-check unavailable (${response.status})`);
      return;
    }

    const payload = await response.json();
    const server = extractServerBook(payload);
    if (!server.valid) {
      lastBookError = `unexpected /book payload keys: ${Object.keys(payload).join(', ') || '(none)'}`;
      console.log(`${prefix} local=${formatLocalSummary(local)} | server-check unexpected payload`);
      console.log(`  payload keys: ${Object.keys(payload).join(', ') || '(none)'}`);
      return;
    }

    lastBookError = null;
    const comparison = compareBooks(local, server);
    const tag = comparison.match ? '✅ MATCH' : '❌ MISMATCH';
    console.log(`${tag} | ${trackedSymbol} | local: ${formatLocalSummary(local)} | server: ${formatServerSummary(server)}`);
    console.log(`  msgs=${counters.messagesIn} snaps=${counters.snapshots} incr=${counters.incrementals} bookEntries=${counters.bookEntries} trades=${counters.tradeEntries} hbIn=${counters.heartbeatsIn} hbOut=${counters.heartbeatsOut} testReq=${counters.testRequestsIn}`);

    if (!comparison.match) {
      counters.divergences++;
      comparison.lines.slice(0, 10).forEach(line => console.log(`  ${line}`));
      console.log(`  local top bids: ${formatTopLevels(local.bids)}`);
      console.log(`  local top asks: ${formatTopLevels(local.asks)}`);
      if (server.bids.length > 0)
        console.log(`  server top bids: ${formatTopLevels(server.bids)}`);
      if (server.asks.length > 0)
        console.log(`  server top asks: ${formatTopLevels(server.asks)}`);
    }
  } catch (error) {
    lastBookError = error instanceof Error ? error.message : String(error);
    console.log(`${prefix} local=${formatLocalSummary(local)} | server-check failed: ${lastBookError}`);
  }
}

async function runFreshWsSnapshotCompare() {
  if (!WS_SNAPSHOT_COMPARE_URL) {
    console.log('[fresh-ws] skipped (WS_SNAPSHOT_COMPARE_URL not set)');
    return;
  }

  if (!snapshotSeen || !trackedSymbol) {
    console.log('[fresh-ws] skipped (no FIX snapshot tracked yet)');
    return;
  }

  try {
    const fresh = await pullFreshWsBookState(WS_SNAPSHOT_COMPARE_URL, trackedSymbol, WS_SNAPSHOT_TIMEOUT_MS);
    const local = computeBookState();
    const comparison = compareBooks(local, fresh);
    const tag = comparison.match ? '✅ MATCH' : '❌ MISMATCH';
    console.log(`${tag} | fresh-ws ${trackedSymbol} | local: ${formatLocalSummary(local)} | snapshot: ${formatServerSummary(fresh)}`);
    console.log(`  wsSnapshot msgs=${fresh.metrics.messages} adds=${fresh.metrics.adds} upd=${fresh.metrics.updates} dels=${fresh.metrics.deletes} snaps=${fresh.metrics.snapshots} clears=${fresh.metrics.clears}`);
    if (!comparison.match) {
      counters.divergences++;
      comparison.lines.slice(0, 10).forEach(line => console.log(`  ${line}`));
      console.log(`  local top bids: ${formatTopLevels(local.bids)}`);
      console.log(`  local top asks: ${formatTopLevels(local.asks)}`);
      console.log(`  snapshot top bids: ${formatTopLevels(fresh.bids)}`);
      console.log(`  snapshot top asks: ${formatTopLevels(fresh.asks)}`);
    }
  } catch (error) {
    counters.divergences++;
    const message = error instanceof Error ? error.message : String(error);
    console.log(`[fresh-ws] compare failed: ${message}`);
  }
}

async function beginShutdown(reason) {
  if (shutdownPromise)
    return shutdownPromise;

  shutdownPromise = (async () => {
    if (runTimer) {
      clearTimeout(runTimer);
      runTimer = null;
    }

    if (reason === 'timer')
      await runFreshWsSnapshotCompare();

    requestShutdown(reason);
  })();

  return shutdownPromise;
}

function requestShutdown(reason) {
  if (shuttingDown)
    return;

  shuttingDown = true;
  shutdownRequested = true;
  clearTimers();
  console.log(`Shutdown requested (${reason})`);
  if (sessionActive && !socket.destroyed) {
    sessionActive = false;
    sendSessionMessage(socket, MSG.Logout, [[TAG.Text, `fix-validate shutdown (${reason})`]]);
    socket.end();
    setTimeout(() => socket.destroy(), 1000).unref();
    return;
  }

  socket.destroy();
  inflater.destroy();
}

function drainInbound(currentSocket) {
  while (inflatedBuffer.length > 0) {
    const decoded = decodeFrame(inflatedBuffer);
    if (decoded.status === 'incomplete')
      return;

    if (decoded.status === 'error') {
      console.error(`Inbound FIX decode failed: ${decoded.error}`);
      currentSocket.destroy();
      return;
    }

    inflatedBuffer = inflatedBuffer.subarray(decoded.bytesConsumed);
    handleMessage(decoded.message, currentSocket);
  }
}

function handleMessage(message, currentSocket) {
  counters.messagesIn++;
  const msgType = message.get(TAG.MsgType);
  if (!msgType) {
    console.error('Inbound FIX frame missing MsgType (35).');
    currentSocket.destroy();
    return;
  }

  switch (msgType) {
    case MSG.Logon: {
      counters.logons++;
      sessionActive = true;
      heartbeatIntervalSeconds = parseNonNegativeInt(message.get(TAG.HeartBtInt) || String(heartbeatIntervalSeconds), heartbeatIntervalSeconds);
      console.log(`Logon ack seq=${message.get(TAG.MsgSeqNum) ?? '?'} heartbeat=${heartbeatIntervalSeconds}s sender=${message.get(TAG.SenderCompId) ?? '?'} target=${message.get(TAG.TargetCompId) ?? '?'}`);
      sendMarketDataRequest(currentSocket);
      break;
    }
    case MSG.Heartbeat:
      counters.heartbeatsIn++;
      if (message.get(TAG.TestReqId))
        console.log(`Heartbeat(TestReqID=${message.get(TAG.TestReqId)})`);
      break;
    case MSG.TestRequest:
      counters.testRequestsIn++;
      console.log(`TestRequest received id=${message.get(TAG.TestReqId) ?? '(missing)'}`);
      counters.heartbeatsOut++;
      sendSessionMessage(currentSocket, MSG.Heartbeat, message.get(TAG.TestReqId)
        ? [[TAG.TestReqId, message.get(TAG.TestReqId)]]
        : []);
      break;
    case MSG.Logout:
      console.log(`Logout received: ${message.get(TAG.Text) ?? '(no reason)'}`);
      requestShutdown('server-logout');
      break;
    case MSG.MarketDataSnapshotFullRefresh:
      applySnapshot(message);
      break;
    case MSG.MarketDataRequestReject:
      console.error(`MarketDataRequestReject mdReqId=${message.get(TAG.MDReqId) ?? '?'} text=${message.get(TAG.Text) ?? '(none)'}`);
      requestShutdown('mdreq-reject');
      break;
    case MSG.MarketDataIncrementalRefresh:
      applyIncremental(message);
      break;
    default:
      counters.unknownMessages++;
      if (counters.unknownMessages <= 5)
        console.log(`Ignoring FIX MsgType=${msgType}`);
      break;
  }
}

function applySnapshot(message) {
  const symbol = normalizeSymbol(message.get(TAG.Symbol) || '');
  const securityId = message.get(TAG.SecurityId) || null;
  if (!acceptInstrument(symbol, securityId))
    return;

  const entries = parseRepeatingGroup(message, TAG.NoMDEntries, TAG.MDEntryType);
  orders.clear();

  for (const entry of entries) {
    const entryType = entry.get(TAG.MDEntryType);
    if (entryType !== '0' && entryType !== '1')
      continue;

    const price = toFiniteNumber(entry.get(TAG.MDEntryPx));
    const size = toFiniteNumber(entry.get(TAG.MDEntrySize));
    if (price == null || size == null)
      continue;

    const orderId = entry.get(TAG.OrderId) || `snapshot:${entryType}:${price}:${orders.size}`;
    orders.set(orderId, { side: entryType, price, size });
  }

  snapshotSeen = true;
  counters.snapshots++;
  const local = computeBookState();
  console.log(`Snapshot loaded for ${trackedSymbol} securityId=${trackedSecurityId ?? '?'} entries=${entries.length} local=${formatLocalSummary(local)}`);
}

function applyIncremental(message) {
  const entries = parseRepeatingGroup(message, TAG.NoMDEntries, TAG.MDUpdateAction);
  let appliedBookEntries = 0;
  let tradeEntries = 0;

  for (const entry of entries) {
    const symbol = normalizeSymbol(entry.get(TAG.Symbol) || message.get(TAG.Symbol) || '');
    const securityId = entry.get(TAG.SecurityId) || message.get(TAG.SecurityId) || null;
    if (!acceptInstrument(symbol, securityId))
      continue;

    const entryType = entry.get(TAG.MDEntryType);
    const action = entry.get(TAG.MDUpdateAction);
    if (!entryType || !action)
      continue;

    if (entryType === '2') {
      tradeEntries++;
      counters.tradeEntries++;
      if (tradeEntries <= 3) {
        console.log(`Trade ${trackedSymbol} px=${entry.get(TAG.MDEntryPx) ?? '?'} qty=${entry.get(TAG.MDEntrySize) ?? '?'} tradeId=${entry.get(TAG.TradeId) ?? '?'}`);
      }
      continue;
    }

    if (entryType !== '0' && entryType !== '1')
      continue;

    applyBookDelta(entryType, action, entry);
    appliedBookEntries++;
    counters.bookEntries++;
  }

  if (appliedBookEntries > 0 || tradeEntries > 0)
    counters.incrementals++;
}

function applyBookDelta(entryType, action, entry) {
  const side = entryType;
  const orderId = entry.get(TAG.OrderId) || null;

  switch (action) {
    case '0':
    case '1': {
      if (!orderId)
        return;

      const price = toFiniteNumber(entry.get(TAG.MDEntryPx));
      const size = toFiniteNumber(entry.get(TAG.MDEntrySize));
      if (price == null || size == null)
        return;

      orders.set(orderId, { side, price, size });
      return;
    }
    case '2':
      if (orderId)
        orders.delete(orderId);
      return;
    case '3': {
      const thresholdPrice = toFiniteNumber(entry.get(TAG.MDEntryPx));
      for (const [existingOrderId, order] of orders) {
        if (order.side !== side)
          continue;

        if (thresholdPrice == null || deleteThruMatches(side, order.price, thresholdPrice))
          orders.delete(existingOrderId);
      }
      return;
    }
    default:
      return;
  }
}

function deleteThruMatches(side, price, thresholdPrice) {
  return side === '0'
    ? price >= thresholdPrice
    : price <= thresholdPrice;
}

function acceptInstrument(symbol, securityId) {
  if (trackedSecurityId && securityId && trackedSecurityId !== securityId)
    return false;
  if (trackedSymbol && symbol && trackedSymbol !== symbol)
    return false;

  if (!trackedSymbol && symbol) {
    trackedSymbol = symbol;
    console.log(`Tracking symbol ${trackedSymbol}`);
  }

  if (!trackedSecurityId && securityId) {
    trackedSecurityId = securityId;
    console.log(`Tracking securityId ${trackedSecurityId}`);
  }

  if (REQUESTED_SYMBOL && trackedSymbol && trackedSymbol !== REQUESTED_SYMBOL)
    return false;

  return true;
}


function sendMarketDataRequest(currentSocket) {
  const requestSecurityId = resolveRequestedSecurityId();
  sendSessionMessage(currentSocket, MSG.MarketDataRequest, [
    [TAG.MDReqId, `mdreq-${REQUESTED_SYMBOL}`],
    [TAG.SubscriptionRequestType, '1'],
    [TAG.NoRelatedSym, '1'],
    [TAG.SecurityId, resolveRequestedSecurityId()],
    [TAG.SecurityIdSource, '8'],
    [TAG.SecurityExchange, 'BVMF'],
  ]);
}

function resolveRequestedSecurityId() {
  if (trackedSecurityId)
    return trackedSecurityId;

  const envSecurityId = process.env.FIX_SECURITY_ID || '';
  if (envSecurityId)
    return envSecurityId;

  throw new Error('FIX_SECURITY_ID env var is required for explicit MarketDataRequest subscriptions.');
}

function sendSessionMessage(currentSocket, msgType, extraFields) {
  const fields = [
    [TAG.MsgType, msgType],
    [TAG.SenderCompId, SENDER_COMP_ID],
    [TAG.TargetCompId, TARGET_COMP_ID],
    [TAG.MsgSeqNum, String(nextOutboundSeqNum++)],
    [TAG.SendingTime, formatUtcTimestamp(new Date())],
    ...extraFields,
  ];

  const payload = encodeFixFrame(fields);
  currentSocket.write(payload);
  lastSentAt = Date.now();
}

function encodeFixFrame(fields) {
  const body = Buffer.from(fields.map(([tag, value]) => `${tag}=${value}${String.fromCharCode(SOH)}`).join(''), 'ascii');
  const prefix = Buffer.from(`8=${BEGIN_STRING}${String.fromCharCode(SOH)}9=${body.length}${String.fromCharCode(SOH)}`, 'ascii');
  const checksumBase = Buffer.concat([prefix, body]);
  const checksum = calculateChecksum(checksumBase);
  const trailer = Buffer.from(`10=${checksum.toString().padStart(3, '0')}${String.fromCharCode(SOH)}`, 'ascii');
  return Buffer.concat([checksumBase, trailer]);
}

function decodeFrame(buffer) {
  const firstSoh = buffer.indexOf(SOH);
  if (firstSoh < 0)
    return { status: 'incomplete' };

  const beginField = buffer.subarray(0, firstSoh).toString('ascii');
  if (beginField !== `8=${BEGIN_STRING}`)
    return { status: 'error', error: `invalid BeginString field (${beginField})` };

  const secondStart = firstSoh + 1;
  const secondSoh = buffer.indexOf(SOH, secondStart);
  if (secondSoh < 0)
    return { status: 'incomplete' };

  const bodyLengthField = buffer.subarray(secondStart, secondSoh).toString('ascii');
  if (!bodyLengthField.startsWith('9='))
    return { status: 'error', error: `missing BodyLength field (${bodyLengthField})` };

  const bodyLength = Number.parseInt(bodyLengthField.slice(2), 10);
  if (!Number.isInteger(bodyLength) || bodyLength < 0)
    return { status: 'error', error: `invalid BodyLength (${bodyLengthField})` };

  const bodyStart = secondSoh + 1;
  const checksumFieldStart = bodyStart + bodyLength;
  if (checksumFieldStart + 7 > buffer.length)
    return { status: 'incomplete' };

  if (buffer[checksumFieldStart] !== 0x31 || buffer[checksumFieldStart + 1] !== 0x30 || buffer[checksumFieldStart + 2] !== 0x3d)
    return { status: 'error', error: 'body length mismatch before CheckSum' };

  const checksumFieldEnd = buffer.indexOf(SOH, checksumFieldStart);
  if (checksumFieldEnd < 0)
    return { status: 'incomplete' };

  const expectedChecksumText = buffer.subarray(checksumFieldStart + 3, checksumFieldEnd).toString('ascii');
  const expectedChecksum = Number.parseInt(expectedChecksumText, 10);
  if (!Number.isInteger(expectedChecksum))
    return { status: 'error', error: `invalid CheckSum (${expectedChecksumText})` };

  const actualChecksum = calculateChecksum(buffer.subarray(0, checksumFieldStart));
  if (actualChecksum !== expectedChecksum)
    return { status: 'error', error: `checksum mismatch expected=${expectedChecksumText} actual=${actualChecksum.toString().padStart(3, '0')}` };

  const totalLength = checksumFieldEnd + 1;
  const message = parseFixMessage(buffer.subarray(0, totalLength));
  return { status: 'complete', message, bytesConsumed: totalLength };
}

function parseFixMessage(frame) {
  const fields = [];
  const valuesByTag = new Map();
  let cursor = 0;

  while (cursor < frame.length) {
    const fieldEnd = frame.indexOf(SOH, cursor);
    if (fieldEnd < 0)
      break;
    if (fieldEnd === cursor) {
      cursor = fieldEnd + 1;
      continue;
    }

    const raw = frame.subarray(cursor, fieldEnd).toString('ascii');
    const equals = raw.indexOf('=');
    if (equals <= 0)
      throw new Error(`Malformed FIX field: ${raw}`);

    const tag = Number.parseInt(raw.slice(0, equals), 10);
    if (!Number.isInteger(tag))
      throw new Error(`Invalid FIX tag: ${raw}`);

    const value = raw.slice(equals + 1);
    fields.push({ tag, value });
    const bucket = valuesByTag.get(tag);
    if (bucket)
      bucket.push(value);
    else
      valuesByTag.set(tag, [value]);

    cursor = fieldEnd + 1;
  }

  return {
    fields,
    get(tag) {
      return valuesByTag.get(tag)?.[0] ?? null;
    },
    getAll(tag) {
      return valuesByTag.get(tag) ?? [];
    },
  };
}

function parseRepeatingGroup(message, countTag, entryStartTag) {
  const entries = [];
  let insideGroup = false;
  let current = null;

  for (const field of message.fields) {
    if (field.tag === countTag) {
      insideGroup = true;
      continue;
    }

    if (!insideGroup || field.tag === TAG.CheckSum)
      continue;

    if (field.tag === entryStartTag) {
      current = new Map();
      entries.push(current);
    }

    if (!current)
      continue;

    current.set(field.tag, field.value);
  }

  return entries.map(values => ({
    get(tag) {
      return values.get(tag) ?? null;
    },
  }));
}

function computeBookState() {
  const bids = aggregateLevels('0', (a, b) => b.price - a.price);
  const asks = aggregateLevels('1', (a, b) => a.price - b.price);

  return {
    bids,
    asks,
    bidOrders: bids.reduce((sum, level) => sum + level.count, 0),
    askOrders: asks.reduce((sum, level) => sum + level.count, 0),
    bidLevels: bids.length,
    askLevels: asks.length,
    bestBid: bids[0]?.price ?? 0,
    bestAsk: asks[0]?.price ?? 0,
    crossed: bids.length > 0 && asks.length > 0 && bids[0].price >= asks[0].price,
  };
}

function aggregateLevels(side, sorter) {
  const aggregates = new Map();
  for (const order of orders.values()) {
    if (order.side !== side)
      continue;

    const key = String(order.price);
    const existing = aggregates.get(key);
    if (existing) {
      existing.qty += order.size;
      existing.count += 1;
    } else {
      aggregates.set(key, { price: order.price, qty: order.size, count: 1 });
    }
  }

  return [...aggregates.values()].sort(sorter);
}

function extractServerBook(payload) {
  const bidArray = firstNonEmptyLevelArray(payload.bids, payload.bidLevels);
  const askArray = firstNonEmptyLevelArray(payload.asks, payload.askLevels);

  const bidOrders = toFiniteNumber(payload.bidOrders) ?? sumKnownCounts(bidArray);
  const askOrders = toFiniteNumber(payload.askOrders) ?? sumKnownCounts(askArray);
  const bidLevels = Array.isArray(payload.bidLevels)
    ? bidArray.length
    : (toFiniteNumber(payload.bidLevels) ?? (bidArray.length > 0 ? bidArray.length : null));
  const askLevels = Array.isArray(payload.askLevels)
    ? askArray.length
    : (toFiniteNumber(payload.askLevels) ?? (askArray.length > 0 ? askArray.length : null));
  const bestBid = toFiniteNumber(payload.bestBid) ?? (bidArray[0]?.price ?? null);
  const bestAsk = toFiniteNumber(payload.bestAsk) ?? (askArray[0]?.price ?? null);
  const crossed = typeof payload.crossed === 'boolean'
    ? payload.crossed
    : (bestBid != null && bestAsk != null && bestBid > 0 && bestAsk > 0 && bestBid >= bestAsk);

  const hasSummary = [bidOrders, askOrders, bidLevels, askLevels, bestBid, bestAsk].some(value => value != null);
  return {
    valid: hasSummary || bidArray.length > 0 || askArray.length > 0,
    bids: bidArray,
    asks: askArray,
    bidOrders,
    askOrders,
    bidLevels,
    askLevels,
    bestBid,
    bestAsk,
    crossed,
  };
}

function firstNonEmptyLevelArray(...sources) {
  for (const source of sources) {
    const levels = extractLevelArray(source);
    if (levels.length > 0)
      return levels;
  }

  return [];
}

function sumKnownCounts(levels) {
  if (levels.length === 0 || levels.some(level => level.count == null))
    return null;

  return levels.reduce((sum, level) => sum + level.count, 0);
}

function extractLevelArray(source) {
  if (!Array.isArray(source))
    return [];

  return source
    .map(level => {
      if (Array.isArray(level)) {
        const price = toFiniteNumber(level[0]);
        const qty = toFiniteNumber(level[1]);
        const count = toFiniteNumber(level[2]);
        return price == null || qty == null ? null : { price, qty, count };
      }

      if (!level || typeof level !== 'object')
        return null;

      const price = toFiniteNumber(level.price ?? level.px ?? level.mdEntryPx ?? level.bestPrice);
      const qty = toFiniteNumber(level.qty ?? level.quantity ?? level.size ?? level.totalQuantity ?? level.mdEntrySize);
      const count = toFiniteNumber(level.count ?? level.orderCount ?? level.orders ?? level.totalOrders ?? level.orderQty);
      return price == null || qty == null ? null : { price, qty, count };
    })
    .filter(Boolean);
}

function compareBooks(local, server) {
  const lines = [];

  if (server.bidOrders != null && server.bidOrders !== local.bidOrders)
    lines.push(`bidOrders local=${local.bidOrders} server=${server.bidOrders}`);
  if (server.askOrders != null && server.askOrders !== local.askOrders)
    lines.push(`askOrders local=${local.askOrders} server=${server.askOrders}`);
  if (server.bidLevels != null && server.bidLevels !== local.bidLevels)
    lines.push(`bidLevels local=${local.bidLevels} server=${server.bidLevels}`);
  if (server.askLevels != null && server.askLevels !== local.askLevels)
    lines.push(`askLevels local=${local.askLevels} server=${server.askLevels}`);
  if (server.bestBid != null && server.bestBid !== local.bestBid)
    lines.push(`bestBid local=${formatPrice(local.bestBid)} server=${formatPrice(server.bestBid)}`);
  if (server.bestAsk != null && server.bestAsk !== local.bestAsk)
    lines.push(`bestAsk local=${formatPrice(local.bestAsk)} server=${formatPrice(server.bestAsk)}`);
  if (server.crossed != null && server.crossed !== local.crossed)
    lines.push(`crossed local=${local.crossed} server=${server.crossed}`);

  compareLevelSide(lines, 'bid', local.bids, server.bids);
  compareLevelSide(lines, 'ask', local.asks, server.asks);

  return {
    match: lines.length === 0,
    lines,
  };
}

function compareLevelSide(lines, side, localLevels, serverLevels) {
  if (serverLevels.length === 0)
    return;

  const max = Math.max(localLevels.length, serverLevels.length);
  for (let index = 0; index < max; index++) {
    const local = localLevels[index];
    const server = serverLevels[index];
    if (!local || !server) {
      lines.push(`${side}[${index}] local=${formatLevel(local)} server=${formatLevel(server)}`);
      continue;
    }

    if (local.price !== server.price || local.qty !== server.qty || (server.count != null && local.count !== server.count)) {
      lines.push(`${side}[${index}] local=${formatLevel(local)} server=${formatLevel(server)}`);
    }
  }
}

function formatLocalSummary(state) {
  return `${state.bidOrders}b/${state.askOrders}a ${state.bidLevels}lv/${state.askLevels}lv bid=${formatPrice(state.bestBid)} ask=${formatPrice(state.bestAsk)} crossed=${state.crossed}`;
}

function formatServerSummary(state) {
  return `${formatMaybe(state.bidOrders)}b/${formatMaybe(state.askOrders)}a ${formatMaybe(state.bidLevels)}lv/${formatMaybe(state.askLevels)}lv bid=${formatPrice(state.bestBid)} ask=${formatPrice(state.bestAsk)} crossed=${formatMaybe(state.crossed)}`;
}

function formatTopLevels(levels) {
  if (!levels || levels.length === 0)
    return '(empty)';

  return levels.slice(0, 5).map(formatLevel).join(' | ');
}

function formatLevel(level) {
  if (!level)
    return '(missing)';
  return `${formatPrice(level.price)} x ${formatNumber(level.qty)} (${level.count})`;
}

function formatPrice(value) {
  if (value == null)
    return '?';
  return Number.isInteger(value)
    ? String(value)
    : value.toFixed(4).replace(/0+$/, '').replace(/\.$/, '');
}

function formatNumber(value) {
  if (value == null)
    return '?';
  return Number.isInteger(value)
    ? String(value)
    : value.toFixed(4).replace(/0+$/, '').replace(/\.$/, '');
}

function formatMaybe(value) {
  return value == null ? '?' : String(value);
}

async function pullFreshWsBookState(wsUrl, symbol, timeoutMs) {
  const { default: WebSocket } = await import('ws');
  const snapshotIdleMs = parsePositiveInt(process.env.WS_SNAPSHOT_IDLE_MS || '200', 200);

  return await new Promise((resolve, reject) => {
    const ws = new WebSocket(wsUrl);
    const wsOrders = new Map();
    let subscribeSent = false;
    let snapshotSeenLocal = false;
    let snapshotPopulationStarted = false;
    let resolved = false;
    let snapshotIdleTimer = null;
    let timeout = setTimeout(() => finishError(`timeout waiting for fresh BookSnapshot after ${timeoutMs}ms`), timeoutMs);
    const metrics = { messages: 0, adds: 0, updates: 0, deletes: 0, snapshots: 0, clears: 0 };

    function buildSubscribe(symbolText) {
      const symBytes = Buffer.from(symbolText, 'utf8');
      const buf = Buffer.alloc(8 + 4 + 1 + symBytes.length);
      buf.writeUInt32LE(buf.length, 0);
      buf.writeUInt16LE(0x0001, 4);
      buf.writeUInt16LE(0, 6);
      buf.writeUInt32LE(0x0003, 8);
      buf.writeUInt8(symBytes.length, 12);
      symBytes.copy(buf, 13);
      return buf;
    }

    function sendSubscribe(reason) {
      if (subscribeSent || ws.readyState !== WebSocket.OPEN)
        return;

      subscribeSent = true;
      console.log(`[fresh-ws] subscribe (${reason}) symbol=${symbol}`);
      ws.send(buildSubscribe(symbol));
    }

    function finishSuccess() {
      if (resolved)
        return;

      resolved = true;
      clearTimeout(timeout);
      cleanup();
      const state = computeWsBookState(wsOrders);
      state.metrics = metrics;
      resolve(state);
      if (ws.readyState === WebSocket.OPEN)
        ws.close();
    }

    function finishError(message) {
      if (resolved)
        return;

      resolved = true;
      clearTimeout(timeout);
      cleanup();
      reject(new Error(message));
      if (ws.readyState === WebSocket.OPEN)
        ws.close();
    }

    function cleanup() {
      if (snapshotIdleTimer) {
        clearTimeout(snapshotIdleTimer);
        snapshotIdleTimer = null;
      }
      ws.removeAllListeners('open');
      ws.removeAllListeners('message');
      ws.removeAllListeners('error');
      ws.removeAllListeners('close');
    }

    function armSnapshotIdleTimer() {
      if (!snapshotSeenLocal || !snapshotPopulationStarted || resolved)
        return;

      if (snapshotIdleTimer)
        clearTimeout(snapshotIdleTimer);
      snapshotIdleTimer = setTimeout(() => {
        snapshotIdleTimer = null;
        finishSuccess();
      }, snapshotIdleMs);
      snapshotIdleTimer.unref?.();
    }

    function processFrame(data) {
      let offset = 0;
      while (offset + 8 <= data.length) {
        const len = data.readUInt32LE(offset);
        if (len < 8 || offset + len > data.length)
          break;

        const type = data.readUInt16LE(offset + 4);
        const headerFlags = data.readUInt16LE(offset + 6);
        if (headerFlags !== 0)
          break;

        const payload = data.subarray(offset + 8, offset + len);
        processMessage(type, payload);
        offset += len;
      }
    }

    function processMessage(type, payload) {
      metrics.messages++;
      switch (type) {
        case 0x0050: {
          const ready = payload.length > 0 && payload.readUInt8(0) === 1;
          if (ready)
            sendSubscribe('server-ready');
          break;
        }
        case 0x0010:
          break;
        case 0x0011: {
          const code = payload.readUInt8(0);
          const sLen = payload.readUInt8(1);
          const sym = payload.subarray(2, 2 + sLen).toString('utf8');
          finishError(`fresh snapshot SubscribeError code=${code} symbol=${sym}`);
          break;
        }
        case 0x0020: {
          metrics.snapshots++;
          snapshotSeenLocal = true;
          snapshotPopulationStarted = false;
          wsOrders.clear();
          break;
        }
        case 0x0030:
        case 0x0031: {
          const orderId = payload.readBigUInt64LE(8).toString();
          const price = Number(payload.readBigInt64LE(16)) / 10000;
          const qty = Number(payload.readBigInt64LE(24));
          const side = payload.readUInt8(32);
          if (type === 0x0030)
            metrics.adds++;
          else
            metrics.updates++;
          wsOrders.set(orderId, { side, price, qty });
          if (snapshotSeenLocal)
            snapshotPopulationStarted = true;
          break;
        }
        case 0x0032: {
          const orderId = payload.readBigUInt64LE(8).toString();
          wsOrders.delete(orderId);
          metrics.deletes++;
          break;
        }
        case 0x0034: {
          metrics.clears++;
          const clearSide = payload.length > 8 ? payload.readUInt8(8) : 0;
          if (clearSide === 0) {
            wsOrders.clear();
          } else {
            const side = clearSide - 1;
            for (const [orderId, order] of wsOrders) {
              if (order.side === side)
                wsOrders.delete(orderId);
            }
          }
          break;
        }
        default:
          break;
      }
    }

    ws.on('open', () => {
      setTimeout(() => sendSubscribe('open-delay'), 250).unref();
    });

    ws.on('message', data => {
      processFrame(Buffer.from(data));
      armSnapshotIdleTimer();
    });

    ws.on('error', error => finishError(error.message));
    ws.on('close', () => {
      if (!resolved)
        finishError('socket closed before fresh BookSnapshot arrived');
    });
  });
}

function computeWsBookState(wsOrders) {
  const bidMap = new Map();
  const askMap = new Map();

  for (const order of wsOrders.values()) {
    const levels = order.side === 0 ? bidMap : askMap;
    const existing = levels.get(order.price);
    if (existing) {
      existing.qty += order.qty;
      existing.count += 1;
    } else {
      levels.set(order.price, { price: order.price, qty: order.qty, count: 1 });
    }
  }

  const bids = [...bidMap.values()].sort((a, b) => b.price - a.price);
  const asks = [...askMap.values()].sort((a, b) => a.price - b.price);
  return {
    valid: true,
    bids,
    asks,
    bidOrders: bids.reduce((sum, level) => sum + level.count, 0),
    askOrders: asks.reduce((sum, level) => sum + level.count, 0),
    bidLevels: bids.length,
    askLevels: asks.length,
    bestBid: bids[0]?.price ?? 0,
    bestAsk: asks[0]?.price ?? 0,
    crossed: bids.length > 0 && asks.length > 0 && bids[0].price >= asks[0].price,
  };
}

function calculateChecksum(buffer) {
  let checksum = 0;
  for (const value of buffer)
    checksum = (checksum + value) & 0xff;
  return checksum;
}

function printSummary(reason) {
  const local = computeBookState();
  console.log(`[summary] ${reason} | symbol=${trackedSymbol ?? '(unknown)'} securityId=${trackedSecurityId ?? '?'} sessionActive=${sessionActive} lastRxMsAgo=${lastReceivedAt ? Date.now() - lastReceivedAt : 'n/a'} lastTxMsAgo=${lastSentAt ? Date.now() - lastSentAt : 'n/a'}`);
  console.log(`[summary] local=${formatLocalSummary(local)} orders=${orders.size} msgs=${counters.messagesIn} logons=${counters.logons} snaps=${counters.snapshots} incr=${counters.incrementals} bookEntries=${counters.bookEntries} trades=${counters.tradeEntries} hbIn=${counters.heartbeatsIn} hbOut=${counters.heartbeatsOut} testReq=${counters.testRequestsIn} divergences=${counters.divergences}`);
}

function clearTimers() {
  if (checkTimer) {
    clearInterval(checkTimer);
    checkTimer = null;
  }
  if (heartbeatTimer) {
    clearInterval(heartbeatTimer);
    heartbeatTimer = null;
  }
  if (runTimer) {
    clearTimeout(runTimer);
    runTimer = null;
  }
}

function formatUtcTimestamp(date) {
  const year = date.getUTCFullYear();
  const month = String(date.getUTCMonth() + 1).padStart(2, '0');
  const day = String(date.getUTCDate()).padStart(2, '0');
  const hour = String(date.getUTCHours()).padStart(2, '0');
  const minute = String(date.getUTCMinutes()).padStart(2, '0');
  const second = String(date.getUTCSeconds()).padStart(2, '0');
  const millisecond = String(date.getUTCMilliseconds()).padStart(3, '0');
  return `${year}${month}${day}-${hour}:${minute}:${second}.${millisecond}`;
}

function normalizeSymbol(value) {
  const normalized = String(value).trim().toUpperCase();
  return normalized.length === 0 ? null : normalized;
}

function toFiniteNumber(value) {
  if (value == null)
    return null;

  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function parseRequiredPort(value) {
  const parsed = Number.parseInt(value, 10);
  if (!Number.isInteger(parsed) || parsed < 1 || parsed > 65535) {
    console.error(`Invalid FIX port: ${value}`);
    process.exit(1);
  }

  return parsed;
}

function parsePositiveInt(value, fallback) {
  const parsed = Number.parseInt(value, 10);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : fallback;
}

function parseNonNegativeInt(value, fallback) {
  const parsed = Number.parseInt(value, 10);
  return Number.isInteger(parsed) && parsed >= 0 ? parsed : fallback;
}
