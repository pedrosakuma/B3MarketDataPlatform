// Web Worker: owns WebSocket, parses messages, maintains all data state.
// Sends render-ready frames to main thread — main thread never parses or computes.

import { MSG, DATA_FLAGS, buildSubscribeOrGet, buildUnsubscribe, parseMessage, flagsStr } from './protocol.js';

// ── Configuration ──
const FRAME_INTERVAL_MS = 16;
const MAX_TRADES = 50;
const MAX_BOOK_LEVELS = 15;
const MAX_RECONNECT_DELAY = 10000;

// ── State ──
const subscriptions = new Map();
const rankings = { volume: [], gainers: [], losers: [] };
const stats = { msgs: 0, books: 0, info: 0, orders: 0, trades: 0 };
let selectedId = null;
let ws = null;
let autoReconnect = true;
let reconnectAttempts = 0;
let reconnectTimer = null;
let serverReady = false;

// ── Dirty tracking ──
let dirty = 0;
const D_SUBS     = 0x01;
const D_BOOK     = 0x02;
const D_INFO     = 0x04;
const D_TRADES   = 0x08;
const D_RANKINGS = 0x10;
const D_STATS    = 0x20;
const D_TITLES   = 0x40;
function mark(flags) { dirty |= flags; }

// ── Helpers ──
function ts() {
  const d = new Date();
  return d.toTimeString().slice(0, 8) + '.' + String(d.getMilliseconds()).padStart(3, '0');
}

function secIdStr(bigint) { return bigint.toString(); }

function log(text, cssClass) {
  postMessage({ type: 'log', text, cssClass, time: ts() });
}

// ── Frame sending (interval-based, only when dirty) ──
setInterval(() => {
  if (!dirty) return;
  const d = dirty;
  dirty = 0;

  const frame = { type: 'frame' };

  if (d & D_SUBS) {
    frame.subs = [];
    for (const [id, sub] of subscriptions) {
      frame.subs.push({ id, symbol: sub.symbol, flags: sub.flags });
    }
  }

  if (d & (D_SUBS | D_TITLES)) {
    frame.selectedId = selectedId;
    const sub = selectedId ? subscriptions.get(selectedId) : null;
    frame.selectedSymbol = sub ? sub.symbol : null;
  }

  if (d & D_BOOK) {
    frame.book = computeBook();
  }

  if (d & D_INFO) {
    const sub = selectedId ? subscriptions.get(selectedId) : null;
    frame.info = (sub && Object.keys(sub.info).length > 0) ? Object.assign({}, sub.info) : null;
  }

  if (d & D_TRADES) {
    const sub = selectedId ? subscriptions.get(selectedId) : null;
    frame.trades = (sub && sub.trades && sub.trades.length > 0) ? sub.trades.slice() : null;
  }

  if (d & D_RANKINGS) {
    frame.rankings = { volume: rankings.volume, gainers: rankings.gainers, losers: rankings.losers };
  }

  if (d & D_STATS) {
    frame.stats = { msgs: stats.msgs, books: stats.books, info: stats.info, orders: stats.orders, trades: stats.trades };
  }

  postMessage(frame);
}, FRAME_INTERVAL_MS);

// ── MBP computation (runs in worker, off main thread) ──
function computeBook() {
  const sub = selectedId ? subscriptions.get(selectedId) : null;
  if (!sub || sub.orders.size === 0) return null;

  const bidMap = new Map();
  const askMap = new Map();
  for (const [, order] of sub.orders) {
    const map = order.side === 0 ? bidMap : askMap;
    const existing = map.get(order.price);
    if (existing) { existing.qty += order.qty; existing.count++; }
    else map.set(order.price, { price: order.price, qty: order.qty, count: 1 });
  }

  const bids = [...bidMap.values()].sort((a, b) => b.price - a.price).slice(0, MAX_BOOK_LEVELS);
  const asks = [...askMap.values()].sort((a, b) => a.price - b.price).slice(0, MAX_BOOK_LEVELS);

  let maxQty = 1;
  for (const b of bids) if (b.qty > maxQty) maxQty = b.qty;
  for (const a of asks) if (a.qty > maxQty) maxQty = a.qty;

  return {
    bids, asks, maxQty,
    totalBids: bidMap.size, totalAsks: askMap.size,
    totalOrders: sub.orders.size, orderCount: sub.orderCount,
  };
}

// ── Trade helper ──
function addTrade(sub, price, qty) {
  if (!sub.trades) sub.trades = [];
  const prev = sub.trades.length > 0 ? sub.trades[sub.trades.length - 1].price : price;
  const direction = price > prev ? 'up' : price < prev ? 'down' : 'flat';
  sub.trades.push({ time: ts(), price, qty, direction });
  if (sub.trades.length > MAX_TRADES) sub.trades.shift();
}

// ── WebSocket ──
let wsUrl = '';

function connect(url) {
  wsUrl = url || wsUrl;
  if (!wsUrl) return;
  postMessage({ type: 'status', status: 'connecting' });

  ws = new WebSocket(wsUrl);
  ws.binaryType = 'arraybuffer';

  const connectTimeout = setTimeout(() => {
    if (ws && ws.readyState === WebSocket.CONNECTING) {
      ws.close();
      log('Connection timeout', 'log-error');
    }
  }, 5000);

  ws.onopen = () => {
    clearTimeout(connectTimeout);
    reconnectAttempts = 0;
    postMessage({ type: 'status', status: 'connected' });
    log('Connected to ' + wsUrl, 'log-sub-ok');
  };

  ws.onclose = () => {
    clearTimeout(connectTimeout);
    serverReady = false;
    postMessage({ type: 'status', status: 'disconnected' });
    log('Disconnected', 'log-error');
    rankings.volume = []; rankings.gainers = []; rankings.losers = [];
    mark(D_RANKINGS);
    scheduleReconnect();
  };

  ws.onerror = () => log('WebSocket error', 'log-error');

  ws.onmessage = (evt) => {
    if (!(evt.data instanceof ArrayBuffer)) return;
    const buf = evt.data;
    const view = new DataView(buf);
    let offset = 0;
    while (offset + 4 <= buf.byteLength) {
      const len = view.getUint16(offset, true);
      if (len < 4 || offset + len > buf.byteLength) break;
      stats.msgs++;
      const msg = parseMessage(buf, offset, len);
      if (msg) handleMessage(msg);
      offset += len;
    }
    mark(D_STATS);
  };
}

function scheduleReconnect() {
  if (!autoReconnect) return;
  reconnectAttempts++;
  const delay = Math.min(1000 * Math.pow(1.5, reconnectAttempts - 1), MAX_RECONNECT_DELAY);
  log('Reconnecting in ' + (delay / 1000).toFixed(1) + 's (attempt ' + reconnectAttempts + ')...', 'log-info');
  reconnectTimer = setTimeout(() => {
    if (!ws || ws.readyState >= WebSocket.CLOSING) connect();
  }, delay);
}

function resubscribeAll() {
  if (!ws || ws.readyState !== WebSocket.OPEN || subscriptions.size === 0) return;
  log('Resubscribing ' + subscriptions.size + ' symbol(s)...', 'log-info');
  for (const [, sub] of subscriptions) {
    ws.send(buildSubscribeOrGet(MSG.SUBSCRIBE, sub.symbol, sub.flags));
    log('\u2192 Resubscribe ' + sub.symbol + ' [' + flagsStr(sub.flags) + ']', '');
  }
}

// ── Message handler ──
function handleMessage(msg) {
  const sel = selectedId;

  switch (msg.type) {
    case 'SubscribeOk': {
      const id = secIdStr(msg.securityId);
      subscriptions.set(id, {
        symbol: msg.symbol, flags: msg.flags, securityId: msg.securityId,
        orders: new Map(), info: {}, trades: [], orderCount: 0, tradeCount: 0,
      });
      let d = D_SUBS;
      if (!sel) { selectedId = id; d |= D_BOOK | D_INFO | D_TRADES | D_TITLES; }
      mark(d);
      log('SubscribeOk ' + msg.symbol + ' id=' + id + ' [' + flagsStr(msg.flags) + ']', 'log-sub-ok');
      break;
    }
    case 'SubscribeError':
      log('SubscribeError ' + msg.symbol + ': ' + msg.errorName, 'log-error');
      break;
    case 'Unsubscribed': {
      const id = secIdStr(msg.securityId);
      const sub = subscriptions.get(id);
      if (sub) {
        subscriptions.delete(id);
        let d = D_SUBS;
        if (sel === id) {
          selectedId = subscriptions.size > 0 ? subscriptions.keys().next().value : null;
          d |= D_BOOK | D_INFO | D_TRADES | D_TITLES;
        }
        mark(d);
      }
      log('Unsubscribed ' + (sub ? sub.symbol : id), 'log-error');
      break;
    }
    case 'BookSnapshot': {
      stats.books++;
      const id = secIdStr(msg.securityId);
      const sub = subscriptions.get(id);
      if (sub) sub.orders = new Map();
      if (sel === id) mark(D_BOOK);
      break;
    }
    case 'InfoSnapshot': {
      stats.info++;
      const id = secIdStr(msg.securityId);
      const sub = subscriptions.get(id);
      if (sub) Object.assign(sub.info, msg.fields);
      if (sel === id) mark(D_INFO);
      break;
    }
    case 'OrderAdded':
    case 'OrderUpdated': {
      stats.orders++;
      const id = secIdStr(msg.securityId);
      const sub = subscriptions.get(id);
      if (sub) {
        sub.orderCount++;
        sub.orders.set(msg.orderId, { side: msg.side, price: msg.price, qty: msg.qty });
      }
      if (sel === id) mark(D_BOOK);
      break;
    }
    case 'OrderDeleted': {
      stats.orders++;
      const id = secIdStr(msg.securityId);
      const sub = subscriptions.get(id);
      if (sub) {
        sub.orderCount++;
        sub.orders.delete(msg.orderId);
      }
      if (sel === id) mark(D_BOOK);
      break;
    }
    case 'Trade': {
      stats.trades++;
      const id = secIdStr(msg.securityId);
      const sub = subscriptions.get(id);
      if (sub) {
        sub.tradeCount++;
        addTrade(sub, msg.price, msg.qty);
      }
      if (sel === id) mark(D_TRADES);
      break;
    }
    case 'BookCleared': {
      const id = secIdStr(msg.securityId);
      const sub = subscriptions.get(id);
      if (sub) {
        if (msg.side === 0) sub.orders = new Map();
        else {
          const orderSide = msg.side - 1;
          for (const [oid, order] of sub.orders) {
            if (order.side === orderSide) sub.orders.delete(oid);
          }
        }
      }
      if (sel === id) mark(D_BOOK);
      const sideNames = ['Both', 'Bid', 'Ask'];
      log('BookCleared ' + (sub ? sub.symbol : id) + ' (' + (sideNames[msg.side] || 'Both') + ')', 'log-book');
      break;
    }
    case 'RankingsUpdate': {
      rankings.volume = msg.volume;
      rankings.gainers = msg.gainers;
      rankings.losers = msg.losers;
      mark(D_RANKINGS);
      break;
    }
    case 'ServerStatus': {
      const wasReady = serverReady;
      serverReady = msg.ready;
      postMessage({ type: 'serverReady', ready: msg.ready });
      log('Server status: ' + (msg.ready ? 'ready' : 'initializing'), msg.ready ? 'log-sub-ok' : 'log-info');
      if (msg.ready && !wasReady)
        resubscribeAll();
      break;
    }
  }
}

// ── Commands from main thread ──
self.onmessage = (evt) => {
  const msg = evt.data;
  switch (msg.cmd) {
    case 'connect':
      autoReconnect = true;
      connect(msg.url);
      break;
    case 'disconnect':
      if (reconnectTimer) { clearTimeout(reconnectTimer); reconnectTimer = null; }
      if (ws && ws.readyState <= WebSocket.OPEN) {
        autoReconnect = false;
        ws.close();
        autoReconnect = true;
      }
      break;
    case 'subscribe':
      if (ws && ws.readyState === WebSocket.OPEN) {
        ws.send(buildSubscribeOrGet(MSG.SUBSCRIBE, msg.symbol, msg.flags));
        log('\u2192 Subscribe ' + msg.symbol + ' [' + flagsStr(msg.flags) + ']', '');
      }
      break;
    case 'get':
      if (ws && ws.readyState === WebSocket.OPEN) {
        ws.send(buildSubscribeOrGet(MSG.GET, msg.symbol, msg.flags));
        log('\u2192 Get ' + msg.symbol + ' [' + flagsStr(msg.flags) + ']', '');
      }
      break;
    case 'unsubscribe':
      if (ws && ws.readyState === WebSocket.OPEN) {
        ws.send(buildUnsubscribe(BigInt(msg.securityId)));
        const sub = subscriptions.get(msg.securityId);
        subscriptions.delete(msg.securityId);
        if (selectedId === msg.securityId) {
          selectedId = subscriptions.size > 0 ? subscriptions.keys().next().value : null;
        }
        mark(D_SUBS | D_BOOK | D_INFO | D_TRADES | D_TITLES);
        log('\u2192 Unsubscribe ' + (sub ? sub.symbol : msg.securityId), '');
      }
      break;
    case 'select':
      selectedId = msg.securityId;
      mark(D_SUBS | D_BOOK | D_INFO | D_TRADES | D_TITLES);
      break;
    case 'rankingSubscribe':
      if (ws && ws.readyState === WebSocket.OPEN) {
        ws.send(buildSubscribeOrGet(MSG.SUBSCRIBE, msg.symbol, DATA_FLAGS.ALL));
        log('\u2192 Subscribe ' + msg.symbol + ' [Book+Info] (from ranking)', '');
      }
      break;
    case 'setAutoReconnect':
      autoReconnect = msg.value;
      break;
  }
};
