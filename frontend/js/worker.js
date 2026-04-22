// Web Worker: owns WebSocket, parses messages, maintains all data state.
// Sends render-ready frames to main thread — main thread never parses or computes.

import { MSG, DATA_FLAGS, buildSubscribeOrGet, buildUnsubscribe, parseMessage, flagsStr } from './protocol.js';

// ── Configuration ──
const FRAME_INTERVAL_MS = 16;
let bookDepth = 15;
const MAX_RECONNECT_DELAY = 10000;
const ALLINFO_THROTTLE_MS = 200;

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
let manualDisconnect = false;

// ── Dirty tracking ──
let dirty = 0;
const D_SUBS     = 0x01;
const D_BOOK     = 0x02;
const D_CHART    = 0x08;
const D_RANKINGS = 0x10;
const D_STATS    = 0x20;
const D_TITLES   = 0x40;
const D_ALLINFO  = 0x80;
const D_TRADES   = 0x100;
const D_AUCTION  = 0x200;
const D_OVERLAYS = 0x400;
let chartFullSwap = false;
function mark(flags) { dirty |= flags; }

// Throttled allInfo — avoids sending table data on every InfoSnapshot
let allInfoPending = false;
function scheduleAllInfo() {
  if (allInfoPending) return;
  allInfoPending = true;
  setTimeout(() => { allInfoPending = false; mark(D_ALLINFO); }, ALLINFO_THROTTLE_MS);
}

// ── Helpers ──
const _secIdCache = new Map();
function secIdStr(bigint) {
  let s = _secIdCache.get(bigint);
  if (s === undefined) {
    s = bigint.toString();
    _secIdCache.set(bigint, s);
  }
  return s;
}

function log(text, cssClass) {
  postMessage({ type: 'log', text, cssClass });
}

// ── Display resolution for chart (user-configurable, default auto) ──
let displayResolution = 0; // 0 = auto, otherwise seconds (1, 5, 15, 60, ...)
let lastSentResolution = 0; // tracks resolution sent to chart for detecting changes
let lastSentCandleTime = 0; // tracks last candle time sent to chart for gap-fill
let lastSentCandleClose = 0; // tracks last candle close sent to chart for gap-fill price

const AUTO_RES_THRESHOLDS = [
  { maxSpan: 600, resolution: 1 },    // up to 10 min → 1s
  { maxSpan: 3600, resolution: 5 },   // up to 1 hour → 5s
  { maxSpan: 7200, resolution: 15 },  // up to 2 hours → 15s
  { maxSpan: 14400, resolution: 60 }, // up to 4 hours → 1m
];
const MAX_CANDLES = 10 * 60 * 60; // 10h retention at 1s resolution

function pickAutoResolution(candles) {
  if (candles.length < 2) return 1;
  const span = candles[candles.length - 1].time - candles[0].time;
  for (const t of AUTO_RES_THRESHOLDS) {
    if (span <= t.maxSpan) return t.resolution;
  }
  return 300; // 5 min fallback
}

const MAX_GAP_FILL = 600; // max gap-fill candles per gap (covers ~10 min at 1s)
const MAX_RECENT_TRADES = 50; // trade tape length per subscription

// AggressorSide values exposed to the UI:
//   0 = unknown (mid-price or no top-of-book yet)
//   1 = Buy aggressor (trade lifted the ask)
//   2 = Sell aggressor (trade hit the bid)
function inferAggressor(sub, tradePrice) {
  const bid = bestOfSide(sub, 0);
  const ask = bestOfSide(sub, 1);
  if (ask !== null && tradePrice >= ask) return 1;
  if (bid !== null && tradePrice <= bid) return 2;
  return 0;
}

function aggregateCandles(candles1s, resolution) {
  if (candles1s.length === 0) return [];
  const res = Math.max(resolution, 1);

  if (res <= 1) {
    // 1s: fill time gaps and enforce open=prev.close continuity
    const first = { ...candles1s[0] };
    const result = [first];
    for (let i = 1; i < candles1s.length; i++) {
      const prev = result[result.length - 1];
      const gap = candles1s[i].time - prev.time;
      if (gap > 1) {
        const n = Math.min(gap - 1, MAX_GAP_FILL);
        const p = prev.close;
        for (let j = 1; j <= n; j++) {
          result.push({ time: prev.time + j, open: p, high: p, low: p, close: p, volume: 0, avg: p });
        }
      }
      const c = candles1s[i];
      const prevClose = result[result.length - 1].close;
      result.push({
        time: c.time, open: prevClose,
        high: Math.max(prevClose, c.high), low: Math.min(prevClose, c.low),
        close: c.close, volume: c.volume, avg: c.avg,
      });
    }
    return result;
  }

  // Higher resolutions: aggregate into buckets and fill gaps between them.
  // Avg is session-cumulative VWAP, so the bucket's representative value is the
  // LAST 1s candle's Avg in that bucket (latest session VWAP at bucket end).
  const result = [];
  let cur = null;
  for (const c of candles1s) {
    const bucket = Math.floor(c.time / res) * res;
    if (cur && cur.time === bucket) {
      if (c.high > cur.high) cur.high = c.high;
      if (c.low < cur.low) cur.low = c.low;
      cur.close = c.close;
      cur.volume += c.volume;
      cur.avg = c.avg;
    } else {
      if (cur) {
        result.push(cur);
        let gapBucket = cur.time + res;
        let gapCount = 0;
        const p = cur.close;
        const a = cur.avg;
        while (gapBucket < bucket && gapCount < MAX_GAP_FILL) {
          result.push({ time: gapBucket, open: p, high: p, low: p, close: p, volume: 0, avg: a });
          gapBucket += res;
          gapCount++;
        }
      }
      const newOpen = cur ? cur.close : c.open;
      cur = {
        time: bucket, open: newOpen,
        high: Math.max(newOpen, c.high), low: Math.min(newOpen, c.low),
        close: c.close, volume: c.volume, avg: c.avg,
      };
    }
  }
  if (cur) result.push(cur);
  return result;
}

function getChartCandles(sub) {
  const raw = sub.candles;
  if (raw.length === 0) return { candles: [], resolution: 1 };
  const res = displayResolution > 0 ? displayResolution : pickAutoResolution(raw);
  const candles = aggregateCandles(raw, res);
  return { candles, resolution: res };
}

function trimCandles(sub) {
  const excess = sub.candles.length - MAX_CANDLES;
  if (excess <= 0) return false;
  sub.candles.splice(0, excess);
  return true;
}

/// Compute just the last aggregated candle by scanning 1s candles in the current bucket.
/// Returns null if no candles exist.
function getLastAggregatedCandle(sub, resolution) {
  const raw = sub.candles;
  if (raw.length === 0) return null;
  if (resolution <= 1) return { ...raw[raw.length - 1] };

  const lastTime = raw[raw.length - 1].time;
  const bucket = Math.floor(lastTime / resolution) * resolution;

  // Find the first 1s candle in this bucket
  let i = raw.length - 1;
  while (i > 0 && Math.floor(raw[i - 1].time / resolution) * resolution === bucket) i--;

  // For the open: we need the close of the previous bucket's last candle
  const prevClose = i > 0 ? raw[i - 1].close : raw[i].open;

  let high = prevClose, low = prevClose, close = prevClose, volume = 0, sumAvgVol = 0;
  for (let j = i; j < raw.length; j++) {
    const c = raw[j];
    if (c.high > high) high = c.high;
    if (c.low < low) low = c.low;
    close = c.close;
    volume += c.volume;
    sumAvgVol += c.avg * c.volume;
  }
  const avg = volume > 0 ? sumAvgVol / volume : close;
  return { time: bucket, open: prevClose, high, low, close, volume, avg };
}

// ── Frame sending (interval-based, only when dirty) ──
setInterval(() => {
  if (!dirty) return;
  const d = dirty;
  dirty = 0;
  const fullSwap = chartFullSwap;
  chartFullSwap = false;

  const frame = { type: 'frame' };

  if (d & (D_SUBS | D_TITLES)) {
    frame.selectedId = selectedId;
    const sub = selectedId ? subscriptions.get(selectedId) : null;
    frame.selectedSymbol = sub ? sub.symbol : null;
  }

  if (d & D_BOOK) {
    frame.book = computeBook();
  }

  if (d & D_CHART) {
    const sub = selectedId ? subscriptions.get(selectedId) : null;
    if (!sub || !sub.snapshotReceived || sub.candles.length === 0) {
      frame.chart = null;
      lastSentResolution = 0;
      lastSentCandleTime = 0;
      lastSentCandleClose = 0;
    } else {
      // Compute desired resolution consistently with getChartCandles
      const desiredRes = displayResolution > 0 ? displayResolution : pickAutoResolution(sub.candles);
      const resChanged = desiredRes !== lastSentResolution;

      if (fullSwap || resChanged) {
        // Full swap: snapshot arrival, symbol change, or resolution change
        const { candles, resolution } = getChartCandles(sub);
        frame.chart = { full: true, scroll: true, candles, resolution };
        lastSentResolution = resolution;
        if (candles.length > 0) {
          const last = candles[candles.length - 1];
          lastSentCandleTime = last.time;
          lastSentCandleClose = last.close;
        } else {
          lastSentCandleTime = 0;
          lastSentCandleClose = 0;
        }
      } else {
        // Incremental update — use lastSentResolution (consistent with last full swap)
        const res = lastSentResolution;
        if (res <= 1 && sub.currentCandle) {
          // 1s incremental update — include gap-fill candles if there's a time gap
          const curTime = sub.currentCandle.time;
          if (lastSentCandleTime > 0 && curTime > lastSentCandleTime + 1) {
            const gap = curTime - lastSentCandleTime;
            const n = Math.min(gap - 1, MAX_GAP_FILL);
            const p = lastSentCandleClose;
            const updates = [];
            for (let j = 1; j <= n; j++) {
              updates.push({ time: lastSentCandleTime + j, open: p, high: p, low: p, close: p, volume: 0, avg: p });
            }
            updates.push({ ...sub.currentCandle });
            frame.chart = { full: false, updates, resolution: res };
          } else {
            frame.chart = { full: false, update: { ...sub.currentCandle }, resolution: res };
          }
          lastSentCandleTime = curTime;
          lastSentCandleClose = sub.currentCandle.close;
        } else if (res > 1) {
          // Aggregated resolution: send incremental update for just the last bucket
          const lastCandle = getLastAggregatedCandle(sub, res);
          if (lastCandle) {
            if (lastSentCandleTime > 0 && lastCandle.time > lastSentCandleTime + res) {
              const p = lastSentCandleClose;
              const updates = [];
              let gapBucket = lastSentCandleTime + res;
              let gapCount = 0;
              while (gapBucket < lastCandle.time && gapCount < MAX_GAP_FILL) {
                updates.push({ time: gapBucket, open: p, high: p, low: p, close: p, volume: 0, avg: p });
                gapBucket += res;
                gapCount++;
              }
              updates.push(lastCandle);
              frame.chart = { full: false, updates, resolution: res };
            } else {
              frame.chart = { full: false, update: lastCandle, resolution: res };
            }
            lastSentCandleTime = lastCandle.time;
            lastSentCandleClose = lastCandle.close;
          }
        }
      }
    }
  }

  if (d & D_OVERLAYS) {
    const sub = selectedId ? subscriptions.get(selectedId) : null;
    if (!sub) {
      frame.overlays = null;
    } else {
      frame.overlays = {
        priceBandLow: sub.info.PriceBandLow ?? null,
        priceBandHigh: sub.info.PriceBandHigh ?? null,
      };
    }
  }

  if (d & D_TRADES) {
    const sub = selectedId ? subscriptions.get(selectedId) : null;
    frame.trades = sub && sub.recentTrades.length > 0
      ? { items: sub.recentTrades.slice() }
      : { items: [] };
  }

  if (d & D_AUCTION) {
    const sub = selectedId ? subscriptions.get(selectedId) : null;
    if (!sub) {
      frame.auction = null;
    } else {
      const ts = sub.info.TradingStatus;
      const isAuction = ts === 2 || ts === 5 || ts === 8 || ts === 21;
      if (!isAuction) {
        frame.auction = null;
      } else {
        frame.auction = {
          tradingStatus: ts,
          theoreticalPrice: sub.info.TheoreticalOpeningPrice ?? null,
          theoreticalSize: sub.info.TheoreticalOpeningSize ?? null,
          imbalance: sub.info.AuctionImbalanceSize ?? null,
        };
      }
    }
  }

  if (d & (D_ALLINFO | D_SUBS)) {
    frame.allInfo = [];
    for (const [id, sub] of subscriptions) {
      frame.allInfo.push({
        id, symbol: sub.symbol, flags: sub.flags,
        info: { ...sub.info },
      });
    }
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
// Incremental price-level helpers. Maintained by order-event handlers so
// computeBook() avoids iterating all orders (often 100k+) every render frame.
// Also maintain cached best-of-side prices so inferAggressor() (called per
// trade) doesn't have to scan all open orders.
function levelAdd(sub, side, price, qty) {
  const levels = side === 0 ? sub.bidLevels : sub.askLevels;
  const lvl = levels.get(price);
  if (lvl) { lvl.qty += qty; lvl.count++; }
  else levels.set(price, { price, qty, count: 1 });
  if (side === 0) {
    if (sub.bestBid === null || price > sub.bestBid) sub.bestBid = price;
  } else {
    if (sub.bestAsk === null || price < sub.bestAsk) sub.bestAsk = price;
  }
}

function levelRemove(sub, side, price, qty) {
  const levels = side === 0 ? sub.bidLevels : sub.askLevels;
  const lvl = levels.get(price);
  if (!lvl) return;
  lvl.qty -= qty;
  lvl.count--;
  if (lvl.count <= 0 || lvl.qty <= 0) {
    levels.delete(price);
    // Best-of-side may need recomputation if we just removed the best level.
    // null sentinel triggers lazy recompute on next read in bestOfSide().
    if (side === 0 && price === sub.bestBid) sub.bestBid = null;
    else if (side === 1 && price === sub.bestAsk) sub.bestAsk = null;
  }
}

function bestOfSide(sub, side) {
  if (side === 0) {
    if (sub.bestBid !== null) return sub.bestBid;
    let max = -Infinity;
    for (const p of sub.bidLevels.keys()) if (p > max) max = p;
    sub.bestBid = max === -Infinity ? null : max;
    return sub.bestBid;
  } else {
    if (sub.bestAsk !== null) return sub.bestAsk;
    let min = Infinity;
    for (const p of sub.askLevels.keys()) if (p < min) min = p;
    sub.bestAsk = min === Infinity ? null : min;
    return sub.bestAsk;
  }
}

function computeBook() {
  const sub = selectedId ? subscriptions.get(selectedId) : null;
  if (!sub || sub.orders.size === 0) return null;

  const bids = [...sub.bidLevels.values()].sort((a, b) => b.price - a.price).slice(0, bookDepth);
  const asks = [...sub.askLevels.values()].sort((a, b) => a.price - b.price).slice(0, bookDepth);

  let maxQty = 1;
  for (const b of bids) if (b.qty > maxQty) maxQty = b.qty;
  for (const a of asks) if (a.qty > maxQty) maxQty = a.qty;

  return {
    bids, asks, maxQty,
    totalBids: sub.bidLevels.size, totalAsks: sub.askLevels.size,
    totalOrders: sub.orders.size, orderCount: sub.orderCount,
  };
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
    if (!manualDisconnect) scheduleReconnect();
    manualDisconnect = false;
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
    scheduleStats();
  };
}

// Stats are throttled separately so they don't drag the whole render loop
// to 60Hz when nothing else is dirty. The stats counter row updates at most
// every STATS_THROTTLE_MS.
const STATS_THROTTLE_MS = 250;
let statsPending = false;
function scheduleStats() {
  if (statsPending) return;
  statsPending = true;
  setTimeout(() => { statsPending = false; mark(D_STATS); }, STATS_THROTTLE_MS);
}

  if (!autoReconnect) return;
  reconnectAttempts++;
  const delay = Math.min(1000 * Math.pow(1.5, reconnectAttempts - 1), MAX_RECONNECT_DELAY);
  log('Reconnecting in ' + (delay / 1000).toFixed(1) + 's...', 'log-info');
  reconnectTimer = setTimeout(() => {
    if (!ws || ws.readyState >= WebSocket.CLOSING) connect();
  }, delay);
}

function resubscribeAll() {
  if (!ws || ws.readyState !== WebSocket.OPEN || subscriptions.size === 0) return;
  log('Resubscribing ' + subscriptions.size + ' symbol(s)...', 'log-info');
  for (const [, sub] of subscriptions) {
    ws.send(buildSubscribeOrGet(MSG.SUBSCRIBE, sub.symbol, sub.flags));
  }
}

// ── Message handler ──
function handleMessage(msg) {
  const sel = selectedId;

  switch (msg.type) {
    case 'SubscribeOk': {
      const id = secIdStr(msg.securityId);
      const existing = subscriptions.get(id);
      if (existing) {
        existing.symbol = msg.symbol;
        existing.flags = msg.flags;
        existing.securityId = msg.securityId;
      } else {
        subscriptions.set(id, {
          symbol: msg.symbol, flags: msg.flags, securityId: msg.securityId,
          orders: new Map(),
          // Aggregated price levels — maintained incrementally on each order event
          // so computeBook() avoids a full O(N) scan over all orders every render frame.
          bidLevels: new Map(), askLevels: new Map(),
          bestBid: null, bestAsk: null,
          info: {}, candles: [], currentCandle: null,
          orderCount: 0, tradeCount: 0, candleResolution: 1, snapshotReceived: false,
          recentTrades: [],   // [{time, price, qty, side, tradeId}] — newest last; capped MAX_RECENT_TRADES
        });
      }
      let d = D_SUBS | D_ALLINFO;
      if (!sel) {
        selectedId = id;
        d |= D_BOOK | D_CHART | D_TITLES | D_TRADES | D_AUCTION | D_OVERLAYS;
      } else if (sel === id) {
        d |= existing ? D_TITLES : D_BOOK | D_CHART | D_TITLES | D_TRADES | D_AUCTION | D_OVERLAYS;
      }
      mark(d);
      log((existing ? 'Subscription refreshed ' : 'Subscribed ') + msg.symbol + ' [' + flagsStr(msg.flags) + ']', 'log-sub-ok');
      break;
    }
    case 'SubscribeError':
      log('Subscribe error: ' + msg.symbol + ' \u2014 ' + msg.errorName, 'log-error');
      break;
    case 'Unsubscribed': {
      const id = secIdStr(msg.securityId);
      const sub = subscriptions.get(id);
      if (sub) {
        subscriptions.delete(id);
        let d = D_SUBS | D_ALLINFO;
        if (sel === id) {
          selectedId = subscriptions.size > 0 ? subscriptions.keys().next().value : null;
          d |= D_BOOK | D_CHART | D_TITLES | D_TRADES | D_AUCTION | D_OVERLAYS;
          chartFullSwap = true;
        }
        mark(d);
      }
      log('Unsubscribed ' + (sub ? sub.symbol : id), 'log-info');
      break;
    }
    case 'BookSnapshot': {
      stats.books++;
      const id = secIdStr(msg.securityId);
      const sub = subscriptions.get(id);
      if (sub) {
        sub.orders = new Map();
        sub.bidLevels.clear();
        sub.askLevels.clear();
        sub.bestBid = null;
        sub.bestAsk = null;
      }
      if (sel === id) mark(D_BOOK);
      break;
    }
    case 'InfoSnapshot': {
      stats.info++;
      const id = secIdStr(msg.securityId);
      const sub = subscriptions.get(id);
      if (sub) {
        Object.assign(sub.info, msg.fields);
        if (sel === id) {
          if ('PriceBandLow' in msg.fields || 'PriceBandHigh' in msg.fields) mark(D_OVERLAYS);
          if ('TradingStatus' in msg.fields || 'TheoreticalOpeningPrice' in msg.fields
              || 'TheoreticalOpeningSize' in msg.fields || 'AuctionImbalanceSize' in msg.fields) {
            mark(D_AUCTION);
          }
        }
      }
      scheduleAllInfo();
      break;
    }
    case 'OrderAdded':
    case 'OrderUpdated': {
      stats.orders++;
      const id = secIdStr(msg.securityId);
      const sub = subscriptions.get(id);
      if (sub) {
        sub.orderCount++;
        const prev = sub.orders.get(msg.orderId);
        if (prev) levelRemove(sub, prev.side, prev.price, prev.qty);
        sub.orders.set(msg.orderId, { side: msg.side, price: msg.price, qty: msg.qty });
        levelAdd(sub, msg.side, msg.price, msg.qty);
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
        const prev = sub.orders.get(msg.orderId);
        if (prev) {
          levelRemove(sub, prev.side, prev.price, prev.qty);
          sub.orders.delete(msg.orderId);
        }
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
        const side = inferAggressor(sub, msg.price);
        sub.recentTrades.push({
          time: Date.now(), price: msg.price, qty: msg.qty,
          side, tradeId: msg.tradeId,
        });
        if (sub.recentTrades.length > MAX_RECENT_TRADES) {
          sub.recentTrades.splice(0, sub.recentTrades.length - MAX_RECENT_TRADES);
        }
        sub.tradesDirty = true;
        if (sel === id) mark(D_TRADES);
      }
      break;
    }
    case 'BookCleared': {
      const id = secIdStr(msg.securityId);
      const sub = subscriptions.get(id);
      if (sub) {
        if (msg.side === 0) {
          sub.orders = new Map();
          sub.bidLevels.clear();
          sub.askLevels.clear();
          sub.bestBid = null;
          sub.bestAsk = null;
        } else {
          const orderSide = msg.side - 1;
          for (const [oid, order] of sub.orders) {
            if (order.side === orderSide) sub.orders.delete(oid);
          }
          (orderSide === 0 ? sub.bidLevels : sub.askLevels).clear();
          if (orderSide === 0) sub.bestBid = null;
          else sub.bestAsk = null;
        }
      }
      if (sel === id) mark(D_BOOK);
      break;
    }
    case 'CandleSnapshot': {
      const id = secIdStr(msg.securityId);
      const sub = subscriptions.get(id);
      if (sub) {
        if (msg.isFirst) {
          sub.candles = msg.candles.slice(-MAX_CANDLES);
          sub.snapshotReceived = false;
        } else {
          // Continuation batch — append
          for (const c of msg.candles) sub.candles.push(c);
          trimCandles(sub);
        }
        sub.currentCandle = sub.candles.length > 0 ? sub.candles[sub.candles.length - 1] : null;
        sub.candleResolution = msg.resolution;
        sub.snapshotReceived = msg.isLast;
      }
      // Full chart swap only after the final snapshot batch, so the chart never renders partial history.
      if (sel === id && msg.isLast) { chartFullSwap = true; mark(D_CHART); }
      break;
    }
    case 'CandleUpdate': {
      const id = secIdStr(msg.securityId);
      const sub = subscriptions.get(id);
      let trimmed = false;
      if (sub) {
        const c = msg.candle;
        const last = sub.currentCandle;
        if (last && last.time === c.time) {
          last.open = c.open; last.high = c.high;
          last.low = c.low; last.close = c.close; last.volume = c.volume; last.avg = c.avg;
        } else {
          sub.candles.push(c);
          trimmed = trimCandles(sub);
          sub.currentCandle = c;
        }
        sub.candleResolution = msg.resolution;
      }
      // Only render chart updates after snapshot has arrived
      if (sel === id && sub && sub.snapshotReceived) {
        if (trimmed) chartFullSwap = true;
        mark(D_CHART);
      }
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
      log('Server ' + (msg.ready ? 'ready' : 'initializing'), msg.ready ? 'log-sub-ok' : 'log-info');
      if (msg.ready && !wasReady) resubscribeAll();
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
        manualDisconnect = true;
        ws.close();
      }
      break;
    case 'subscribe':
      if (ws && ws.readyState === WebSocket.OPEN) {
        try { ws.send(buildSubscribeOrGet(MSG.SUBSCRIBE, msg.symbol, msg.flags)); }
        catch (e) { log('Subscribe send error: ' + e.message, 'log-error'); }
      } else {
        log('Cannot subscribe: not connected', 'log-error');
      }
      break;
    case 'get':
      if (ws && ws.readyState === WebSocket.OPEN) {
        try { ws.send(buildSubscribeOrGet(MSG.GET, msg.symbol, msg.flags)); }
        catch (e) { log('Get send error: ' + e.message, 'log-error'); }
      } else {
        log('Cannot get: not connected', 'log-error');
      }
      break;
    case 'unsubscribe':
      if (ws && ws.readyState === WebSocket.OPEN) {
        try { ws.send(buildUnsubscribe(BigInt(msg.securityId))); }
        catch (e) { log('Unsubscribe send error: ' + e.message, 'log-error'); }
        // State cleanup happens when server confirms with 'Unsubscribed'
      } else {
        log('Cannot unsubscribe: not connected', 'log-error');
      }
      break;
    case 'select':
      selectedId = msg.securityId;
      chartFullSwap = true;
      mark(D_BOOK | D_CHART | D_TITLES | D_TRADES | D_AUCTION | D_OVERLAYS);
      break;
    case 'rankingSubscribe':
      if (ws && ws.readyState === WebSocket.OPEN)
        ws.send(buildSubscribeOrGet(MSG.SUBSCRIBE, msg.symbol, DATA_FLAGS.ALL));
      break;
    case 'setAutoReconnect':
      autoReconnect = msg.value;
      break;
    case 'setResolution':
      displayResolution = msg.value;
      if (selectedId) { chartFullSwap = true; mark(D_CHART); }
      break;
    case 'setBookDepth':
      bookDepth = msg.value;
      mark(D_BOOK);
      break;
  }
};
