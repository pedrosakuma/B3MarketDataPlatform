// Application entry point — WebSocket connection, message handling, actions.
// Uses a single rAF dirty-flag render loop for all UI regions.

import { MSG, DATA_FLAGS, buildSubscribeOrGet, buildUnsubscribe, parseMessage, flagsStr } from './protocol.js';
import { state, subscriptions, rankings, stats } from './state.js';
import {
  $, secIdStr, setStatus, renderSubList, updateTitles, renderBook, renderInfo, renderTrades,
  renderHealth, renderRankings, addLog, clearLog, setLogEnabled, updateStats, addTrade,
} from './ui.js';

// ── Dirty-flag render loop (single rAF for all UI regions) ──
let _dirty = 0;
let _rafPending = false;
const D_BOOK     = 0x01;
const D_INFO     = 0x02;
const D_TRADES   = 0x04;
const D_STATS    = 0x08;
const D_SUBLIST  = 0x10;
const D_RANKINGS = 0x20;
const D_TITLES   = 0x40;

function markDirty(flags) {
  _dirty |= flags;
  if (!_rafPending) {
    _rafPending = true;
    requestAnimationFrame(flushRender);
  }
}

function flushRender() {
  _rafPending = false;
  const d = _dirty;
  _dirty = 0;
  if (d & D_SUBLIST) renderSubList();
  if (d & D_TITLES) updateTitles();
  if (d & D_BOOK) renderBook();
  if (d & D_INFO) renderInfo();
  if (d & D_TRADES) renderTrades();
  if (d & D_STATS) updateStats();
  if (d & D_RANKINGS) renderRankings();
}

// ── Helpers ──

function getFlags() {
  let f = 0;
  if ($('flagBook').checked) f |= DATA_FLAGS.BOOK;
  if ($('flagInfo').checked) f |= DATA_FLAGS.INFO;
  return f || DATA_FLAGS.ALL;
}

// ── Health polling ──

let healthInterval = null;

function startHealthPolling() {
  if (healthInterval) return;
  pollHealth();
  healthInterval = setInterval(pollHealth, 5000);
}

function httpBase() {
  return $('wsUrl').value.trim().replace(/^ws(s?):\/\//, 'http$1://').replace(/\/ws\/?$/, '');
}

async function pollHealth() {
  try {
    const resp = await fetch(httpBase() + '/health');
    if (resp.ok) {
      state.healthData = await resp.json();
    } else {
      state.healthData = { status: 'unreachable' };
    }
  } catch {
    state.healthData = { status: 'unreachable' };
  }
  renderHealth();
}

// ── Symbol autocomplete ──

let acTimer = null;

async function symbolAutocomplete(query) {
  clearTimeout(acTimer);
  if (query.length < 2) { $('symbolSuggestions').innerHTML = ''; return; }
  acTimer = setTimeout(async () => {
    try {
      const resp = await fetch(httpBase() + '/symbols?q=' + encodeURIComponent(query) + '&limit=20');
      if (!resp.ok) return;
      const data = await resp.json();
      const dl = $('symbolSuggestions');
      dl.innerHTML = '';
      for (const sym of data.symbols || []) {
        const opt = document.createElement('option');
        opt.value = sym;
        dl.appendChild(opt);
      }
    } catch { /* ignore */ }
  }, 150);
}

// ── Connection ──

const MAX_RECONNECT_DELAY = 10000;

function connect() {
  const url = $('wsUrl').value.trim();
  if (!url) return;
  setStatus('connecting');
  const ws = new WebSocket(url);
  ws.binaryType = 'arraybuffer';
  state.ws = ws;

  ws.onopen = () => {
    state.reconnectAttempts = 0;
    setStatus('connected');
    $('btnSubscribe').disabled = false;
    $('btnGet').disabled = false;
    $('btnConnect').textContent = 'Disconnect';
    addLog('Connected to ' + url, 'log-sub-ok');
    startHealthPolling();
  };

  ws.onclose = () => {
    setStatus('disconnected');
    $('btnSubscribe').disabled = true;
    $('btnGet').disabled = true;
    $('btnConnect').textContent = 'Connect';
    addLog('Disconnected', 'log-error');
    rankings.volume = []; rankings.gainers = []; rankings.losers = [];
    markDirty(D_RANKINGS);
    scheduleReconnect();
  };

  ws.onerror = () => addLog('WebSocket error', 'log-error');

  // Zero-copy message parsing: single DataView over the coalesced frame,
  // parseMessage reads directly at offset — no buf.slice().
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
    markDirty(D_STATS);
  };
}

function scheduleReconnect() {
  if (!state.autoReconnect || !$('autoReconnect').checked) return;
  state.reconnectAttempts++;
  const delay = Math.min(1000 * Math.pow(1.5, state.reconnectAttempts - 1), MAX_RECONNECT_DELAY);
  addLog('Reconnecting in ' + (delay / 1000).toFixed(1) + 's (attempt ' + state.reconnectAttempts + ')...', 'log-info');
  state.reconnectTimer = setTimeout(() => {
    if (!state.ws || state.ws.readyState >= WebSocket.CLOSING) {
      connect();
    }
  }, delay);
}

function toggleConnection() {
  if (state.reconnectTimer) { clearTimeout(state.reconnectTimer); state.reconnectTimer = null; }
  if (state.ws && state.ws.readyState <= WebSocket.OPEN) {
    state.autoReconnect = false;
    state.ws.close();
    state.autoReconnect = true;
    return;
  }
  connect();
}

// ── Actions ──

function doSubscribe() {
  const sym = $('symbolInput').value.trim().toUpperCase();
  if (!sym || !state.ws || state.ws.readyState !== WebSocket.OPEN) return;
  state.ws.send(buildSubscribeOrGet(MSG.SUBSCRIBE, sym, getFlags()));
  addLog('\u2192 Subscribe ' + sym + ' [' + flagsStr(getFlags()) + ']', '');
}

function doGet() {
  const sym = $('symbolInput').value.trim().toUpperCase();
  if (!sym || !state.ws || state.ws.readyState !== WebSocket.OPEN) return;
  state.ws.send(buildSubscribeOrGet(MSG.GET, sym, getFlags()));
  addLog('\u2192 Get ' + sym + ' [' + flagsStr(getFlags()) + ']', '');
}

function doUnsubscribe(securityIdStr) {
  if (!state.ws || state.ws.readyState !== WebSocket.OPEN) return;
  const bigint = BigInt(securityIdStr);
  state.ws.send(buildUnsubscribe(bigint));

  const sub = subscriptions.get(securityIdStr);
  subscriptions.delete(securityIdStr);
  if (state.selectedSecurityId === securityIdStr) {
    state.selectedSecurityId = subscriptions.size > 0 ? subscriptions.keys().next().value : null;
  }
  markDirty(D_SUBLIST | D_BOOK | D_INFO | D_TRADES | D_TITLES);
  addLog('\u2192 Unsubscribe ' + (sub ? sub.symbol : securityIdStr), '');
}

function selectSubscription(id) {
  state.selectedSecurityId = id;
  markDirty(D_SUBLIST | D_BOOK | D_INFO | D_TRADES | D_TITLES);
}

function rankingClick(symbol) {
  if (!state.ws || state.ws.readyState !== WebSocket.OPEN) return;
  for (const [id, sub] of subscriptions) {
    if (sub.symbol === symbol) { selectSubscription(id); return; }
  }
  state.ws.send(buildSubscribeOrGet(MSG.SUBSCRIBE, symbol, DATA_FLAGS.ALL));
  addLog('\u2192 Subscribe ' + symbol + ' [Book+Info] (from ranking)', '');
}

function switchRankingsTab(tab) {
  state.rankingsTab = tab;
  markDirty(D_RANKINGS);
}

// ── Message handler (state mutations only — rendering deferred to rAF) ──

function handleMessage(msg) {
  const sel = state.selectedSecurityId;

  switch (msg.type) {
    case 'SubscribeOk': {
      const id = secIdStr(msg.securityId);
      subscriptions.set(id, {
        symbol: msg.symbol, flags: msg.flags, securityId: msg.securityId,
        orders: new Map(), info: {}, trades: [], orderCount: 0, tradeCount: 0,
      });
      let d = D_SUBLIST;
      if (!sel) {
        state.selectedSecurityId = id;
        d |= D_BOOK | D_INFO | D_TRADES | D_TITLES;
      }
      markDirty(d);
      addLog('SubscribeOk ' + msg.symbol + ' id=' + id + ' [' + flagsStr(msg.flags) + ']', 'log-sub-ok');
      break;
    }
    case 'SubscribeError':
      addLog('SubscribeError ' + msg.symbol + ': ' + msg.errorName, 'log-error');
      break;
    case 'Unsubscribed': {
      const id = secIdStr(msg.securityId);
      const sub = subscriptions.get(id);
      if (sub) {
        subscriptions.delete(id);
        let d = D_SUBLIST;
        if (sel === id) {
          state.selectedSecurityId = subscriptions.size > 0 ? subscriptions.keys().next().value : null;
          d |= D_BOOK | D_INFO | D_TRADES | D_TITLES;
        }
        markDirty(d);
      }
      addLog('Unsubscribed ' + (sub ? sub.symbol : id), 'log-error');
      break;
    }
    case 'BookSnapshot': {
      stats.books++;
      const id = secIdStr(msg.securityId);
      const sub = subscriptions.get(id);
      if (sub) sub.orders = new Map();
      if (sel === id) markDirty(D_BOOK);
      break;
    }
    case 'InfoSnapshot': {
      stats.info++;
      const id = secIdStr(msg.securityId);
      const sub = subscriptions.get(id);
      if (sub) Object.assign(sub.info, msg.fields);
      if (sel === id) markDirty(D_INFO);
      break;
    }
    case 'OrderAdded':
    case 'OrderUpdated': {
      stats.orders++;
      const id = secIdStr(msg.securityId);
      const sub = subscriptions.get(id);
      if (sub) {
        sub.orderCount++;
        // BigInt key avoids toString() allocation per order event
        sub.orders.set(msg.orderId, { side: msg.side, price: msg.price, qty: msg.qty });
      }
      if (sel === id) markDirty(D_BOOK);
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
      if (sel === id) markDirty(D_BOOK);
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
      if (sel === id) markDirty(D_TRADES);
      break;
    }
    case 'BookCleared': {
      const id = secIdStr(msg.securityId);
      const sub = subscriptions.get(id);
      if (sub) {
        if (msg.side === 0) {
          sub.orders = new Map();
        } else {
          const orderSide = msg.side - 1;
          for (const [oid, order] of sub.orders) {
            if (order.side === orderSide) sub.orders.delete(oid);
          }
        }
      }
      if (sel === id) markDirty(D_BOOK);
      const sideNames = ['Both', 'Bid', 'Ask'];
      addLog('BookCleared ' + (sub ? sub.symbol : id) + ' (' + (sideNames[msg.side] || 'Both') + ')', 'log-book');
      break;
    }
    case 'RankingsUpdate': {
      rankings.volume = msg.volume;
      rankings.gainers = msg.gainers;
      rankings.losers = msg.losers;
      markDirty(D_RANKINGS);
      break;
    }
  }
}

// ── Expose to window for HTML onclick handlers ──
window.toggleConnection = toggleConnection;
window.doSubscribe = doSubscribe;
window.doGet = doGet;
window.doUnsubscribe = doUnsubscribe;
window.selectSubscription = selectSubscription;
window.rankingClick = rankingClick;
window.switchRankingsTab = switchRankingsTab;
window.symbolAutocomplete = symbolAutocomplete;
window.clearLog = clearLog;
window.toggleLog = setLogEnabled;

// ── Init ──
setStatus('disconnected');
startHealthPolling();
connect();
