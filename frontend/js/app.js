// Application entry point — WebSocket connection, message handling, actions.

import { MSG, DATA_FLAGS, buildSubscribeOrGet, buildUnsubscribe, parseMessage, flagsStr } from './protocol.js';
import { state, subscriptions, stats } from './state.js';
import {
  $, secIdStr, formatPrice, formatQty,
  setStatus, renderSubList, renderSelected, renderBook, renderInfo, renderTrades, renderHealth,
  addLog, clearLog, updateStats, addTrade,
} from './ui.js';

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

async function pollHealth() {
  try {
    const wsUrl = $('wsUrl').value.trim();
    const httpBase = wsUrl.replace(/^ws(s?):\/\//, 'http$1://').replace(/\/ws\/?$/, '');
    const resp = await fetch(httpBase + '/health');
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
    scheduleReconnect();
  };

  ws.onerror = () => addLog('WebSocket error', 'log-error');

  ws.onmessage = (evt) => {
    if (!(evt.data instanceof ArrayBuffer)) return;
    stats.msgs++;
    const msg = parseMessage(evt.data);
    if (msg) handleMessage(msg);
    updateStats();
  };
}

function scheduleReconnect() {
  if (!state.autoReconnect || !$('autoReconnect').checked) return;
  state.reconnectAttempts++;
  const delay = Math.min(1000 * Math.pow(1.5, state.reconnectAttempts - 1), MAX_RECONNECT_DELAY);
  addLog(`Reconnecting in ${(delay / 1000).toFixed(1)}s (attempt ${state.reconnectAttempts})...`, 'log-info');
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
  addLog(`→ Subscribe ${sym} [${flagsStr(getFlags())}]`, '');
}

function doGet() {
  const sym = $('symbolInput').value.trim().toUpperCase();
  if (!sym || !state.ws || state.ws.readyState !== WebSocket.OPEN) return;
  state.ws.send(buildSubscribeOrGet(MSG.GET, sym, getFlags()));
  addLog(`→ Get ${sym} [${flagsStr(getFlags())}]`, '');
}

function doUnsubscribe(securityIdStr) {
  if (!state.ws || state.ws.readyState !== WebSocket.OPEN) return;
  const bigint = BigInt(securityIdStr);
  state.ws.send(buildUnsubscribe(bigint));
  addLog(`→ Unsubscribe id=${securityIdStr}`, '');
}

function selectSubscription(id) {
  state.selectedSecurityId = id;
  renderSubList();
  renderSelected();
}

// ── Message handler ──

function handleMessage(msg) {
  switch (msg.type) {
    case 'SubscribeOk': {
      const id = secIdStr(msg.securityId);
      subscriptions.set(id, {
        symbol: msg.symbol, flags: msg.flags, securityId: msg.securityId,
        book: null, info: {}, trades: [], orderCount: 0, tradeCount: 0,
      });
      if (!state.selectedSecurityId) selectSubscription(id);
      renderSubList();
      addLog(`SubscribeOk ${msg.symbol} id=${id} [${flagsStr(msg.flags)}]`, 'log-sub-ok');
      break;
    }
    case 'SubscribeError':
      addLog(`SubscribeError ${msg.symbol}: ${msg.errorName}`, 'log-error');
      break;
    case 'Unsubscribed': {
      const id = secIdStr(msg.securityId);
      const sub = subscriptions.get(id);
      subscriptions.delete(id);
      if (state.selectedSecurityId === id) {
        state.selectedSecurityId = subscriptions.size > 0 ? subscriptions.keys().next().value : null;
        renderSelected();
      }
      renderSubList();
      addLog(`Unsubscribed ${sub?.symbol || id}`, 'log-error');
      break;
    }
    case 'BookSnapshot': {
      stats.books++;
      const id = secIdStr(msg.securityId);
      const sub = subscriptions.get(id);
      if (sub) {
        sub.book = { rptSeq: msg.rptSeq, bids: msg.bids, asks: msg.asks };
        sub.orderCount = 0;
      }
      if (state.selectedSecurityId === id) renderBook();
      const sym = sub?.symbol || id;
      addLog(`BookSnapshot ${sym} seq=${msg.rptSeq} ${msg.bids.length}b/${msg.asks.length}a`, 'log-book');
      break;
    }
    case 'InfoSnapshot': {
      stats.info++;
      const id = secIdStr(msg.securityId);
      const sub = subscriptions.get(id);
      if (sub) Object.assign(sub.info, msg.fields);
      if (state.selectedSecurityId === id) renderInfo();
      const sym = sub?.symbol || id;
      const n = Object.keys(msg.fields).length;
      addLog(`InfoSnapshot ${sym} ${n} fields`, 'log-info');
      break;
    }
    case 'OrderAdded':
    case 'OrderUpdated': {
      stats.orders++;
      const id = secIdStr(msg.securityId);
      const sub = subscriptions.get(id);
      if (sub) sub.orderCount++;
      const side = msg.side === 0 ? 'BID' : 'ASK';
      const sym = sub?.symbol || id;
      addLog(`${msg.type} ${sym} ${side} ${formatPrice(msg.price)} x${formatQty(msg.qty)}`, 'log-order');
      break;
    }
    case 'OrderDeleted': {
      stats.orders++;
      const id = secIdStr(msg.securityId);
      const sub = subscriptions.get(id);
      if (sub) sub.orderCount++;
      const side = msg.side === 0 ? 'BID' : 'ASK';
      const sym = sub?.symbol || id;
      addLog(`OrderDeleted ${sym} ${side} oid=${msg.orderId}`, 'log-order');
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
      if (state.selectedSecurityId === id) renderTrades();
      const sym = sub?.symbol || id;
      addLog(`Trade ${sym} ${formatPrice(msg.price)} x${formatQty(msg.qty)}`, 'log-trade');
      break;
    }
    case 'BookCleared': {
      const id = secIdStr(msg.securityId);
      const sub = subscriptions.get(id);
      if (sub) sub.book = null;
      if (state.selectedSecurityId === id) renderBook();
      const sym = sub?.symbol || id;
      addLog(`BookCleared ${sym}`, 'log-book');
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
window.clearLog = clearLog;

// ── Init ──
setStatus('disconnected');
startHealthPolling();
