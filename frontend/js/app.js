// Main thread: UI events + rendering only.
// All WebSocket I/O, parsing, and state live in worker.js.

import { DATA_FLAGS } from './protocol.js';
import {
  $, setStatus, renderSubList, updateTitles, renderBook, renderInfo, renderTrades,
  renderHealth, renderRankings, addLog, clearLog, setLogEnabled, updateStats,
} from './ui.js';

// ── Worker ──
const worker = new Worker('./js/worker.js', { type: 'module' });

// ── View state (populated by worker frames, read by render functions) ──
const view = {
  subs: [],
  selectedId: null,
  selectedSymbol: null,
  book: null,
  info: null,
  trades: null,
  rankings: { volume: [], gainers: [], losers: [] },
  stats: { msgs: 0, books: 0, info: 0, orders: 0, trades: 0 },
  connected: false,
  rankingsTab: 'volume',
  healthData: null,
};

// ── Worker message handler ──
worker.onmessage = (evt) => {
  const msg = evt.data;

  switch (msg.type) {
    case 'frame':
      if (msg.subs !== undefined) view.subs = msg.subs;
      if (msg.selectedId !== undefined) view.selectedId = msg.selectedId;
      if (msg.selectedSymbol !== undefined) view.selectedSymbol = msg.selectedSymbol;
      if (msg.book !== undefined) view.book = msg.book;
      if (msg.info !== undefined) view.info = msg.info;
      if (msg.trades !== undefined) view.trades = msg.trades;
      if (msg.rankings !== undefined) view.rankings = msg.rankings;
      if (msg.stats !== undefined) view.stats = msg.stats;
      scheduleRender();
      break;
    case 'status':
      view.connected = msg.status === 'connected';
      setStatus(msg.status);
      $('btnSubscribe').disabled = !view.connected;
      $('btnGet').disabled = !view.connected;
      $('btnConnect').textContent = view.connected ? 'Disconnect' : 'Connect';
      if (view.connected) startHealthPolling();
      break;
    case 'log':
      addLog(msg.text, msg.cssClass);
      break;
  }
};

// ── Render (single rAF per worker frame) ──
let renderPending = false;

function scheduleRender() {
  if (renderPending) return;
  renderPending = true;
  requestAnimationFrame(doRender);
}

function doRender() {
  renderPending = false;
  renderSubList(view.subs, view.selectedId);
  updateTitles(view.selectedSymbol);
  renderBook(view.book);
  renderInfo(view.info);
  renderTrades(view.trades);
  updateStats(view.stats);
  renderRankings(view.rankings, view.rankingsTab, view.connected);
}

// ── Helpers ──
function getFlags() {
  let f = 0;
  if ($('flagBook').checked) f |= DATA_FLAGS.BOOK;
  if ($('flagInfo').checked) f |= DATA_FLAGS.INFO;
  return f || DATA_FLAGS.ALL;
}

// ── Health polling (fetch stays on main thread) ──
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
    view.healthData = resp.ok ? await resp.json() : { status: 'unreachable' };
  } catch {
    view.healthData = { status: 'unreachable' };
  }
  renderHealth(view.healthData);
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

// ── UI actions (forward commands to worker) ──

function toggleConnection() {
  if (view.connected) {
    worker.postMessage({ cmd: 'disconnect' });
  } else {
    worker.postMessage({ cmd: 'connect', url: $('wsUrl').value.trim() });
  }
}

function doSubscribe() {
  const sym = $('symbolInput').value.trim().toUpperCase();
  if (!sym || !view.connected) return;
  worker.postMessage({ cmd: 'subscribe', symbol: sym, flags: getFlags() });
}

function doGet() {
  const sym = $('symbolInput').value.trim().toUpperCase();
  if (!sym || !view.connected) return;
  worker.postMessage({ cmd: 'get', symbol: sym, flags: getFlags() });
}

function doUnsubscribe(securityId) {
  if (!view.connected) return;
  worker.postMessage({ cmd: 'unsubscribe', securityId });
}

function selectSubscription(id) {
  worker.postMessage({ cmd: 'select', securityId: id });
}

function rankingClick(symbol) {
  if (!view.connected) return;
  // Check if already subscribed locally
  const existing = view.subs.find(s => s.symbol === symbol);
  if (existing) {
    selectSubscription(existing.id);
  } else {
    worker.postMessage({ cmd: 'rankingSubscribe', symbol });
  }
}

function switchRankingsTab(tab) {
  view.rankingsTab = tab;
  scheduleRender();
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
worker.postMessage({ cmd: 'connect', url: $('wsUrl').value.trim() });
