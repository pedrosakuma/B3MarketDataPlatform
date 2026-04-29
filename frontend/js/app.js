// Main thread: UI events + rendering only.
// All WebSocket I/O, parsing, and state live in worker.js.

import { DATA_FLAGS, INFO_FIELDS } from './protocol.js';
import {
  $, setStatus, updateTitles, renderBook, renderSubsTable,
  renderHealth, renderRankings, showToast, updateStats, formatPrice,
  renderTrades, renderAuction,
} from './ui.js';

// ── Worker ──
const worker = new Worker('./js/worker.js', { type: 'module' });

// ── Chart (TradingView Lightweight Charts) ──
const LWC = globalThis.LightweightCharts;
let chart = null;
let candleSeries = null;
let vwapSeries = null;
let priceLineLow = null;
let priceLineHigh = null;
let lastPriceBandLow = null;
let lastPriceBandHigh = null;

function initChart() {
  const container = $('chartContainer');
  if (!LWC) {
    container.textContent = 'Chart library not loaded';
    return;
  }
  chart = LWC.createChart(container, {
    autoSize: true,
    layout: { background: { type: 'solid', color: '#1a1a2e' }, textColor: '#8899aa', fontSize: 11 },
    grid: { vertLines: { color: '#2a3a5e' }, horzLines: { color: '#2a3a5e' } },
    timeScale: { timeVisible: true, secondsVisible: true, borderColor: '#2a3a5e' },
    rightPriceScale: { borderColor: '#2a3a5e' },
    crosshair: { mode: LWC.CrosshairMode.Normal },
  });
  candleSeries = chart.addCandlestickSeries({
    upColor: '#00c853', downColor: '#ff1744',
    borderUpColor: '#00c853', borderDownColor: '#ff1744',
    wickUpColor: '#00c853', wickDownColor: '#ff1744',
    priceFormat: { type: 'custom', formatter: (p) => formatPrice(p), minMove: 1 },
  });
  // VWAP overlay — thin yellow line. Hidden until first sample arrives.
  vwapSeries = chart.addLineSeries({
    color: '#ffd600', lineWidth: 1,
    priceLineVisible: false, lastValueVisible: false,
    priceFormat: { type: 'custom', formatter: (p) => formatPrice(p), minMove: 1 },
  });
}

// Add/update/remove the price-band horizontal lines on the candle series. Compares against
// the cached values to avoid removing+recreating lines on every frame.
function applyPriceBands(low, high) {
  if (low !== lastPriceBandLow) {
    if (priceLineLow) { candleSeries.removePriceLine(priceLineLow); priceLineLow = null; }
    if (low != null) {
      priceLineLow = candleSeries.createPriceLine({
        price: low, color: '#ff5252', lineWidth: 1,
        lineStyle: LWC.LineStyle.Dashed, axisLabelVisible: true, title: 'PB Low',
      });
    }
    lastPriceBandLow = low;
  }
  if (high !== lastPriceBandHigh) {
    if (priceLineHigh) { candleSeries.removePriceLine(priceLineHigh); priceLineHigh = null; }
    if (high != null) {
      priceLineHigh = candleSeries.createPriceLine({
        price: high, color: '#ff5252', lineWidth: 1,
        lineStyle: LWC.LineStyle.Dashed, axisLabelVisible: true, title: 'PB High',
      });
    }
    lastPriceBandHigh = high;
  }
}

function handleChartData(chartData) {
  if (!chartData) {
    $('chartEmpty').style.display = '';
    if (candleSeries) candleSeries.setData([]);
    if (vwapSeries) vwapSeries.setData([]);
    return;
  }

  $('chartEmpty').style.display = 'none';

  if (!chart) initChart();
  if (!candleSeries) return;

  if (chartData.full) {
    candleSeries.setData(chartData.candles);
    if (vwapSeries) {
      vwapSeries.setData(chartData.candles.map(c => ({ time: c.time, value: c.avg })));
    }
    if (chartData.scroll) chart.timeScale().scrollToRealTime();
  } else if (chartData.updates) {
    for (const u of chartData.updates) {
      candleSeries.update(u);
      if (vwapSeries) vwapSeries.update({ time: u.time, value: u.avg });
    }
  } else if (chartData.update) {
    candleSeries.update(chartData.update);
    if (vwapSeries) vwapSeries.update({ time: chartData.update.time, value: chartData.update.avg });
  }

  updateResolutionLabel(chartData.resolution);
}

// Compute effective price band horizontals from raw SBE mantissa values.
// PriceLimitType is the discriminator from B3 schema (tag 1306):
//   0 = PRICE_UNIT  → limits ARE absolute prices in PriceOptional units (exp -4)
//   1 = TICKS       → limits are tick offsets vs. TradingReferencePrice combined with MinPriceIncrement
//   2 = PERCENTAGE  → limits are percentage offsets vs. TradingReferencePrice (e.g. ±1.0000 = ±1%)
// All inputs/outputs are mantissa values (chart price coordinates use exp -4 like LastTradePrice).
// TradingReferencePrice and MinPriceIncrement use Fixed8 (exp -8) per the schema.
function computeEffectiveBands(lowOff, highOff, type, refPrice, minIncr) {
  if (type == null || type === 0) {
    // PRICE_UNIT: limits are already absolute prices in the chart's exp (-4).
    return { low: lowOff, high: highOff };
  }
  if (refPrice == null) return { low: null, high: null };
  const refDecimal = refPrice / 1e8; // Fixed8 → decimal price units
  if (type === 2) {
    // PERCENTAGE: offset / 1e4 = percent (e.g. 10000 → 1.0%). Effective = ref * (1 + pct/100).
    const lowPct = lowOff != null ? lowOff / 1e4 : null;
    const highPct = highOff != null ? highOff / 1e4 : null;
    return {
      low: lowPct != null ? Math.round(refDecimal * (1 + lowPct / 100) * 1e4) : null,
      high: highPct != null ? Math.round(refDecimal * (1 + highPct / 100) * 1e4) : null,
    };
  }
  if (type === 1) {
    // TICKS: offset / 1e4 = tick count, mpi / 1e8 = price step. Effective = ref + ticks * mpi.
    if (minIncr == null) return { low: null, high: null };
    const mpi = minIncr / 1e8;
    const lowT = lowOff != null ? lowOff / 1e4 : null;
    const highT = highOff != null ? highOff / 1e4 : null;
    return {
      low: lowT != null ? Math.round((refDecimal + lowT * mpi) * 1e4) : null,
      high: highT != null ? Math.round((refDecimal + highT * mpi) * 1e4) : null,
    };
  }
  return { low: null, high: null };
}

// Overlay frame carries only price bands now; VWAP is derived per-candle by handleChartData.
function handleOverlays(ov) {
  if (!ov) { applyPriceBands(null, null); return; }
  const eff = computeEffectiveBands(
    ov.priceBandLow, ov.priceBandHigh,
    ov.priceLimitType, ov.tradingReferencePrice, ov.minPriceIncrement,
  );
  applyPriceBands(eff.low, eff.high);
}

function updateResolutionLabel(resolution) {
  const el = $('chartResolution');
  if (!el) return;
  if (!resolution) { el.textContent = ''; return; }
  if (resolution < 60) el.textContent = resolution + 's';
  else if (resolution < 3600) el.textContent = (resolution / 60) + 'm';
  else el.textContent = (resolution / 3600) + 'h';
}

// ── Column selector ──
const DEFAULT_COLUMNS = ['LastTradePrice', 'HighPrice', 'LowPrice', 'TradeVolume', 'NetChange', 'TradingStatus'];
const STORAGE_KEY = 'subsTableColumns';
let visibleColumns = loadColumns() || [...DEFAULT_COLUMNS];

function loadColumns() {
  try {
    const stored = localStorage.getItem(STORAGE_KEY);
    if (stored) {
      const parsed = JSON.parse(stored);
      if (Array.isArray(parsed) && parsed.length > 0) return parsed;
    }
  } catch { /* ignore */ }
  return null;
}

function saveColumns() {
  try { localStorage.setItem(STORAGE_KEY, JSON.stringify(visibleColumns)); } catch { /* ignore */ }
}

function initColumnSelector() {
  const dropdown = $('colSelectorDropdown');
  if (!dropdown) return;
  dropdown.innerHTML = '';
  for (const field of INFO_FIELDS) {
    const label = document.createElement('label');
    const cb = document.createElement('input');
    cb.type = 'checkbox';
    cb.checked = visibleColumns.includes(field);
    cb.dataset.field = field;
    cb.addEventListener('change', () => {
      if (cb.checked) {
        if (!visibleColumns.includes(field)) visibleColumns.push(field);
      } else {
        visibleColumns = visibleColumns.filter(c => c !== field);
      }
      saveColumns();
      renderDirty |= DR_SUBS;
      scheduleRender();
    });
    label.appendChild(cb);
    label.appendChild(document.createTextNode(' ' + field));
    dropdown.appendChild(label);
  }
}

function toggleColumnSelector() {
  $('colSelectorDropdown').classList.toggle('open');
}

document.addEventListener('click', (e) => {
  if (!e.target.closest('.col-selector')) {
    const dd = $('colSelectorDropdown');
    if (dd) dd.classList.remove('open');
  }
});

// ── View state (populated by worker frames) ──
const view = {
  selectedId: null,
  selectedSymbol: null,
  book: null,
  allInfo: null,
  overlays: undefined,
  trades: undefined,
  auction: undefined,
  rankings: { volume: [], gainers: [], losers: [] },
  stats: { msgs: 0, books: 0, info: 0, orders: 0, trades: 0 },
  connected: false,
  serverReady: false,
  rankingsTab: 'volume',
  healthData: null,
};

// Chart updates are queued to preserve ordering. A full swap (or null clear)
// discards any queued incrementals — the full swap is always the authoritative
// state. Incrementals are appended after the last full swap so they are applied
// in the correct order when rAF fires, preventing a CandleUpdate from
// overwriting a pending snapshot full-swap.
let chartQueue = [];

// Per-section dirty bits for doRender. Each worker frame sets the bits it
// touched; doRender renders only those sections, then clears them. This
// avoids unconditional full re-renders of book/subs/stats/rankings every
// 16ms when only a counter changed.
const DR_TITLES   = 0x01;
const DR_BOOK     = 0x02;
const DR_CHART    = 0x04;
const DR_OVERLAYS = 0x08;
const DR_SUBS     = 0x10;
const DR_TRADES   = 0x20;
const DR_AUCTION  = 0x40;
const DR_STATS    = 0x80;
const DR_RANKINGS = 0x100;
let renderDirty = 0;

// ── Worker message handler ──
worker.onmessage = (evt) => {
  const msg = evt.data;

  switch (msg.type) {
    case 'frame':
      if (msg.selectedId !== undefined) { view.selectedId = msg.selectedId; renderDirty |= DR_TITLES | DR_BOOK | DR_CHART | DR_OVERLAYS | DR_TRADES | DR_AUCTION; }
      if (msg.selectedSymbol !== undefined) { view.selectedSymbol = msg.selectedSymbol; renderDirty |= DR_TITLES; }
      if (msg.book !== undefined) { view.book = msg.book; renderDirty |= DR_BOOK; }
      if (msg.chart !== undefined) {
        if (msg.chart === null || msg.chart.full) {
          // Full swap or clear: discard any queued incrementals — full swap is authoritative
          chartQueue = [msg.chart];
        } else {
          // Incremental: append after whatever is already queued
          chartQueue.push(msg.chart);
        }
        renderDirty |= DR_CHART;
      }
      if (msg.allInfo !== undefined) { view.allInfo = msg.allInfo; renderDirty |= DR_SUBS; }
      if (msg.overlays !== undefined) { view.overlays = msg.overlays; renderDirty |= DR_OVERLAYS; }
      if (msg.trades !== undefined) { view.trades = msg.trades; renderDirty |= DR_TRADES; }
      if (msg.auction !== undefined) { view.auction = msg.auction; renderDirty |= DR_AUCTION; }
      if (msg.rankings !== undefined) { view.rankings = msg.rankings; renderDirty |= DR_RANKINGS; }
      if (msg.stats !== undefined) { view.stats = msg.stats; renderDirty |= DR_STATS; }
      if (renderDirty !== 0) scheduleRender();
      break;
    case 'status':
      view.connected = msg.status === 'connected';
      setStatus(view.serverReady ? msg.status : msg.status === 'connected' ? 'waiting' : msg.status);
      updateActionButtons();
      $('btnConnect').textContent = view.connected ? 'Disconnect' : 'Connect';
      if (view.connected) startHealthPolling();
      else stopHealthPolling();
      break;
    case 'serverReady':
      view.serverReady = msg.ready;
      setStatus(view.connected ? (msg.ready ? 'connected' : 'waiting') : 'disconnected');
      updateActionButtons();
      break;
    case 'log':
      showToast(msg.text, msg.cssClass);
      break;
    case 'recoveryProgress':
      renderRecoveryBanner(msg.totalStaleSymbols, msg.totalSymbols, msg.staleByKind);
      break;
  }
};

// Per-symbol stale-status kinds (mirrors B3.Umdf.Book.SymbolGapKind).
const RECOVERY_KIND_NAMES = [
  'MBO', 'OpeningPrice', 'TheoreticalOpeningPrice', 'ClosingPrice',
  'AuctionImbalance', 'QuantityBand', 'PriceBand', 'HighPrice',
  'LowPrice', 'LastTradePrice', 'SettlementPrice', 'OpenInterest',
  'ExecutionStatistics', 'SecurityStatus',
];

function renderRecoveryBanner(staleCount, totalCount, staleByKind) {
  let banner = $('recoveryBanner');
  if (!banner) {
    banner = document.createElement('div');
    banner.id = 'recoveryBanner';
    banner.className = 'recovery-banner';
    document.body.appendChild(banner);
  }
  if (!staleCount) { banner.style.display = 'none'; return; }
  const parts = [];
  for (const [kindId, n] of Object.entries(staleByKind || {})) {
    const name = RECOVERY_KIND_NAMES[+kindId] || `Kind${kindId}`;
    parts.push(`${name}:${n}`);
  }
  const breakdown = parts.length > 0 ? ` (${parts.join(', ')})` : '';
  banner.textContent = `\u26A0 Recovering ${staleCount}/${totalCount} symbols${breakdown}`;
  banner.style.display = 'block';
}

// ── Render (single rAF per worker frame) ──
let renderPending = false;

function scheduleRender() {
  if (renderPending) return;
  renderPending = true;
  requestAnimationFrame(doRender);
}

function doRender() {
  renderPending = false;
  const d = renderDirty;
  renderDirty = 0;
  if (d & DR_TITLES) updateTitles(view.selectedSymbol);
  if (d & DR_BOOK) renderBook(view.book);
  if (chartQueue.length > 0) {
    const pending = chartQueue.splice(0);
    for (const cd of pending) handleChartData(cd);
  }
  if (d & DR_OVERLAYS && view.overlays !== undefined) {
    handleOverlays(view.overlays);
    view.overlays = undefined;
  }
  if (d & DR_SUBS) renderSubsTable(view.allInfo, visibleColumns, view.selectedId);
  if (d & DR_TRADES && view.trades !== undefined) {
    renderTrades(view.trades);
    view.trades = undefined;
  }
  if (d & DR_AUCTION && view.auction !== undefined) {
    renderAuction(view.auction);
    view.auction = undefined;
  }
  if (d & DR_STATS) updateStats(view.stats);
  if (d & DR_RANKINGS) renderRankings(view.rankings, view.rankingsTab, view.connected);
}

// ── Subscriptions table event delegation ──
$('subsTable').addEventListener('click', (e) => {
  const detailBtn = e.target.closest('.detail-btn');
  if (detailBtn) {
    const sym = detailBtn.dataset.symbol;
    if (sym) showInstrumentDetail(sym);
    return;
  }
  const unsubBtn = e.target.closest('.unsub-btn');
  if (unsubBtn) {
    doUnsubscribe(unsubBtn.dataset.id);
    return;
  }
  const tr = e.target.closest('tr[data-id]');
  if (tr) selectSubscription(tr.dataset.id);
});

// ── Helpers ──
function getFlags() {
  let f = 0;
  if ($('flagBook').checked) f |= DATA_FLAGS.BOOK;
  if ($('flagInfo').checked) f |= DATA_FLAGS.INFO;
  if ($('flagMbp').checked) f |= DATA_FLAGS.MBP;
  return f || DATA_FLAGS.MBP;
}

// ── Health polling (fetch stays on main thread) ──
let healthInterval = null;
let healthAbortController = null;

function startHealthPolling() {
  if (healthInterval) return;
  pollHealth();
  healthInterval = setInterval(pollHealth, 5000);
}

function stopHealthPolling() {
  if (healthInterval) { clearInterval(healthInterval); healthInterval = null; }
  if (healthAbortController) { healthAbortController.abort(); healthAbortController = null; }
}

function httpBase() {
  return $('wsUrl').value.trim().replace(/^ws(s?):\/\//, 'http$1://').replace(/\/ws\/?$/, '');
}

async function pollHealth() {
  if (healthAbortController) healthAbortController.abort();
  healthAbortController = new AbortController();
  try {
    const resp = await fetch(httpBase() + '/health', { signal: healthAbortController.signal });
    view.healthData = resp.ok ? await resp.json() : { status: 'unreachable' };
  } catch (e) {
    if (e.name === 'AbortError') return;
    view.healthData = { status: 'unreachable' };
  }
  renderHealth(view.healthData);
}

// ── Symbol autocomplete ──
let acTimer = null;
let acAbortController = null;

async function symbolAutocomplete(query) {
  clearTimeout(acTimer);
  if (acAbortController) { acAbortController.abort(); acAbortController = null; }
  if (query.length < 2) { $('symbolSuggestions').innerHTML = ''; return; }
  acAbortController = new AbortController();
  const controller = acAbortController;
  acTimer = setTimeout(async () => {
    try {
      const resp = await fetch(httpBase() + '/symbols?q=' + encodeURIComponent(query) + '&limit=20', {
        signal: controller.signal,
      });
      if (!resp.ok) return;
      const data = await resp.json();
      const dl = $('symbolSuggestions');
      dl.innerHTML = '';
      for (const sym of data.symbols || []) {
        const opt = document.createElement('option');
        opt.value = sym;
        dl.appendChild(opt);
      }
    } catch (e) {
      if (e.name !== 'AbortError') { /* ignore other errors */ }
    }
  }, 150);
}

// ── UI actions (forward commands to worker) ──

let connectDebounceTimer = null;

function toggleConnection() {
  if (connectDebounceTimer) return;
  connectDebounceTimer = setTimeout(() => { connectDebounceTimer = null; }, 500);
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

function updateActionButtons() {
  const canSubscribe = view.connected && view.serverReady;
  $('btnSubscribe').disabled = !canSubscribe;
  $('btnGet').disabled = !canSubscribe;
}

function doUnsubscribe(securityId) {
  if (!view.connected) return;
  worker.postMessage({ cmd: 'unsubscribe', securityId });
}

function selectSubscription(id) {
  view.selectedId = id; // Optimistic update — confirmed by next worker frame
  renderDirty |= DR_TITLES | DR_BOOK | DR_SUBS;
  scheduleRender();
  worker.postMessage({ cmd: 'select', securityId: id });
  closeSidebarIfMobile();
}

function rankingClick(symbol) {
  if (!view.connected) return;
  const existing = view.allInfo ? view.allInfo.find(s => s.symbol === symbol) : null;
  if (existing) {
    selectSubscription(existing.id);
  } else {
    worker.postMessage({ cmd: 'rankingSubscribe', symbol });
  }
  closeSidebarIfMobile();
}

function closeSidebarIfMobile() {
  if (window.innerWidth <= 900) {
    document.getElementById('sidebar')?.classList.remove('open');
    document.getElementById('sidebarBackdrop')?.classList.remove('open');
  }
}

function switchRankingsTab(tab) {
  view.rankingsTab = tab;
  renderDirty |= DR_RANKINGS;
  scheduleRender();
}

function setChartResolution(value) {
  worker.postMessage({ cmd: 'setResolution', value: parseInt(value) || 0 });
}

function setBookDepth(value) {
  worker.postMessage({ cmd: 'setBookDepth', value: parseInt(value) || 15 });
}

// ── Expose to window for HTML onclick handlers ──
window.toggleConnection = toggleConnection;
window.doSubscribe = doSubscribe;
window.doGet = doGet;
window.rankingClick = rankingClick;
window.switchRankingsTab = switchRankingsTab;
window.setChartResolution = setChartResolution;
window.setBookDepth = setBookDepth;
window.symbolAutocomplete = symbolAutocomplete;
window.showInstrumentDetail = showInstrumentDetail;
window.closeModal = closeModal;
window.toggleColumnSelector = toggleColumnSelector;

// ── Instrument detail modal ──

const SECURITY_TYPE_NAMES = {
  1:'CASH (Rights)',2:'CORP',3:'CS (Common Stock)',4:'DTERM (Forward)',5:'ETF',
  6:'FOPT (Future Opt)',7:'FORWARD',8:'FUT (Futures)',9:'INDEX',10:'INDEXOPT',
  11:'MLEG (Multileg)',12:'OPT (Option)',13:'OPTEXER',14:'PS (Preferred)',
  15:'SECLOAN',16:'SOPT (Spot Opt)',17:'SPOT',
};
const PRODUCT_NAMES = {
  2:'Commodity',3:'Corporate',4:'Currency',5:'Equity',6:'Government',
  7:'Index',15:'Economic Indicator',16:'Multileg',
};
const PUT_CALL_NAMES = { 0:'Put', 1:'Call' };
const EXERCISE_NAMES = { 0:'European', 1:'American' };
const TRADING_STATUS_NAMES = {
  2:'Paused',4:'Closed',17:'Open',18:'Forbidden',20:'Unknown',21:'Auction',101:'FinalClosing',
};
const TRADING_EVENT_NAMES = { 0:'None',1:'Change',4:'NewStatus',101:'PriceBand' };
const PRICE_LIMIT_TYPE_NAMES = { 0:'Price Unit', 1:'Ticks', 2:'Percentage' };

const CFI_CATEGORY = { E:'Equity',D:'Debt',R:'Right/Warrant',O:'Option',F:'Future',M:'Others(Multileg)',S:'Swap' };
const CFI_EQUITY_GROUP = { S:'Common/Ordinary',P:'Preferred',C:'Convertible',F:'Preference',L:'Limited Partnership',D:'Depositary Receipt',Y:'Structured Instrument',M:'Others' };
const LEG_SIDE_NAMES = { 1:'Buy', 2:'Sell' };
const INSTR_ATTRIB_TYPE_NAMES = { 24:'Trade Type Eligibility', 34:'GTD/GTC Eligibility' };
const INSTR_ATTRIB_VALUE_NAMES = { 1:'Electronic Match / GTD-GTC', 2:'Order Cross', 3:'Block Trade', 14:'RFQ for Cross', 17:'Negotiated Quote' };

function decodeCfi(code) {
  if (!code || code.length < 2) return null;
  const cat = CFI_CATEGORY[code[0]];
  if (!cat) return code;
  let group = null;
  if (code[0] === 'E') group = CFI_EQUITY_GROUP[code[1]];
  return group ? cat + ' / ' + group : cat;
}

function enumLabel(map, val) {
  if (val == null) return null;
  return map[val] || String(val);
}

let detailAbortController = null;

async function showInstrumentDetail(symbol) {
  symbol = symbol || view.selectedSymbol;
  if (!symbol) return;
  if (detailAbortController) { detailAbortController.abort(); detailAbortController = null; }
  detailAbortController = new AbortController();
  const controller = detailAbortController;
  const modal = $('instrumentModal');
  const body = $('modalBody');
  $('modalTitle').textContent = 'Instrument Detail \u2014 ' + symbol;
  body.innerHTML = '<div class="empty-msg">Loading...</div>';
  modal.classList.remove('hidden');

  try {
    const resp = await fetch(httpBase() + '/instrument/' + encodeURIComponent(symbol), {
      signal: controller.signal,
    });
    if (!resp.ok) { body.innerHTML = '<div class="empty-msg">Not available (HTTP ' + resp.status + ')</div>'; return; }
    const data = await resp.json();
    renderModal(body, data);
  } catch (e) {
    if (e.name !== 'AbortError') {
      body.innerHTML = '<div class="empty-msg">Error: ' + e.message + '</div>';
    }
  }
}

function renderModal(body, d) {
  const fmtP = (v, dec = 4) => v == null ? null : (v / Math.pow(10, dec)).toFixed(dec);
  const fmtP8 = (v) => fmtP(v, 8);
  const fmtFixed8 = (v) => {
    if (v == null) return null;
    const s = (v / 1e8).toFixed(8);
    return s.includes('.') ? s.replace(/0+$/, '').replace(/\.$/, '') : s;
  };
  const fmtQ = (v) => v == null ? null : v.toLocaleString();
  const fmtDate = (v) => {
    if (v == null) return null;
    const s = String(v);
    if (s.length === 8) return s.slice(0,4)+'-'+s.slice(4,6)+'-'+s.slice(6,8);
    return s;
  };

  const sections = [
    { title: 'Identity', fields: [
      ['Security ID', d.securityId],
      ['Symbol', d.symbol],
      ['Asset', d.asset],
      ['ISIN', d.isinNumber],
      ['Currency', d.currency],
      ['CFI Code', d.cfiCode],
      ['CFI Interpretation', decodeCfi(d.cfiCode)],
      ['Security Group', d.securityGroup],
      ['Market Segment', d.marketSegmentID],
      ['Description', d.securityDescription],
    ]},
    { title: 'Classification', fields: [
      ['Security Type', enumLabel(SECURITY_TYPE_NAMES, d.securityType)],
      ['Security SubType', d.securitySubType],
      ['Product', enumLabel(PRODUCT_NAMES, d.product)],
      ['Put/Call', enumLabel(PUT_CALL_NAMES, d.putOrCall)],
      ['Exercise Style', enumLabel(EXERCISE_NAMES, d.exerciseStyle)],
      ['Maturity Date', fmtDate(d.maturityDate)],
    ]},
    { title: 'Tick & Lot', fields: [
      ['Min Price Increment', fmtFixed8(d.minPriceIncrement)],
      ['Price Divisor', fmtFixed8(d.priceDivisor)],
      ['Contract Multiplier', fmtFixed8(d.contractMultiplier)],
      ['Strike Price', fmtP(d.strikePrice)],
      ['Tick Size Denominator', d.tickSizeDenominator],
    ]},
    { title: 'Trading Status', fields: [
      ['Status', enumLabel(TRADING_STATUS_NAMES, d.tradingStatus)],
      ['Event', enumLabel(TRADING_EVENT_NAMES, d.tradingEvent)],
    ]},
    { title: 'Prices', fields: [
      ['Opening', fmtP(d.openingPrice)],
      ['Closing', fmtP8(d.closingPrice)],
      ['High', fmtP(d.highPrice)],
      ['Low', fmtP(d.lowPrice)],
      ['Last Trade', fmtP(d.lastTradePrice)],
      ['Last Trade Size', fmtQ(d.lastTradeSize)],
      ['Settlement', fmtP(d.settlementPrice)],
      ['VWAP', fmtP(d.vwapPrice)],
    ]},
    { title: 'Auction & Bands', fields: [
      ['Theoretical Opening', fmtP(d.theoreticalOpeningPrice)],
      ['Theoretical Open Size', fmtQ(d.theoreticalOpeningSize)],
      ['Auction Imbalance', fmtQ(d.auctionImbalanceSize)],
      ['Band Low', fmtP(d.priceBandLow)],
      ['Band High', fmtP(d.priceBandHigh)],
      ['Band Type', enumLabel(PRICE_LIMIT_TYPE_NAMES, d.priceLimitType)],
      ['Trading Reference', fmtP8(d.tradingReferencePrice)],
    ]},
    { title: 'Statistics', fields: [
      ['Trade Volume', fmtQ(d.tradeVolume)],
      ['Net Change', fmtP8(d.netChangeFromPrevDay)],
      ['Number of Trades', fmtQ(d.numberOfTrades)],
      ['Open Interest', fmtQ(d.openInterest)],
      ['Avg Daily Traded Qty', fmtQ(d.avgDailyTradedQty)],
      ['Max Trade Vol', fmtQ(d.maxTradeVol)],
    ]},
  ];

  let html = '';
  for (const sec of sections) {
    const visible = sec.fields.some(([,v]) => v != null);
    if (!visible) continue;
    html += '<div class="modal-section"><h4>' + sec.title + '</h4><div class="modal-grid">';
    for (const [label, val] of sec.fields) {
      const cls = val == null ? 'val null' : 'val';
      const display = val == null ? '\u2014' : val;
      html += '<div class="modal-field"><span class="lbl">' + label + '</span><span class="' + cls + '">' + display + '</span></div>';
    }
    html += '</div></div>';
  }

  if (d.underlyings && d.underlyings.length > 0) {
    html += '<div class="modal-section"><h4>Underlyings</h4><table class="modal-table"><thead><tr><th>Security ID</th><th>Symbol</th></tr></thead><tbody>';
    for (const u of d.underlyings) {
      html += '<tr><td>' + u.securityId + '</td><td>' + (u.symbol || '\u2014') + '</td></tr>';
    }
    html += '</tbody></table></div>';
  }

  if (d.legs && d.legs.length > 0) {
    html += '<div class="modal-section"><h4>Legs</h4><table class="modal-table"><thead><tr><th>Security ID</th><th>Symbol</th><th>Ratio Qty</th><th>Security Type</th><th>Side</th></tr></thead><tbody>';
    for (const l of d.legs) {
      const ratioDisplay = l.ratioQty != null ? (l.ratioQty / 1e7).toFixed(7).replace(/0+$/, '').replace(/\.$/, '') : '\u2014';
      html += '<tr><td>' + l.securityId + '</td><td>' + (l.symbol || '\u2014') + '</td><td>' + ratioDisplay + '</td><td>' + enumLabel(SECURITY_TYPE_NAMES, l.securityType) + '</td><td>' + (enumLabel(LEG_SIDE_NAMES, l.side) || '\u2014') + '</td></tr>';
    }
    html += '</tbody></table></div>';
  }

  if (d.instrAttribs && d.instrAttribs.length > 0) {
    html += '<div class="modal-section"><h4>Attributes</h4><table class="modal-table"><thead><tr><th>Type</th><th>Value</th></tr></thead><tbody>';
    for (const a of d.instrAttribs) {
      html += '<tr><td>' + (INSTR_ATTRIB_TYPE_NAMES[a.type] || a.type) + '</td><td>' + (INSTR_ATTRIB_VALUE_NAMES[a.value] || a.value) + '</td></tr>';
    }
    html += '</tbody></table></div>';
  }

  body.innerHTML = html;
}

function closeModal() {
  $('instrumentModal').classList.add('hidden');
}

// ── Keyboard shortcuts ──

function navigateSubscription(delta) {
  if (!view.allInfo || view.allInfo.length === 0) return;
  const currentIdx = view.allInfo.findIndex(s => s.id === view.selectedId);
  let next = currentIdx + delta;
  if (next < 0) next = view.allInfo.length - 1;
  if (next >= view.allInfo.length) next = 0;
  selectSubscription(view.allInfo[next].id);
}

document.addEventListener('keydown', (e) => {
  if (e.key === 'Escape') { closeModal(); return; }

  if (e.ctrlKey && e.key === 'i') {
    e.preventDefault();
    const modal = $('instrumentModal');
    if (modal.classList.contains('hidden')) showInstrumentDetail();
    else closeModal();
    return;
  }

  if (e.altKey && e.key === 'ArrowUp') { e.preventDefault(); navigateSubscription(-1); return; }
  if (e.altKey && e.key === 'ArrowDown') { e.preventDefault(); navigateSubscription(1); return; }
});

// ── Init ──
setStatus('disconnected');
initColumnSelector();
initChart();
startHealthPolling();
worker.postMessage({ cmd: 'connect', url: $('wsUrl').value.trim() });
