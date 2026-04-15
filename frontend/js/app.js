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
  serverReady: false,
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
      setStatus(view.serverReady ? msg.status : msg.status === 'connected' ? 'waiting' : msg.status);
      updateActionButtons();
      $('btnConnect').textContent = view.connected ? 'Disconnect' : 'Connect';
      if (view.connected) startHealthPolling();
      break;
    case 'serverReady':
      view.serverReady = msg.ready;
      setStatus(view.connected ? (msg.ready ? 'connected' : 'waiting') : 'disconnected');
      updateActionButtons();
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
  worker.postMessage({ cmd: 'select', securityId: id });
  closeSidebarIfMobile();
}

function rankingClick(symbol) {
  if (!view.connected) return;
  const existing = view.subs.find(s => s.symbol === symbol);
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
window.showInstrumentDetail = showInstrumentDetail;
window.closeModal = closeModal;

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

async function showInstrumentDetail() {
  if (!view.selectedSymbol) return;
  const modal = $('instrumentModal');
  const body = $('modalBody');
  $('modalTitle').textContent = 'Instrument Detail — ' + view.selectedSymbol;
  body.innerHTML = '<div class="empty-msg">Loading...</div>';
  modal.classList.remove('hidden');

  try {
    const resp = await fetch(httpBase() + '/instrument/' + encodeURIComponent(view.selectedSymbol));
    if (!resp.ok) { body.innerHTML = '<div class="empty-msg">Not available (HTTP ' + resp.status + ')</div>'; return; }
    const data = await resp.json();
    renderModal(body, data);
  } catch (e) {
    body.innerHTML = '<div class="empty-msg">Error: ' + e.message + '</div>';
  }
}

function renderModal(body, d) {
  const dec = parseInt($('priceDecimals').value) || 0;
  const divisor = Math.pow(10, dec);
  const fmtP = (v) => v == null ? null : (v / divisor).toFixed(dec);
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
      ['Min Price Increment', fmtP(d.minPriceIncrement)],
      ['Price Divisor', d.priceDivisor],
      ['Contract Multiplier', d.contractMultiplier],
      ['Strike Price', fmtP(d.strikePrice)],
      ['Tick Size Denominator', d.tickSizeDenominator],
    ]},
    { title: 'Trading Status', fields: [
      ['Status', enumLabel(TRADING_STATUS_NAMES, d.tradingStatus)],
      ['Event', enumLabel(TRADING_EVENT_NAMES, d.tradingEvent)],
    ]},
    { title: 'Prices', fields: [
      ['Opening', fmtP(d.openingPrice)],
      ['Closing', fmtP(d.closingPrice)],
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
      ['Trading Reference', fmtP(d.tradingReferencePrice)],
    ]},
    { title: 'Statistics', fields: [
      ['Trade Volume', fmtQ(d.tradeVolume)],
      ['Net Change', fmtP(d.netChangeFromPrevDay)],
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
      const display = val == null ? '—' : val;
      html += '<div class="modal-field"><span class="lbl">' + label + '</span><span class="' + cls + '">' + display + '</span></div>';
    }
    html += '</div></div>';
  }

  // Underlyings table
  if (d.underlyings && d.underlyings.length > 0) {
    html += '<div class="modal-section"><h4>Underlyings</h4><table class="modal-table"><thead><tr><th>Security ID</th><th>Symbol</th></tr></thead><tbody>';
    for (const u of d.underlyings) {
      html += '<tr><td>' + u.securityId + '</td><td>' + (u.symbol || '—') + '</td></tr>';
    }
    html += '</tbody></table></div>';
  }

  // Legs table
  if (d.legs && d.legs.length > 0) {
    html += '<div class="modal-section"><h4>Legs</h4><table class="modal-table"><thead><tr><th>Security ID</th><th>Symbol</th><th>Ratio Qty</th><th>Security Type</th><th>Side</th></tr></thead><tbody>';
    for (const l of d.legs) {
      const ratioDisplay = l.ratioQty != null ? (l.ratioQty / 1e7).toFixed(7).replace(/0+$/, '').replace(/\.$/, '') : '—';
      html += '<tr><td>' + l.securityId + '</td><td>' + (l.symbol || '—') + '</td><td>' + ratioDisplay + '</td><td>' + enumLabel(SECURITY_TYPE_NAMES, l.securityType) + '</td><td>' + (enumLabel(LEG_SIDE_NAMES, l.side) || '—') + '</td></tr>';
    }
    html += '</tbody></table></div>';
  }

  // Instrument Attributes
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
  if (view.subs.length === 0) return;
  const currentIdx = view.subs.findIndex(s => s.id === view.selectedId);
  let next = currentIdx + delta;
  if (next < 0) next = view.subs.length - 1;
  if (next >= view.subs.length) next = 0;
  selectSubscription(view.subs[next].id);
}

document.addEventListener('keydown', (e) => {
  // Escape closes modal
  if (e.key === 'Escape') { closeModal(); return; }

  // Ctrl+I toggles instrument detail
  if (e.ctrlKey && e.key === 'i') {
    e.preventDefault();
    const modal = $('instrumentModal');
    if (modal.classList.contains('hidden')) showInstrumentDetail();
    else closeModal();
    return;
  }

  // Alt+↑/↓ navigate subscriptions
  if (e.altKey && e.key === 'ArrowUp') { e.preventDefault(); navigateSubscription(-1); return; }
  if (e.altKey && e.key === 'ArrowDown') { e.preventDefault(); navigateSubscription(1); return; }
});

// ── Init ──
setStatus('disconnected');
startHealthPolling();
worker.postMessage({ cmd: 'connect', url: $('wsUrl').value.trim() });
