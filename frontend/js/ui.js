// DOM helpers, formatting, and rendering with DOM pooling.
// All render functions are parameterized — no global state imports.
// Main thread only: receives pre-computed data from worker, updates DOM pools.

import { INFO_FIELDS, PRICE_FIELDS, flagsStr } from './protocol.js';

export const $ = (id) => document.getElementById(id);

export function formatPrice(mantissa) {
  const dec = parseInt($('priceDecimals').value) || 0;
  if (dec === 0) return mantissa.toLocaleString();
  const divisor = Math.pow(10, dec);
  return (mantissa / divisor).toFixed(dec);
}

export function formatQty(v) { return v.toLocaleString(); }

// ── Connection status ──

export function setStatus(s) {
  const dot = $('statusDot');
  const text = $('statusText');
  if (s === 'connected')       { dot.className = 'dot on';  text.textContent = 'Connected'; }
  else if (s === 'waiting')    { dot.className = 'dot wait'; text.textContent = 'Waiting for feed...'; }
  else if (s === 'connecting') { dot.className = 'dot off'; text.textContent = 'Connecting...'; }
  else                         { dot.className = 'dot off'; text.textContent = 'Disconnected'; }
}

// ── DOM Pool infrastructure ──

const MAX_BOOK_LEVELS = 15;
const MAX_TRADES = 50;
const MAX_RANK_ITEMS = 10;

// Book pool
let bookPool = null;

function getBookPool() {
  if (bookPool) return bookPool;
  const body = $('bookBody');
  body.innerHTML = '';

  const summary = document.createElement('div');
  summary.style.cssText = 'font-size:.7rem;color:var(--muted);margin-bottom:.3rem';
  body.appendChild(summary);

  const spread = document.createElement('div');
  spread.className = 'spread-bar';
  spread.style.display = 'none';
  const sBid = document.createElement('span');
  sBid.className = 'bid-price';
  const sAsk = document.createElement('span');
  sAsk.className = 'ask-price';
  const sSpread = document.createElement('strong');
  spread.appendChild(document.createTextNode('BBO: '));
  spread.appendChild(sBid);
  spread.appendChild(document.createTextNode(' / '));
  spread.appendChild(sAsk);
  spread.appendChild(document.createTextNode('\u00a0\u00a0Spread: '));
  spread.appendChild(sSpread);
  body.appendChild(spread);

  const grid = document.createElement('div');
  grid.className = 'book-grid';
  body.appendChild(grid);

  function createSide(label) {
    const side = document.createElement('div');
    side.className = 'book-side';
    const h4 = document.createElement('h4');
    h4.textContent = label;
    side.appendChild(h4);
    const table = document.createElement('table');
    const thead = document.createElement('tr');
    for (const th of ['Price', 'Qty', '#', '']) {
      const el = document.createElement('th');
      el.textContent = th;
      thead.appendChild(el);
    }
    table.appendChild(thead);
    const rows = [];
    for (let i = 0; i < MAX_BOOK_LEVELS; i++) {
      const tr = document.createElement('tr');
      tr.style.display = 'none';
      const tdPrice = document.createElement('td');
      tdPrice.className = label === 'Bids' ? 'bid-price' : 'ask-price';
      const tdQty = document.createElement('td');
      const tdCount = document.createElement('td');
      const tdDepth = document.createElement('td');
      tdDepth.className = 'depth-cell';
      const bar = document.createElement('div');
      bar.className = label === 'Bids' ? 'depth-bar bid-bar' : 'depth-bar ask-bar';
      tdDepth.appendChild(bar);
      tr.appendChild(tdPrice); tr.appendChild(tdQty); tr.appendChild(tdCount); tr.appendChild(tdDepth);
      table.appendChild(tr);
      rows.push({ tr, tdPrice, tdQty, tdCount, bar });
    }
    const emptyTr = document.createElement('tr');
    emptyTr.style.display = 'none';
    const emptyTd = document.createElement('td');
    emptyTd.colSpan = 4;
    emptyTd.style.cssText = 'color:var(--muted);text-align:center';
    emptyTd.textContent = 'empty';
    emptyTr.appendChild(emptyTd);
    table.appendChild(emptyTr);
    side.appendChild(table);
    grid.appendChild(side);
    return { rows, emptyTr };
  }

  const bidSide = createSide('Bids');
  const askSide = createSide('Asks');

  const emptyMsg = document.createElement('div');
  emptyMsg.className = 'empty-msg';
  emptyMsg.textContent = 'No book data';
  emptyMsg.style.display = 'none';
  body.insertBefore(emptyMsg, body.firstChild);

  bookPool = {
    summary, spread, sBid, sAsk, sSpread, grid, emptyMsg,
    bids: bidSide.rows, asks: askSide.rows,
    bidEmpty: bidSide.emptyTr, askEmpty: askSide.emptyTr,
  };
  return bookPool;
}

// Trade pool
let tradePool = null;

function getTradePool() {
  if (tradePool) return tradePool;
  const body = $('tradeBody');
  body.innerHTML = '';
  const emptyMsg = document.createElement('div');
  emptyMsg.className = 'empty-msg';
  emptyMsg.textContent = 'No trades yet';
  body.appendChild(emptyMsg);
  const table = document.createElement('table');
  table.className = 'trade-table';
  table.style.display = 'none';
  const thead = document.createElement('tr');
  for (const th of ['Time', 'Price', 'Qty']) {
    const el = document.createElement('th');
    el.textContent = th;
    thead.appendChild(el);
  }
  table.appendChild(thead);
  const rows = [];
  for (let i = 0; i < MAX_TRADES; i++) {
    const tr = document.createElement('tr');
    tr.style.display = 'none';
    const tdTime = document.createElement('td'); tdTime.className = 'trade-time';
    const tdPrice = document.createElement('td');
    const tdQty = document.createElement('td');
    tr.appendChild(tdTime); tr.appendChild(tdPrice); tr.appendChild(tdQty);
    table.appendChild(tr);
    rows.push({ tr, tdTime, tdPrice, tdQty });
  }
  body.appendChild(table);
  tradePool = { table, rows, emptyMsg };
  return tradePool;
}

// Info pool
let infoPool = null;

function getInfoPool() {
  if (infoPool) return infoPool;
  const body = $('infoBody');
  body.innerHTML = '';
  const emptyMsg = document.createElement('div');
  emptyMsg.className = 'empty-msg';
  emptyMsg.textContent = 'No info data';
  body.appendChild(emptyMsg);
  const grid = document.createElement('div');
  grid.className = 'info-grid';
  grid.style.display = 'none';
  body.appendChild(grid);
  const items = {};
  for (const field of INFO_FIELDS) {
    const item = document.createElement('div');
    item.className = 'info-item';
    item.style.display = 'none';
    const label = document.createElement('span');
    label.className = 'info-label';
    label.textContent = field;
    const value = document.createElement('span');
    value.className = 'info-value';
    item.appendChild(label); item.appendChild(value);
    grid.appendChild(item);
    items[field] = { item, value };
  }
  infoPool = { grid, items, emptyMsg };
  return infoPool;
}

// Rankings pool
let rankPool = null;

function getRankPool() {
  if (rankPool) return rankPool;
  const list = $('rankList');
  list.innerHTML = '';
  const emptyMsg = document.createElement('div');
  emptyMsg.className = 'empty-msg';
  emptyMsg.textContent = 'Waiting for data...';
  list.appendChild(emptyMsg);
  const connectMsg = document.createElement('div');
  connectMsg.className = 'empty-msg';
  connectMsg.textContent = 'Connect to see rankings';
  connectMsg.style.display = 'none';
  list.appendChild(connectMsg);
  list.addEventListener('click', (e) => {
    const item = e.target.closest('.rank-item');
    if (item && item.dataset.symbol && window.rankingClick) {
      window.rankingClick(item.dataset.symbol);
    }
  });
  const rows = [];
  for (let i = 0; i < MAX_RANK_ITEMS; i++) {
    const div = document.createElement('div');
    div.className = 'rank-item';
    div.style.display = 'none';
    const pos = document.createElement('span'); pos.className = 'rank-pos'; pos.textContent = String(i + 1);
    const sym = document.createElement('span'); sym.className = 'rank-sym';
    const val = document.createElement('span'); val.className = 'rank-val';
    div.appendChild(pos); div.appendChild(sym); div.appendChild(val);
    list.appendChild(div);
    rows.push({ div, sym, val });
  }
  rankPool = { rows, emptyMsg, connectMsg };
  return rankPool;
}

// Subscription list pool
let subPool = null;
const SUB_POOL_SIZE = 30;

function getSubPool() {
  if (subPool) return subPool;
  const ul = $('subList');
  ul.innerHTML = '';
  ul.addEventListener('click', (e) => {
    const li = e.target.closest('.sub-item');
    if (!li) return;
    if (e.target.closest('.sub-unsub')) {
      if (window.doUnsubscribe) window.doUnsubscribe(li.dataset.id);
    } else {
      if (window.selectSubscription) window.selectSubscription(li.dataset.id);
    }
  });
  const rows = [];
  for (let i = 0; i < SUB_POOL_SIZE; i++) {
    const li = document.createElement('li');
    li.className = 'sub-item';
    li.style.display = 'none';
    li.style.cursor = 'pointer';
    const span = document.createElement('span');
    span.style.cssText = 'flex:1;cursor:pointer';
    const strong = document.createElement('strong');
    const flags = document.createElement('span');
    flags.className = 'flags';
    span.appendChild(strong);
    span.appendChild(document.createTextNode(' '));
    span.appendChild(flags);
    const btn = document.createElement('button');
    btn.className = 'sub-unsub';
    btn.title = 'Unsubscribe';
    btn.textContent = '\u2715';
    li.appendChild(span); li.appendChild(btn);
    ul.appendChild(li);
    rows.push({ li, strong, flags });
  }
  subPool = { rows };
  return subPool;
}

// ── Parameterized render functions ──

export function renderSubList(subs, selectedId) {
  const pool = getSubPool();
  $('subEmpty').style.display = (!subs || subs.length === 0) ? '' : 'none';
  if (!subs) subs = [];
  for (let i = 0; i < pool.rows.length; i++) {
    const row = pool.rows[i];
    if (i < subs.length) {
      const s = subs[i];
      row.li.style.display = '';
      row.li.dataset.id = s.id;
      row.li.className = 'sub-item' + (s.id === selectedId ? ' active' : '');
      row.strong.textContent = s.symbol;
      row.flags.textContent = '[' + flagsStr(s.flags) + ']';
    } else {
      row.li.style.display = 'none';
    }
  }
}

export function updateTitles(selectedSymbol) {
  $('bookTitle').textContent = selectedSymbol ? 'Order Book \u2014 ' + selectedSymbol : 'Order Book';
  $('infoTitle').textContent = selectedSymbol ? 'Instrument Info \u2014 ' + selectedSymbol : 'Instrument Info';
  $('tradeTitle').textContent = selectedSymbol ? 'Trades \u2014 ' + selectedSymbol : 'Recent Trades';
}

export function renderBook(bookData) {
  const pool = getBookPool();

  if (!bookData) {
    pool.emptyMsg.style.display = '';
    pool.summary.style.display = 'none';
    pool.spread.style.display = 'none';
    pool.grid.style.display = 'none';
    return;
  }

  pool.emptyMsg.style.display = 'none';
  pool.summary.style.display = '';
  pool.grid.style.display = '';

  const { bids, asks, maxQty, totalBids, totalAsks, totalOrders, orderCount } = bookData;

  pool.summary.textContent = totalBids + 'b/' + totalAsks + 'a | ' + totalOrders + ' orders | +' + orderCount + ' events';

  if (bids.length > 0 && asks.length > 0) {
    pool.spread.style.display = '';
    pool.sBid.textContent = formatPrice(bids[0].price);
    pool.sAsk.textContent = formatPrice(asks[0].price);
    pool.sSpread.textContent = formatPrice(asks[0].price - bids[0].price);
  } else {
    pool.spread.style.display = 'none';
  }

  for (let i = 0; i < MAX_BOOK_LEVELS; i++) {
    const row = pool.bids[i];
    if (i < bids.length) {
      const b = bids[i];
      row.tr.style.display = '';
      row.tdPrice.textContent = formatPrice(b.price);
      row.tdQty.textContent = formatQty(b.qty);
      row.tdCount.textContent = String(b.count);
      row.bar.style.width = (b.qty / maxQty * 100).toFixed(1) + '%';
    } else {
      row.tr.style.display = 'none';
    }
  }
  pool.bidEmpty.style.display = bids.length === 0 ? '' : 'none';

  for (let i = 0; i < MAX_BOOK_LEVELS; i++) {
    const row = pool.asks[i];
    if (i < asks.length) {
      const a = asks[i];
      row.tr.style.display = '';
      row.tdPrice.textContent = formatPrice(a.price);
      row.tdQty.textContent = formatQty(a.qty);
      row.tdCount.textContent = String(a.count);
      row.bar.style.width = (a.qty / maxQty * 100).toFixed(1) + '%';
    } else {
      row.tr.style.display = 'none';
    }
  }
  pool.askEmpty.style.display = asks.length === 0 ? '' : 'none';
}

function tradingStatusName(v) {
  const names = { 2: 'Paused', 4: 'Closed', 17: 'Open', 18: 'Forbidden', 20: 'Unknown', 21: 'Auction', 101: 'FinalClosing' };
  return names[v] || 'Status(' + v + ')';
}

function tradingEventName(v) {
  const names = { 0: 'None', 1: 'Change', 4: 'NewStatus', 101: 'PriceBand' };
  return names[v] || 'Event(' + v + ')';
}

export function renderInfo(infoData) {
  const pool = getInfoPool();
  if (!infoData) {
    pool.emptyMsg.style.display = '';
    pool.grid.style.display = 'none';
    return;
  }
  pool.emptyMsg.style.display = 'none';
  pool.grid.style.display = '';
  for (const field of INFO_FIELDS) {
    const item = pool.items[field];
    if (field in infoData) {
      item.item.style.display = '';
      const raw = infoData[field];
      let display;
      if (field === 'TradingStatus') display = tradingStatusName(raw);
      else if (field === 'TradingEvent') display = tradingEventName(raw);
      else if (PRICE_FIELDS.has(field)) display = formatPrice(raw);
      else display = formatQty(raw);
      item.value.textContent = display;
    } else {
      item.item.style.display = 'none';
    }
  }
}

export function renderTrades(trades) {
  const pool = getTradePool();
  if (!trades || trades.length === 0) {
    pool.emptyMsg.style.display = '';
    pool.table.style.display = 'none';
    return;
  }
  pool.emptyMsg.style.display = 'none';
  pool.table.style.display = '';
  const len = trades.length;
  for (let i = 0; i < MAX_TRADES; i++) {
    const row = pool.rows[i];
    const ti = len - 1 - i;
    if (ti >= 0) {
      const t = trades[ti];
      row.tr.style.display = '';
      row.tdTime.textContent = t.time;
      row.tdPrice.textContent = formatPrice(t.price);
      row.tdPrice.className = t.direction === 'up' ? 'bid-price' : t.direction === 'down' ? 'ask-price' : '';
      row.tdQty.textContent = formatQty(t.qty);
    } else {
      row.tr.style.display = 'none';
    }
  }
}

export function renderRankings(rankingsData, tab, connected) {
  for (const t of ['volume', 'gainers', 'losers']) {
    const btn = $('rankTab_' + t);
    if (btn) btn.className = 'rank-tab' + (t === tab ? ' active' : '');
  }
  const pool = getRankPool();
  const entries = rankingsData ? (rankingsData[tab] || []) : [];
  pool.connectMsg.style.display = !connected ? '' : 'none';
  pool.emptyMsg.style.display = connected && entries.length === 0 ? '' : 'none';
  for (let i = 0; i < pool.rows.length; i++) {
    const row = pool.rows[i];
    if (i < entries.length) {
      const e = entries[i];
      row.div.style.display = '';
      row.div.dataset.symbol = e.symbol;
      row.sym.textContent = e.symbol;
      row.val.textContent = tab === 'volume' ? formatQty(e.value) : formatPrice(e.value);
      row.val.className = 'rank-val' + (tab === 'losers' ? ' ask-price' : tab === 'gainers' ? ' bid-price' : '');
    } else {
      row.div.style.display = 'none';
    }
  }
}

// ── Health widget ──

export function renderHealth(healthData) {
  const el = $('healthInfo');
  if (!el) return;
  if (!healthData) { el.textContent = '\u2014'; return; }
  let parts = [];
  parts.push(healthData.status === 'ready' ? '\ud83d\udfe2 Ready' : '\ud83d\udfe1 ' + (healthData.status || 'unknown'));
  if (healthData.uptime) parts.push('\u23f1 ' + healthData.uptime);
  if (healthData.feedGroups) {
    const groups = Object.entries(healthData.feedGroups).map(([k, v]) => k + ':' + v).join(' ');
    parts.push(groups);
  }
  el.textContent = parts.join(' \u2502 ');
}

// ── Event log (buffered via rAF) ──

const MAX_LOG = 200;
let logCount = 0;
const logBuffer = [];
let logFlushScheduled = false;
let logEnabled = true;

function ts() {
  const d = new Date();
  return d.toTimeString().slice(0, 8) + '.' + String(d.getMilliseconds()).padStart(3, '0');
}

export function addLog(text, cssClass) {
  if (!logEnabled) return;
  logBuffer.push({ text, cssClass, time: ts() });
  if (!logFlushScheduled) {
    logFlushScheduled = true;
    requestAnimationFrame(flushLog);
  }
}

function flushLog() {
  logFlushScheduled = false;
  const body = $('logBody');
  const frag = document.createDocumentFragment();
  for (const entry of logBuffer) {
    const div = document.createElement('div');
    div.className = 'log-entry';
    const tsSpan = document.createElement('span');
    tsSpan.className = 'log-ts';
    tsSpan.textContent = entry.time;
    const msgSpan = document.createElement('span');
    msgSpan.className = entry.cssClass || '';
    msgSpan.textContent = ' ' + entry.text;
    div.appendChild(tsSpan); div.appendChild(msgSpan);
    frag.appendChild(div);
    logCount++;
  }
  logBuffer.length = 0;
  body.appendChild(frag);
  while (logCount > MAX_LOG) { body.removeChild(body.firstChild); logCount--; }
  body.scrollTop = body.scrollHeight;
}

export function clearLog() { $('logBody').innerHTML = ''; logCount = 0; logBuffer.length = 0; }

export function setLogEnabled(enabled) {
  logEnabled = enabled;
  const logArea = document.querySelector('.log-area');
  if (logArea) logArea.classList.toggle('hidden', !enabled);
  if (!enabled) clearLog();
}

// ── Stats bar ──

export function updateStats(statsData) {
  if (!statsData) return;
  $('statMsgs').textContent = statsData.msgs.toLocaleString();
  $('statBooks').textContent = statsData.books.toLocaleString();
  $('statInfo').textContent = statsData.info.toLocaleString();
  $('statOrders').textContent = statsData.orders.toLocaleString();
  $('statTrades').textContent = statsData.trades.toLocaleString();
}
