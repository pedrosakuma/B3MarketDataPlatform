// DOM helpers, formatting, and rendering with DOM pooling.
// All render functions are parameterized — no global state imports.
// Main thread only: receives pre-computed data from worker, updates DOM pools.

import { FIELD_DECIMALS } from './protocol.js';

export const $ = (id) => document.getElementById(id);

const DEFAULT_PRICE_DECIMALS = 4;

export function formatPrice(mantissa, decimals = DEFAULT_PRICE_DECIMALS) {
  const divisor = Math.pow(10, decimals);
  return (mantissa / divisor).toFixed(decimals);
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

const MAX_BOOK_POOL = 50;
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
    for (let i = 0; i < MAX_BOOK_POOL; i++) {
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

// ── Titles ──

export function updateTitles(selectedSymbol) {
  $('bookTitle').textContent = selectedSymbol ? 'Order Book \u2014 ' + selectedSymbol : 'Order Book';
  const chartText = $('chartTitleText');
  if (chartText) {
    // Update only the leading text node, preserving the chartResolution span
    const label = selectedSymbol ? 'Chart \u2014 ' + selectedSymbol + ' ' : 'Chart ';
    if (chartText.firstChild && chartText.firstChild.nodeType === Node.TEXT_NODE) {
      chartText.firstChild.textContent = label;
    }
  }
}

// ── Render functions ──

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

  for (let i = 0; i < pool.bids.length; i++) {
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

  for (let i = 0; i < pool.asks.length; i++) {
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
      row.val.textContent = tab === 'volume' ? formatQty(e.value) : formatPrice(e.value, 8);
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

// ── Stats bar ──

export function updateStats(statsData) {
  if (!statsData) return;
  $('statMsgs').textContent = statsData.msgs.toLocaleString();
  $('statBooks').textContent = statsData.books.toLocaleString();
  $('statInfo').textContent = statsData.info.toLocaleString();
  $('statOrders').textContent = statsData.orders.toLocaleString();
  $('statTrades').textContent = statsData.trades.toLocaleString();
}

// ── Toast notifications ──

const MAX_TOASTS = 5;
const TOAST_DURATION = 4000;

const TOAST_CLASS_MAP = {
  'log-sub-ok': 'toast-ok',
  'log-error': 'toast-error',
  'log-info': 'toast-info',
  'log-book': 'toast-info',
};

export function showToast(text, cssClass) {
  const container = $('toastContainer');
  if (!container) return;

  const toast = document.createElement('div');
  toast.className = 'toast ' + (TOAST_CLASS_MAP[cssClass] || 'toast-default');
  toast.textContent = text;
  container.appendChild(toast);

  // Trigger reflow then animate in
  void toast.offsetHeight;
  toast.classList.add('show');

  // Limit visible toasts
  while (container.children.length > MAX_TOASTS) {
    container.removeChild(container.firstChild);
  }

  setTimeout(() => {
    toast.classList.remove('show');
    toast.classList.add('fade-out');
    setTimeout(() => { if (toast.parentNode) toast.remove(); }, 300);
  }, TOAST_DURATION);
}

// ── Subscriptions table (DOM-pooled) ──

const subsState = { rows: new Map(), columnsKey: '' };

function tradingStatusName(v) {
  const names = { 2: 'Paused', 4: 'Closed', 17: 'Open', 18: 'Forbidden', 20: 'Unknown', 21: 'Auction', 101: 'FinalClosing' };
  return names[v] || ('(' + v + ')');
}

function tradingEventName(v) {
  const names = { 0: 'None', 1: 'Change', 4: 'NewStatus', 101: 'PriceBand' };
  return names[v] || ('(' + v + ')');
}

function formatField(field, value) {
  if (value == null || value === undefined) return '\u2014';
  if (field === 'TradingStatus') return tradingStatusName(value);
  if (field === 'TradingEvent') return tradingEventName(value);
  const dec = FIELD_DECIMALS[field];
  if (dec !== undefined) return formatPrice(value, dec);
  return formatQty(value);
}

export function renderSubsTable(allInfo, columns, selectedId) {
  const empty = $('subsEmpty');
  const wrap = $('subsTableWrap');

  if (!allInfo || allInfo.length === 0) {
    empty.style.display = '';
    wrap.style.display = 'none';
    return;
  }

  empty.style.display = 'none';
  wrap.style.display = '';

  const thead = $('subsTableHead');
  const tbody = $('subsTableRows');
  const colsKey = columns.join(',');

  // Rebuild header + clear row cache when columns change
  if (subsState.columnsKey !== colsKey) {
    subsState.columnsKey = colsKey;
    thead.innerHTML = '';
    const tr = document.createElement('tr');
    const thSym = document.createElement('th'); thSym.textContent = 'Symbol'; tr.appendChild(thSym);
    for (const col of columns) {
      const th = document.createElement('th'); th.textContent = col; tr.appendChild(th);
    }
    const thAct = document.createElement('th'); tr.appendChild(thAct);
    thead.appendChild(tr);
    for (const [, row] of subsState.rows) row.tr.remove();
    subsState.rows.clear();
  }

  const currentIds = new Set();

  for (const item of allInfo) {
    currentIds.add(item.id);
    let row = subsState.rows.get(item.id);

    if (!row) {
      const tr = document.createElement('tr');
      tr.dataset.id = item.id;
      const tdSym = document.createElement('td');
      tdSym.className = 'sym-cell';
      tr.appendChild(tdSym);

      const cells = {};
      for (const col of columns) {
        const td = document.createElement('td');
        tr.appendChild(td);
        cells[col] = td;
      }

      const tdAct = document.createElement('td');
      const btnDetail = document.createElement('button');
      btnDetail.className = 'detail-btn';
      btnDetail.textContent = '\uD83D\uDD0D';
      btnDetail.title = 'Detail';
      const btnUnsub = document.createElement('button');
      btnUnsub.className = 'unsub-btn';
      btnUnsub.textContent = '\u2715';
      btnUnsub.title = 'Unsubscribe';
      tdAct.appendChild(btnDetail);
      tdAct.appendChild(btnUnsub);
      tr.appendChild(tdAct);

      tbody.appendChild(tr);
      row = { tr, tdSym, cells, btnDetail, btnUnsub };
      subsState.rows.set(item.id, row);
    }

    row.tdSym.textContent = item.symbol;
    row.btnDetail.dataset.symbol = item.symbol;
    row.btnUnsub.dataset.id = item.id;
    row.tr.className = item.id === selectedId ? 'active' : '';

    for (const col of columns) {
      const td = row.cells[col];
      if (!td) continue;
      const val = item.info[col];
      td.textContent = formatField(col, val);
      if (col === 'NetChange' && val != null) {
        td.className = val > 0 ? 'bid-price' : val < 0 ? 'ask-price' : '';
      }
    }
  }

  // Remove stale rows
  for (const [id, row] of subsState.rows) {
    if (!currentIds.has(id)) {
      row.tr.remove();
      subsState.rows.delete(id);
    }
  }
}

// ── Trade tape (DOM-pooled, mirrors getBookPool pattern) ──

// Trade tape: pool of MAX_TAPE_ROWS <tr> elements held in display order
// (newest at index 0). We accept either a full snapshot or an append delta:
//   - { full: true, items: [...] }  oldest-first (matches sub.recentTrades)
//   - { append: [...] }             oldest-first new trades since last frame
// On append we mutate only the rows that change and use insertBefore to move
// the recycled row to the top of the tbody — no re-render of unchanged rows.
const MAX_TAPE_ROWS = 50;
let tradesPool = null;
let tradesCount = 0; // number of pool rows currently bound to a real trade

function getTradesPool() {
  if (tradesPool) return tradesPool;
  const tbody = $('tradesRows');
  tbody.innerHTML = '';
  const rows = [];
  for (let i = 0; i < MAX_TAPE_ROWS; i++) {
    const tr = document.createElement('tr');
    tr.style.display = 'none';
    const tdTime = document.createElement('td');
    const tdSide = document.createElement('td');
    const tdPrice = document.createElement('td');
    const tdQty = document.createElement('td');
    const tdId = document.createElement('td');
    tr.append(tdTime, tdSide, tdPrice, tdQty, tdId);
    tbody.appendChild(tr);
    rows.push({ tr, tdTime, tdSide, tdPrice, tdQty, tdId });
  }
  tradesPool = { rows, tbody };
  return tradesPool;
}

function bindTradeRow(row, t) {
  if (row.tr.style.display === 'none') row.tr.style.display = '';
  row.tdTime.textContent = formatTimeOfDay(t.time);
  let sideText, sideClass;
  if (t.side === 1)      { sideText = 'BUY';  sideClass = 'trade-buy'; }
  else if (t.side === 2) { sideText = 'SELL'; sideClass = 'trade-sell'; }
  else                   { sideText = '—';    sideClass = 'trade-flat'; }
  row.tdSide.textContent = sideText;
  row.tdSide.className = sideClass;
  row.tdPrice.className = sideClass;
  row.tdPrice.textContent = formatPrice(t.price, 4);
  row.tdQty.textContent = formatQty(t.qty);
  row.tdId.textContent = String(t.tradeId);
}

function formatTimeOfDay(ts) {
  const d = new Date(ts);
  const hh = String(d.getHours()).padStart(2, '0');
  const mm = String(d.getMinutes()).padStart(2, '0');
  const ss = String(d.getSeconds()).padStart(2, '0');
  const mss = String(d.getMilliseconds()).padStart(3, '0');
  return `${hh}:${mm}:${ss}.${mss}`;
}

export function renderTrades(tradesData) {
  const empty = $('tradesEmpty');
  const wrap = $('tradesWrap');

  if (!tradesData) return;

  if (tradesData.full) {
    const items = tradesData.items || [];
    if (items.length === 0) {
      empty.style.display = '';
      wrap.style.display = 'none';
      const pool = getTradesPool();
      for (let i = 0; i < tradesCount; i++) pool.rows[i].tr.style.display = 'none';
      tradesCount = 0;
      return;
    }
    empty.style.display = 'none';
    wrap.style.display = '';
    const pool = getTradesPool();
    const n = Math.min(items.length, MAX_TAPE_ROWS);
    for (let i = 0; i < n; i++) {
      bindTradeRow(pool.rows[i], items[items.length - 1 - i]); // newest first
    }
    for (let i = n; i < tradesCount; i++) pool.rows[i].tr.style.display = 'none';
    tradesCount = n;
    return;
  }

  // Append delta: rotate rows so newest are at top. Process oldest-first so
  // the most recent trade ends up at index 0.
  const appends = tradesData.append;
  if (!appends || appends.length === 0) return;
  empty.style.display = 'none';
  wrap.style.display = '';
  const pool = getTradesPool();
  const tbody = pool.tbody;
  for (let k = 0; k < appends.length; k++) {
    const t = appends[k];
    let row;
    if (tradesCount < MAX_TAPE_ROWS) {
      // Reuse the next hidden row at position `tradesCount`.
      row = pool.rows[tradesCount];
      pool.rows.splice(tradesCount, 1);
      tradesCount++;
    } else {
      // Evict the oldest visible row (last position).
      row = pool.rows[MAX_TAPE_ROWS - 1];
      pool.rows.splice(MAX_TAPE_ROWS - 1, 1);
    }
    bindTradeRow(row, t);
    pool.rows.unshift(row);
    tbody.insertBefore(row.tr, tbody.firstChild);
  }
}

// ── Auction banner (no pool needed; few elements, infrequent updates) ──

let auctionEls = null;

function getAuctionEls() {
  if (auctionEls) return auctionEls;
  const banner = $('auctionBanner');
  banner.innerHTML = '';
  const tag = document.createElement('span');
  tag.className = 'au-tag';
  const price = document.createElement('span');
  const size = document.createElement('span');
  const imb = document.createElement('span');
  banner.append(tag, price, size, imb);
  auctionEls = { banner, tag, price, size, imb };
  return auctionEls;
}

export function renderAuction(auctionData) {
  const els = getAuctionEls();
  if (!auctionData) {
    if (!els.banner.classList.contains('hidden')) els.banner.classList.add('hidden');
    return;
  }
  els.banner.classList.remove('hidden');
  els.tag.textContent = tradingStatusName(auctionData.tradingStatus).toUpperCase();
  els.price.innerHTML = '<span class="au-label">Theor. Price</span>'
    + (auctionData.theoreticalPrice != null ? formatPrice(auctionData.theoreticalPrice, 4) : '\u2014');
  els.size.innerHTML = '<span class="au-label">Theor. Size</span>'
    + (auctionData.theoreticalSize != null ? formatQty(auctionData.theoreticalSize) : '\u2014');
  els.imb.innerHTML = '<span class="au-label">Imbalance</span>'
    + (auctionData.imbalance != null ? formatQty(auctionData.imbalance) : '\u2014');
}
