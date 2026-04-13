// DOM helpers, formatting, and rendering.

import { INFO_FIELDS, PRICE_FIELDS, flagsStr } from './protocol.js';
import { subscriptions, state, stats } from './state.js';

export const $ = (id) => document.getElementById(id);

export function secIdStr(bigint) { return bigint.toString(); }

export function ts() {
  const d = new Date();
  return d.toTimeString().slice(0, 8) + '.' + String(d.getMilliseconds()).padStart(3, '0');
}

export function formatPrice(mantissa) {
  const dec = parseInt($('priceDecimals').value) || 0;
  if (dec === 0) return mantissa.toLocaleString();
  const divisor = Math.pow(10, dec);
  return (mantissa / divisor).toFixed(Math.min(dec, 4));
}

export function formatQty(v) { return v.toLocaleString(); }

// ── Connection status ──

export function setStatus(s) {
  const dot = $('statusDot');
  const text = $('statusText');
  if (s === 'connected')       { dot.className = 'dot on';  text.textContent = 'Connected'; }
  else if (s === 'connecting') { dot.className = 'dot off'; text.textContent = 'Connecting...'; }
  else                         { dot.className = 'dot off'; text.textContent = 'Disconnected'; }
}

// ── Subscription list ──

export function renderSubList() {
  const ul = $('subList');
  ul.innerHTML = '';
  $('subEmpty').style.display = subscriptions.size === 0 ? '' : 'none';
  for (const [id, sub] of subscriptions) {
    const li = document.createElement('li');
    li.className = 'sub-item' + (id === state.selectedSecurityId ? ' active' : '');
    li.innerHTML = `
      <span onclick="selectSubscription('${id}')" style="flex:1;cursor:pointer">
        <strong>${sub.symbol}</strong>
        <span class="flags">[${flagsStr(sub.flags)}]</span>
      </span>
      <button class="sub-unsub" onclick="doUnsubscribe('${id}')" title="Unsubscribe">✕</button>`;
    ul.appendChild(li);
  }
}

// ── Panels ──

export function renderSelected() {
  renderBook();
  renderInfo();
  renderTrades();
  const sub = subscriptions.get(state.selectedSecurityId);
  $('bookTitle').textContent = sub ? `Order Book — ${sub.symbol}` : 'Order Book';
  $('infoTitle').textContent = sub ? `Instrument Info — ${sub.symbol}` : 'Instrument Info';
  $('tradeTitle').textContent = sub ? `Trades — ${sub.symbol}` : 'Recent Trades';
}

export function renderBook() {
  const body = $('bookBody');
  const sub = subscriptions.get(state.selectedSecurityId);
  if (!sub || !sub.book) {
    body.innerHTML = '<div class="empty-msg">No book data</div>';
    return;
  }
  const { rptSeq, bids, asks } = sub.book;
  const maxLevels = 15;

  // Calculate max qty for depth bars
  const allLevels = [...bids.slice(0, maxLevels), ...asks.slice(0, maxLevels)];
  const maxQty = allLevels.reduce((m, l) => Math.max(m, l.qty), 1);

  // Spread indicator
  let spreadHtml = '';
  if (bids.length > 0 && asks.length > 0) {
    const bestBid = bids[0].price;
    const bestAsk = asks[0].price;
    const spread = bestAsk - bestBid;
    spreadHtml = `<div class="spread-bar">BBO: <span class="bid-price">${formatPrice(bestBid)}</span> / <span class="ask-price">${formatPrice(bestAsk)}</span> &nbsp; Spread: <strong>${formatPrice(spread)}</strong></div>`;
  }

  let html = `<div style="font-size:.7rem;color:var(--muted);margin-bottom:.3rem">rptSeq: ${rptSeq} | ${bids.length}b/${asks.length}a | +${sub.orderCount} events</div>`;
  html += spreadHtml;
  html += '<div class="book-grid">';

  // Bids with depth bars
  html += '<div class="book-side"><h4>Bids</h4><table><tr><th>Price</th><th>Qty</th><th>#</th><th></th></tr>';
  for (let i = 0; i < Math.min(bids.length, maxLevels); i++) {
    const b = bids[i];
    const pct = (b.qty / maxQty * 100).toFixed(1);
    html += `<tr><td class="bid-price">${formatPrice(b.price)}</td><td>${formatQty(b.qty)}</td><td>${b.count}</td>
      <td class="depth-cell"><div class="depth-bar bid-bar" style="width:${pct}%"></div></td></tr>`;
  }
  if (bids.length === 0) html += '<tr><td colspan="4" style="color:var(--muted);text-align:center">empty</td></tr>';
  html += '</table></div>';

  // Asks with depth bars
  html += '<div class="book-side"><h4>Asks</h4><table><tr><th>Price</th><th>Qty</th><th>#</th><th></th></tr>';
  for (let i = 0; i < Math.min(asks.length, maxLevels); i++) {
    const a = asks[i];
    const pct = (a.qty / maxQty * 100).toFixed(1);
    html += `<tr><td class="ask-price">${formatPrice(a.price)}</td><td>${formatQty(a.qty)}</td><td>${a.count}</td>
      <td class="depth-cell"><div class="depth-bar ask-bar" style="width:${pct}%"></div></td></tr>`;
  }
  if (asks.length === 0) html += '<tr><td colspan="4" style="color:var(--muted);text-align:center">empty</td></tr>';
  html += '</table></div>';

  html += '</div>';
  body.innerHTML = html;
}

function tradingStatusName(v) {
  const names = { 2: 'Trading', 4: 'Forbidden', 17: 'Auction', 18: 'Closing', 20: 'Open', 21: 'PreClose', 101: 'FinalClosing' };
  return names[v] || `Status(${v})`;
}

function tradingEventName(v) {
  const names = { 0: 'None', 1: 'Change', 4: 'NewStatus', 101: 'PriceBand' };
  return names[v] || `Event(${v})`;
}

export function renderInfo() {
  const body = $('infoBody');
  const sub = subscriptions.get(state.selectedSecurityId);
  if (!sub || Object.keys(sub.info).length === 0) {
    body.innerHTML = '<div class="empty-msg">No info data</div>';
    return;
  }
  let html = '<div class="info-grid">';
  for (const field of INFO_FIELDS) {
    if (!(field in sub.info)) continue;
    const raw = sub.info[field];
    let display;
    if (field === 'TradingStatus') display = tradingStatusName(raw);
    else if (field === 'TradingEvent') display = tradingEventName(raw);
    else if (PRICE_FIELDS.has(field)) display = formatPrice(raw);
    else display = formatQty(raw);
    html += `<div class="info-item"><span class="info-label">${field}</span><span class="info-value">${display}</span></div>`;
  }
  html += '</div>';
  body.innerHTML = html;
}

// ── Trade tape ──

const MAX_TRADES = 50;

export function renderTrades() {
  const body = $('tradeBody');
  const sub = subscriptions.get(state.selectedSecurityId);
  if (!sub || !sub.trades || sub.trades.length === 0) {
    body.innerHTML = '<div class="empty-msg">No trades yet</div>';
    return;
  }
  let html = '<table class="trade-table"><tr><th>Time</th><th>Price</th><th>Qty</th></tr>';
  // Show newest first
  for (let i = sub.trades.length - 1; i >= 0; i--) {
    const t = sub.trades[i];
    const priceClass = t.direction === 'up' ? 'bid-price' : t.direction === 'down' ? 'ask-price' : '';
    html += `<tr><td class="trade-time">${t.time}</td><td class="${priceClass}">${formatPrice(t.price)}</td><td>${formatQty(t.qty)}</td></tr>`;
  }
  html += '</table>';
  body.innerHTML = html;
}

export function addTrade(sub, price, qty) {
  if (!sub.trades) sub.trades = [];
  const prevPrice = sub.trades.length > 0 ? sub.trades[sub.trades.length - 1].price : price;
  const direction = price > prevPrice ? 'up' : price < prevPrice ? 'down' : 'flat';
  sub.trades.push({ time: ts(), price, qty, direction });
  if (sub.trades.length > MAX_TRADES) sub.trades.shift();
}

// ── Health widget ──

export function renderHealth() {
  const el = $('healthInfo');
  if (!el) return;
  const h = state.healthData;
  if (!h) { el.textContent = '—'; return; }

  let parts = [];
  parts.push(h.status === 'ready' ? '🟢 Ready' : '🟡 ' + (h.status || 'unknown'));
  if (h.uptime) parts.push('⏱ ' + h.uptime);
  if (h.feedGroups) {
    const groups = Object.entries(h.feedGroups).map(([k, v]) => `${k}:${v}`).join(' ');
    parts.push(groups);
  }
  el.textContent = parts.join(' │ ');
}

// ── Event log ──

const MAX_LOG = 500;
let logCount = 0;

export function addLog(text, cssClass) {
  const body = $('logBody');
  const div = document.createElement('div');
  div.className = 'log-entry';
  div.innerHTML = `<span class="log-ts">${ts()}</span> <span class="${cssClass}">${text}</span>`;
  body.appendChild(div);
  logCount++;
  while (logCount > MAX_LOG) { body.removeChild(body.firstChild); logCount--; }
  body.scrollTop = body.scrollHeight;
}

export function clearLog() { $('logBody').innerHTML = ''; logCount = 0; }

// ── Stats bar ──

export function updateStats() {
  $('statMsgs').textContent = stats.msgs.toLocaleString();
  $('statBooks').textContent = stats.books.toLocaleString();
  $('statInfo').textContent = stats.info.toLocaleString();
  $('statOrders').textContent = stats.orders.toLocaleString();
  $('statTrades').textContent = stats.trades.toLocaleString();
}
