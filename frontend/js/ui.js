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
  const sub = subscriptions.get(state.selectedSecurityId);
  $('bookTitle').textContent = sub ? `Order Book — ${sub.symbol}` : 'Order Book';
  $('infoTitle').textContent = sub ? `Instrument Info — ${sub.symbol}` : 'Instrument Info';
}

export function renderBook() {
  const body = $('bookBody');
  const sub = subscriptions.get(state.selectedSecurityId);
  if (!sub || !sub.book) {
    body.innerHTML = '<div class="empty-msg">No book data</div>';
    return;
  }
  const { rptSeq, bids, asks } = sub.book;
  const maxLevels = 20;
  let html = `<div style="font-size:.7rem;color:var(--muted);margin-bottom:.4rem">rptSeq: ${rptSeq} | ${bids.length} bids, ${asks.length} asks | +${sub.orderCount} events since snapshot</div>`;
  html += '<div class="book-grid">';

  html += '<div class="book-side"><h4>Bids</h4><table><tr><th>Price</th><th>Qty</th><th>#</th></tr>';
  for (let i = 0; i < Math.min(bids.length, maxLevels); i++) {
    const b = bids[i];
    html += `<tr><td class="bid-price">${formatPrice(b.price)}</td><td>${formatQty(b.qty)}</td><td>${b.count}</td></tr>`;
  }
  if (bids.length > maxLevels) html += `<tr><td colspan="3" style="color:var(--muted);text-align:center">... ${bids.length - maxLevels} more</td></tr>`;
  if (bids.length === 0) html += '<tr><td colspan="3" style="color:var(--muted);text-align:center">empty</td></tr>';
  html += '</table></div>';

  html += '<div class="book-side"><h4>Asks</h4><table><tr><th>Price</th><th>Qty</th><th>#</th></tr>';
  for (let i = 0; i < Math.min(asks.length, maxLevels); i++) {
    const a = asks[i];
    html += `<tr><td class="ask-price">${formatPrice(a.price)}</td><td>${formatQty(a.qty)}</td><td>${a.count}</td></tr>`;
  }
  if (asks.length > maxLevels) html += `<tr><td colspan="3" style="color:var(--muted);text-align:center">... ${asks.length - maxLevels} more</td></tr>`;
  if (asks.length === 0) html += '<tr><td colspan="3" style="color:var(--muted);text-align:center">empty</td></tr>';
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
