// Shared application state.
// Mutable objects — modules import and mutate directly.

export const state = {
  ws: null,
  selectedSecurityId: null,
  autoReconnect: true,
  reconnectAttempts: 0,
  reconnectTimer: null,
  healthData: null,
};

// securityId (string) → { symbol, flags, securityId, book, info, trades, orderCount, tradeCount }
export const subscriptions = new Map();

export const stats = { msgs: 0, books: 0, info: 0, orders: 0, trades: 0 };
