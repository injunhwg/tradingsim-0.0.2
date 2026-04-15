import {
  STORAGE_KEYS,
  RealtimeClient,
  apiFetch,
  clearStorage,
  debounce,
  dollarsToCents,
  formatErrorMessage,
  formatDuration,
  formatMoney,
  formatOrderSide,
  formatOrderStatus,
  formatOrderType,
  formatPercentFromBps,
  formatPrice,
  formatSessionStatus,
  formatStockLabel,
  formatTime,
  hidePanel,
  makeIdempotencyKey,
  readStorage,
  renderListItems,
  renderOrderBookRows,
  renderTableRows,
  setConnectionPill,
  showPanel,
  writeStorage
} from '/app.js';

const dom = {
  joinPanel: document.querySelector('#student-join-panel'),
  joinForm: document.querySelector('#student-join-form'),
  dashboard: document.querySelector('#student-dashboard'),
  logout: document.querySelector('#student-logout'),
  error: document.querySelector('#student-error'),
  toast: document.querySelector('#student-toast'),
  connection: document.querySelector('#student-connection'),
  stockSelect: document.querySelector('#student-stock-select'),
  selectedStock: document.querySelector('#student-selected-stock'),
  orderForm: document.querySelector('#student-order-form'),
  orderStock: document.querySelector('#order-stock'),
  orderType: document.querySelector('#order-type'),
  orderPriceWrap: document.querySelector('#order-price-wrap'),
  cash: document.querySelector('#student-cash'),
  shares: document.querySelector('#student-shares'),
  portfolio: document.querySelector('#student-portfolio'),
  bestBid: document.querySelector('#student-best-bid'),
  bestAsk: document.querySelector('#student-best-ask'),
  sessionState: document.querySelector('#student-session-state'),
  timeRemaining: document.querySelector('#student-time-remaining'),
  openOrderCount: document.querySelector('#open-order-count'),
  openOrders: document.querySelector('#student-open-orders'),
  fills: document.querySelector('#student-fills'),
  orderBook: document.querySelector('#student-order-book'),
  trades: document.querySelector('#student-trades'),
  announcements: document.querySelector('#student-announcements'),
  peekStatus: document.querySelector('#peek-status'),
  buyPeek: document.querySelector('#buy-peek'),
  peekList: document.querySelector('#peek-list'),
  liquidation: document.querySelector('#student-liquidation'),
  leaderboard: document.querySelector('#student-leaderboard')
};

const state = {
  auth: readStorage(STORAGE_KEYS.student),
  me: null,
  session: null,
  stocks: [],
  selectedStockId: null,
  stockSnapshots: [],
  account: null,
  orders: [],
  recentFills: [],
  announcements: [],
  features: null,
  privatePeeks: [],
  liquidation: null,
  leaderboard: [],
  sessionSnapshotAt: 0
};

let realtimeClient = null;

function setStatus(status) {
  const labels = {
    connecting: '연결 중',
    connected: '실시간 연결됨',
    reconnecting: '다시 연결 중',
    disconnected: '오프라인',
    error: '네트워크 오류'
  };

  setConnectionPill(dom.connection, status, labels[status] || '오프라인');
}

function showToast(message) {
  showPanel(dom.toast, message);
  window.clearTimeout(showToast.timer);
  showToast.timer = window.setTimeout(() => hidePanel(dom.toast), 3000);
}

function showError(message) {
  showPanel(dom.error, message);
}

function clearError() {
  hidePanel(dom.error);
}

function syncSelectedStock(preferredStockId = state.selectedStockId) {
  const availableIds = new Set(state.stocks.map((stock) => stock.id));
  state.selectedStockId = availableIds.has(preferredStockId) ? preferredStockId : state.stocks[0]?.id || null;

  const selectedValue = state.selectedStockId ? String(state.selectedStockId) : '';
  if (dom.stockSelect.value !== selectedValue) {
    dom.stockSelect.value = selectedValue;
  }
  if (dom.orderStock.value !== selectedValue) {
    dom.orderStock.value = selectedValue;
  }
}

function renderStockOptions() {
  const options = state.stocks
    .map((stock) => `<option value="${stock.id}">${formatStockLabel(stock)}</option>`)
    .join('');

  dom.stockSelect.innerHTML = options;
  dom.orderStock.innerHTML = options;
  syncSelectedStock(state.selectedStockId);
}

function getSelectedStock() {
  return state.stocks.find((stock) => stock.id === state.selectedStockId) || state.stocks[0] || null;
}

function getSelectedSnapshot() {
  return state.stockSnapshots.find((snapshot) => snapshot.stock?.id === state.selectedStockId) || state.stockSnapshots[0] || null;
}

function getHolding(sessionStockId) {
  return state.account?.holdings?.find((holding) => holding.sessionStockId === sessionStockId) || null;
}

function getMarkPrice(sessionStockId) {
  const stock = state.stocks.find((entry) => entry.id === sessionStockId);
  const snapshot = state.stockSnapshots.find((entry) => entry.stock?.id === sessionStockId);
  return snapshot?.market?.markPriceCents ?? snapshot?.market?.lastTradePriceCents ?? stock?.referencePriceCents ?? 0;
}

function computePortfolioValue() {
  const holdingsValue = (state.account?.holdings || []).reduce(
    (sum, holding) => sum + holding.positionQty * getMarkPrice(holding.sessionStockId),
    0
  );

  return (state.account?.cashCents || 0) + holdingsValue;
}

function formatStockPrefix(item) {
  const label = formatStockLabel(item);
  return label === '-' ? '' : `${label} | `;
}

function applyOrderBook(snapshot) {
  state.session = snapshot.session || state.session;
  state.stocks = snapshot.stocks || state.stocks;
  state.stockSnapshots = snapshot.stockSnapshots || [];
  state.announcements = snapshot.announcements || [];
  state.liquidation = snapshot.liquidation || state.liquidation;
  renderStockOptions();
  state.sessionSnapshotAt = Date.now();
}

function applyAccount(accountState) {
  state.session = accountState.session || state.session;
  state.stocks = accountState.stocks || state.stocks;
  state.account = accountState.account || state.account;
  state.orders = accountState.orders || [];
  state.recentFills = accountState.recentFills || [];
  state.privatePeeks = accountState.privatePeeks || [];
  renderStockOptions();
}

function applyLeaderboard(leaderboardState) {
  state.stocks = leaderboardState.stocks || state.stocks;
  state.liquidation = leaderboardState.liquidation || state.liquidation;
  state.leaderboard = leaderboardState.leaderboard || [];
  renderStockOptions();
}

function replaceStockSnapshots(stockSnapshots) {
  if (!Array.isArray(stockSnapshots)) {
    return;
  }

  state.stockSnapshots = stockSnapshots;
}

function getLiveRemainingSeconds() {
  const baseRemaining = Number(state.session?.remainingSeconds || 0);
  if (state.session?.status !== 'OPEN') {
    return baseRemaining;
  }

  const elapsedSinceSnapshot = Math.floor((Date.now() - state.sessionSnapshotAt) / 1000);
  return Math.max(0, baseRemaining - elapsedSinceSnapshot);
}

function renderSummary() {
  const selectedStock = getSelectedStock();
  const selectedSnapshot = getSelectedSnapshot();
  const holding = getHolding(selectedStock?.id);

  dom.selectedStock.textContent = formatStockLabel(selectedStock);
  dom.cash.textContent = formatMoney(state.account?.cashCents || 0);
  dom.shares.textContent = String(holding?.positionQty || 0);
  dom.portfolio.textContent = formatMoney(computePortfolioValue());
  dom.bestBid.textContent = selectedSnapshot?.bids?.[0]
    ? `${formatPrice(selectedSnapshot.bids[0].priceCents)} x ${selectedSnapshot.bids[0].totalQty}`
    : '-';
  dom.bestAsk.textContent = selectedSnapshot?.asks?.[0]
    ? `${formatPrice(selectedSnapshot.asks[0].priceCents)} x ${selectedSnapshot.asks[0].totalQty}`
    : '-';
  dom.sessionState.textContent = formatSessionStatus(state.session?.status);
  dom.timeRemaining.textContent = formatDuration(getLiveRemainingSeconds());
  dom.peekStatus.textContent = state.features?.peeks?.supported
    ? `${formatStockLabel(selectedStock)} 내부정보 구매 1회 가격은 ${formatMoney(state.features.peeks.priceCents)}이며 카드 기여값 3개를 보여줍니다.`
    : '이 버전에서는 내부정보를 사용할 수 없습니다.';
  dom.buyPeek.disabled = !state.features?.peeks?.supported || state.session?.status === 'CLOSED' || !selectedStock;
}

function formatPeekContribution(value) {
  return String(Number(value) || 0);
}

function renderOrders() {
  const openOrders = state.orders.filter((order) => ['OPEN', 'PARTIALLY_FILLED'].includes(order.status));
  dom.openOrderCount.textContent = `미체결 ${openOrders.length}건`;
  renderTableRows(
    dom.openOrders,
    openOrders.map(
      (order) => `
        <tr>
          <td>${formatStockLabel(order)}</td>
          <td>${formatOrderSide(order.side)}</td>
          <td>${formatOrderType(order.orderType)}</td>
          <td>${order.remainingQty} / ${order.originalQty}</td>
          <td>${order.limitPriceCents ? formatPrice(order.limitPriceCents) : '시장가'}</td>
          <td>${formatOrderStatus(order.status)}</td>
          <td><button class="button ghost" data-cancel-order="${order.id}" type="button">취소</button></td>
        </tr>
      `
    ),
    '미체결 주문이 없습니다.'
  );
}

function renderFills() {
  renderTableRows(
    dom.fills,
    state.recentFills.map(
      (fill) => `
        <tr>
          <td>${formatStockLabel(fill)}</td>
          <td>${formatOrderSide(fill.side)}</td>
          <td>${fill.qty}</td>
          <td>${formatPrice(fill.priceCents)}</td>
          <td>${formatTime(fill.executedAt)}</td>
        </tr>
      `
    ),
    '체결내역이 없습니다.'
  );
}

function renderBook() {
  const snapshot = getSelectedSnapshot();
  renderOrderBookRows(dom.orderBook, snapshot?.bids || [], snapshot?.asks || []);
}

function renderTrades() {
  const snapshot = getSelectedSnapshot();
  renderTableRows(
    dom.trades,
    (snapshot?.recentTrades || []).map(
      (trade) => `
        <tr>
          <td>${trade.qty}</td>
          <td>${formatPrice(trade.priceCents)}</td>
          <td>${formatTime(trade.executedAt)}</td>
        </tr>
      `
    ),
    '아직 거래가 없습니다.'
  );
}

function renderAnnouncements() {
  renderListItems(
    dom.announcements,
    state.announcements.map(
      (announcement) => `
        <li>
          <strong>${formatTime(announcement.createdAt)}</strong>
          <div>${formatStockPrefix(announcement)}${announcement.message}</div>
        </li>
      `
    ),
    '공시내역이 없습니다.'
  );
}

function renderPeeks() {
  renderListItems(
    dom.peekList,
    state.privatePeeks.map(
      (peek) => `
        <li>
          <strong>${formatStockPrefix(peek)}${peek.contributions.map((value) => formatPeekContribution(value)).join(', ')}</strong>
          <div class="muted">${formatTime(peek.createdAt)}</div>
        </li>
      `
    ),
    '아직 구매한 내부정보가 없습니다.'
  );
}

function renderLiquidation() {
  if (!state.liquidation?.revealed) {
    dom.liquidation.textContent = '공개 전까지 비공개입니다.';
    return;
  }

  dom.liquidation.innerHTML = (state.liquidation.stocks || [])
    .map(
      (entry) => `
        <div>
          <strong>${formatStockLabel(entry.stock)}:</strong> ${formatMoney(entry.valueCents)}
          <span class="muted"> | 카드: ${(entry.cards || []).map((card) => card.label).join(', ')}</span>
        </div>
      `
    )
    .join('');
}

function renderLeaderboard() {
  renderLiquidation();

  renderTableRows(
    dom.leaderboard,
    state.leaderboard.map(
      (row) => `
        <tr>
          <td>${row.rank}</td>
          <td>${row.displayName}</td>
          <td>${formatMoney(row.portfolioValueCents)}</td>
          <td>${formatPercentFromBps(row.returnBps)}</td>
        </tr>
      `
    ),
    '아직 순위표가 없습니다.'
  );
}

function render() {
  const hasAuth = Boolean(state.auth?.token);
  dom.joinPanel.hidden = hasAuth;
  dom.dashboard.hidden = !hasAuth;
  dom.logout.hidden = !hasAuth;

  if (!hasAuth) {
    return;
  }

  renderSummary();
  renderOrders();
  renderFills();
  renderBook();
  renderTrades();
  renderAnnouncements();
  renderPeeks();
  renderLeaderboard();
}

async function refreshStudentData() {
  const [me, orderBook, accountState, leaderboard] = await Promise.all([
    apiFetch('/api/me', { token: state.auth.token }),
    apiFetch('/api/order-book', { token: state.auth.token }),
    apiFetch('/api/me/account', { token: state.auth.token }),
    apiFetch('/api/leaderboard', { token: state.auth.token })
  ]);

  if (me.principal.role !== 'STUDENT') {
    throw new Error('이 토큰은 수강생용이 아닙니다.');
  }

  state.me = me;
  state.features = me.features;
  state.session = me.principal.session;
  state.stocks = me.stocks || state.stocks;
  state.sessionSnapshotAt = Date.now();
  applyOrderBook(orderBook);
  applyAccount(accountState);
  applyLeaderboard(leaderboard);
  render();
}

const debouncedRefresh = debounce(() => {
  refreshStudentData().catch((error) => {
    showError(formatErrorMessage(error));
  });
}, 200);

function handleRealtimeEvent(event) {
  switch (event.type) {
    case 'orderbook.snapshot':
    case 'orderbook.updated':
      applyOrderBook(event.payload);
      render();
      break;
    case 'account.updated':
      applyAccount(event.payload);
      render();
      break;
    case 'player.fill':
      showToast(
        `체결: ${event.payload.fills
          .map((fill) => `${formatStockPrefix(state.stocks.find((stock) => stock.id === fill.sessionStockId) || fill)}${fill.qty}주 @ ${formatPrice(fill.priceCents)}`)
          .join(', ')}`
      );
      debouncedRefresh();
      break;
    case 'peek.revealed':
      showToast(
        `내부정보: ${formatStockPrefix(state.stocks.find((stock) => stock.id === event.payload.peek.sessionStockId) || event.payload.peek)}${event.payload.peek.contributions
          .map((value) => formatPeekContribution(value))
          .join(', ')}`
      );
      debouncedRefresh();
      break;
    case 'announcement.created':
      state.announcements = [...state.announcements, event.payload.announcement].slice(-20);
      renderAnnouncements();
      break;
    case 'leaderboard.final':
      applyLeaderboard(event.payload);
      render();
      showToast('내재가치가 공개되었습니다.');
      break;
    case 'game.state':
      state.session = {
        ...state.session,
        ...(event.payload.session || {})
      };
      if (Array.isArray(event.payload.stocks)) {
        state.stocks = event.payload.stocks;
        renderStockOptions();
      }
      replaceStockSnapshots(event.payload.stockSnapshots);
      state.sessionSnapshotAt = Date.now();
      render();
      break;
    default:
      break;
  }
}

function connectRealtime() {
  realtimeClient?.disconnect();
  realtimeClient = new RealtimeClient({
    token: state.auth.token,
    onEvent: handleRealtimeEvent,
    onStatusChange: setStatus
  });
  realtimeClient.connect();
}

function resetSession() {
  realtimeClient?.disconnect();
  realtimeClient = null;
  state.auth = null;
  state.me = null;
  state.session = null;
  state.stocks = [];
  state.selectedStockId = null;
  state.stockSnapshots = [];
  state.account = null;
  state.orders = [];
  state.recentFills = [];
  state.announcements = [];
  state.features = null;
  state.privatePeeks = [];
  state.liquidation = null;
  state.leaderboard = [];
  state.sessionSnapshotAt = 0;
  clearStorage(STORAGE_KEYS.student);
  render();
  clearError();
  hidePanel(dom.toast);
  setStatus('disconnected');
}

window.setInterval(() => {
  if (state.auth?.token && state.session?.status === 'OPEN') {
    renderSummary();
  }
}, 1000);

async function restoreStudent() {
  if (!state.auth?.token) {
    render();
    return;
  }

  try {
    clearError();
    await refreshStudentData();
    connectRealtime();
  } catch (error) {
    resetSession();
    showError(formatErrorMessage(error));
  }
}

dom.joinForm.addEventListener('submit', async (event) => {
  event.preventDefault();

  const form = new FormData(dom.joinForm);
  try {
    clearError();
    const joined = await apiFetch('/api/sessions/join', {
      method: 'POST',
      body: {
        joinCode: String(form.get('joinCode') || '').trim().toUpperCase(),
        displayName: String(form.get('displayName') || '').trim()
      }
    });

    state.auth = {
      token: joined.participant.authToken,
      participantId: joined.participant.id,
      sessionId: joined.session.id
    };
    writeStorage(STORAGE_KEYS.student, state.auth);
    await restoreStudent();
  } catch (error) {
    showError(formatErrorMessage(error));
  }
});

dom.logout.addEventListener('click', () => {
  resetSession();
});

dom.stockSelect.addEventListener('change', () => {
  state.selectedStockId = Number.parseInt(dom.stockSelect.value, 10) || null;
  syncSelectedStock(state.selectedStockId);
  render();
});

dom.orderStock.addEventListener('change', () => {
  state.selectedStockId = Number.parseInt(dom.orderStock.value, 10) || null;
  syncSelectedStock(state.selectedStockId);
  render();
});

dom.orderType.addEventListener('change', () => {
  dom.orderPriceWrap.hidden = dom.orderType.value !== 'LIMIT';
});

dom.orderForm.addEventListener('submit', async (event) => {
  event.preventDefault();
  const form = new FormData(dom.orderForm);
  const orderType = String(form.get('orderType')).toUpperCase();
  const limitPriceCents = orderType === 'LIMIT' ? dollarsToCents(form.get('limitPrice')) : undefined;

  try {
    clearError();

    if (orderType === 'LIMIT' && !limitPriceCents) {
      throw new Error('올바른 지정가를 입력하세요.');
    }

    await apiFetch('/api/orders', {
      method: 'POST',
      token: state.auth.token,
      headers: {
        'idempotency-key': makeIdempotencyKey()
      },
      body: {
        sessionStockId: Number.parseInt(form.get('sessionStockId'), 10),
        side: String(form.get('side')).toUpperCase(),
        orderType,
        quantity: Number.parseInt(form.get('quantity'), 10),
        ...(orderType === 'LIMIT' ? { limitPriceCents } : {})
      }
    });

    dom.orderForm.reset();
    document.querySelector('#order-quantity').value = '1';
    dom.orderType.value = 'MARKET';
    dom.orderPriceWrap.hidden = true;
    syncSelectedStock(state.selectedStockId);
    if (realtimeClient === null) {
      await refreshStudentData();
    }
  } catch (error) {
    showError(formatErrorMessage(error));
  }
});

dom.openOrders.addEventListener('click', async (event) => {
  const button = event.target.closest('[data-cancel-order]');
  if (!button) {
    return;
  }

  try {
    clearError();
    await apiFetch(`/api/orders/${button.dataset.cancelOrder}/cancel`, {
      method: 'POST',
      token: state.auth.token
    });

    if (realtimeClient === null) {
      await refreshStudentData();
    }
  } catch (error) {
    showError(formatErrorMessage(error));
  }
});

dom.buyPeek.addEventListener('click', async () => {
  try {
    clearError();
    const result = await apiFetch('/api/peeks', {
      method: 'POST',
      token: state.auth.token,
      body: {
        sessionStockId: state.selectedStockId
      }
    });
    showToast(
      `내부정보 구매: ${formatStockPrefix(result.peek)}${result.peek.contributions
        .map((value) => formatPeekContribution(value))
        .join(', ')}`
    );

    if (realtimeClient === null) {
      await refreshStudentData();
    }
  } catch (error) {
    showError(formatErrorMessage(error));
  }
});

restoreStudent();
