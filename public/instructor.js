import {
  STORAGE_KEYS,
  RealtimeClient,
  apiFetch,
  clearStorage,
  debounce,
  dollarsToCents,
  formatErrorMessage,
  formatDuration,
  formatCardColor,
  formatMoney,
  formatPublicInfoType,
  formatScheduleStatus,
  formatSessionStatus,
  formatPercentFromBps,
  formatPrice,
  formatSignedMoney,
  formatTime,
  hidePanel,
  readStorage,
  renderListItems,
  renderOrderBookRows,
  renderTableRows,
  setConnectionPill,
  showPanel,
  writeStorage
} from '/app.js';

const dom = {
  setup: document.querySelector('#instructor-setup'),
  dashboard: document.querySelector('#instructor-dashboard'),
  error: document.querySelector('#instructor-error'),
  toast: document.querySelector('#instructor-toast'),
  connection: document.querySelector('#instructor-connection'),
  logout: document.querySelector('#instructor-logout'),
  bootstrapForm: document.querySelector('#bootstrap-form'),
  restoreTokenForm: document.querySelector('#restore-token-form'),
  bootstrapSecret: document.querySelector('#bootstrap-secret'),
  sessionNameInput: document.querySelector('#session-name'),
  referencePriceInput: document.querySelector('#reference-price'),
  restoreTokenInput: document.querySelector('#restore-token'),
  openButton: document.querySelector('#session-open'),
  pauseButton: document.querySelector('#session-pause'),
  closeButton: document.querySelector('#session-close'),
  finalizeButton: document.querySelector('#session-finalize'),
  sessionName: document.querySelector('#dashboard-session-name'),
  joinCode: document.querySelector('#dashboard-join-code'),
  sessionState: document.querySelector('#dashboard-session-state'),
  studentCount: document.querySelector('#dashboard-student-count'),
  connectedCount: document.querySelector('#dashboard-connected-count'),
  lastTrade: document.querySelector('#dashboard-last-trade'),
  timeRemaining: document.querySelector('#dashboard-time-remaining'),
  liquidation: document.querySelector('#dashboard-liquidation'),
  cardTotal: document.querySelector('#dashboard-card-total'),
  students: document.querySelector('#dashboard-students'),
  publicInfoSchedule: document.querySelector('#public-info-schedule'),
  cards: document.querySelector('#dashboard-cards'),
  leaderboard: document.querySelector('#dashboard-leaderboard'),
  orderBook: document.querySelector('#dashboard-order-book'),
  trades: document.querySelector('#dashboard-trades'),
  announcements: document.querySelector('#dashboard-announcements')
};

const state = {
  auth: readStorage(STORAGE_KEYS.instructor),
  bootstrapSecret: readStorage(STORAGE_KEYS.bootstrapSecret),
  me: null,
  dashboard: null,
  dashboardSnapshotAt: 0
};

let realtimeClient = null;
let refreshTimer = null;

if (state.bootstrapSecret) {
  dom.bootstrapSecret.value = state.bootstrapSecret.secret;
}

function setStatus(status) {
  const labels = {
    connecting: '연결 중',
    connected: '실시간 연결됨',
    reconnecting: '다시 연결 중',
    disconnected: '오프라인',
    error: '네트워크 문제'
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

function getLiveRemainingSeconds() {
  const remainingSeconds = Number(state.dashboard?.session?.remainingSeconds || 0);
  if (state.dashboard?.session?.status !== 'OPEN') {
    return remainingSeconds;
  }

  const elapsedSinceSnapshot = Math.floor((Date.now() - state.dashboardSnapshotAt) / 1000);
  return Math.max(0, remainingSeconds - elapsedSinceSnapshot);
}

function renderDashboard() {
  const dashboard = state.dashboard;
  const session = dashboard?.session;
  dom.setup.hidden = Boolean(state.auth?.token);
  dom.dashboard.hidden = !state.auth?.token;
  dom.logout.hidden = !state.auth?.token;

  if (!dashboard || !session) {
    return;
  }

  dom.sessionName.textContent = session.sessionName;
  dom.joinCode.textContent = session.joinCode;
  dom.sessionState.textContent = formatSessionStatus(session.status);
  dom.studentCount.textContent = String(dashboard.students.length);
  dom.connectedCount.textContent = String(dashboard.students.filter((student) => student.connected).length);
  dom.lastTrade.textContent = dashboard.market?.lastTradePriceCents ? formatPrice(dashboard.market.lastTradePriceCents) : '-';
  dom.timeRemaining.textContent = formatDuration(getLiveRemainingSeconds());
  dom.liquidation.textContent = dashboard.liquidation?.revealed
    ? formatPrice(dashboard.liquidation.valueCents)
    : '비공개';
  dom.cardTotal.textContent = dashboard.liquidationComposition
    ? `현재 활성 카드 합계는 ${formatSignedMoney(dashboard.liquidationComposition.currentValueCents)}이며 카드 수는 ${dashboard.liquidationComposition.cardCount}장입니다.`
    : '카드 구성을 불러올 수 없습니다.';

  dom.openButton.disabled = session.status === 'OPEN';
  dom.pauseButton.disabled = session.status === 'PAUSED';
  dom.closeButton.disabled = session.status === 'CLOSED';
  dom.finalizeButton.disabled = session.status === 'CLOSED';

  renderTableRows(
    dom.students,
    dashboard.students.map(
      (student) => `
        <tr>
          <td>${student.displayName}</td>
          <td>${student.connected ? '예' : '아니오'}</td>
          <td>${student.positionQty}</td>
          <td>${formatMoney(student.cashCents)}</td>
          <td>${student.openOrderCount}</td>
        </tr>
      `
    ),
    '아직 참가한 학생이 없습니다.'
  );

  renderListItems(
    dom.publicInfoSchedule,
    dashboard.publicInfoSchedule.map(
      (item) => `
        <li>
          <strong>${formatPublicInfoType(item.infoType)} ${item.sequenceNo}차</strong>
          <div>상태: ${formatScheduleStatus(item.status)}</div>
          <div class="muted">시작 후 ${formatDuration(item.scheduledOffsetSeconds)}</div>
        </li>
      `
    ),
    '자동 공개 정보가 없습니다.'
  );

  renderTableRows(
    dom.cards,
    (dashboard.liquidationComposition?.cards || []).map(
      (card) => `
        <tr>
          <td>${card.label}</td>
          <td>${formatCardColor(card.color)}</td>
          <td>${formatSignedMoney(card.contributionCents)}</td>
        </tr>
      `
    ),
    '활성 카드가 없습니다.'
  );

  renderTableRows(
    dom.leaderboard,
    dashboard.leaderboard.map(
      (row, index) => `
        <tr>
          <td>${row.rank || index + 1}</td>
          <td>${row.displayName}</td>
          <td>${formatMoney(row.portfolioValueCents)}</td>
          <td>${formatPercentFromBps(row.returnBps)}</td>
          <td>${formatMoney(row.cashCents)}</td>
          <td>${row.positionQty}</td>
        </tr>
      `
    ),
    '아직 순위표가 없습니다.'
  );

  renderOrderBookRows(dom.orderBook, dashboard.orderBook.bids, dashboard.orderBook.asks);

  renderTableRows(
    dom.trades,
    dashboard.orderBook.recentTrades.map(
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

  renderListItems(
    dom.announcements,
    dashboard.orderBook.announcements.map(
      (announcement) => `
        <li>
          <strong>${formatTime(announcement.createdAt)}</strong>
          <div>${announcement.message}</div>
        </li>
      `
    ),
    '아직 공지가 없습니다.'
  );
}

async function loadInstructorSession() {
  const [me, dashboard] = await Promise.all([
    apiFetch('/api/me', { token: state.auth.token }),
    apiFetch('/api/instructor/dashboard', { token: state.auth.token })
  ]);

  if (me.principal.role !== 'INSTRUCTOR') {
    throw new Error('이 토큰은 강사용 계정용이 아닙니다.');
  }

  state.me = me;
  state.dashboard = dashboard;
  state.dashboardSnapshotAt = Date.now();
  renderDashboard();
}

const debouncedRefresh = debounce(() => {
  loadInstructorSession().catch((error) => showError(formatErrorMessage(error)));
}, 200);

function handleRealtimeEvent(event) {
  if (['orderbook.snapshot', 'orderbook.updated', 'trade.executed', 'announcement.created', 'leaderboard.updated', 'game.state', 'leaderboard.final'].includes(event.type)) {
    debouncedRefresh();
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

function startPolling() {
  window.clearInterval(refreshTimer);
  refreshTimer = window.setInterval(() => {
    loadInstructorSession().catch((error) => showError(formatErrorMessage(error)));
  }, 5000);
}

function clearInstructorState() {
  realtimeClient?.disconnect();
  window.clearInterval(refreshTimer);
  refreshTimer = null;
  state.auth = null;
  state.me = null;
  state.dashboard = null;
  state.dashboardSnapshotAt = 0;
  clearStorage(STORAGE_KEYS.instructor);
  renderDashboard();
  hidePanel(dom.toast);
  clearError();
  setStatus('disconnected');
}

window.setInterval(() => {
  if (state.auth?.token && state.dashboard?.session?.status === 'OPEN') {
    renderDashboard();
  }
}, 1000);

async function restoreInstructor() {
  if (!state.auth?.token) {
    renderDashboard();
    return;
  }

  try {
    clearError();
    await loadInstructorSession();
    connectRealtime();
    startPolling();
  } catch (error) {
    clearInstructorState();
    showError(formatErrorMessage(error));
  }
}

async function createOrReset(path) {
  const secret = dom.bootstrapSecret.value.trim();
  const referencePriceCents = dollarsToCents(dom.referencePriceInput.value);

  if (!secret) {
    throw new Error('초기 관리자 비밀키를 입력하세요.');
  }

  if (!referencePriceCents) {
    throw new Error('올바른 기준 가격을 입력하세요.');
  }

  writeStorage(STORAGE_KEYS.bootstrapSecret, { secret });

  const created = await apiFetch(path, {
    method: 'POST',
    headers: {
      'x-admin-secret': secret
    },
    body: {
      sessionName: dom.sessionNameInput.value.trim() || '투자 수업',
      referencePriceCents
    }
  });

  state.auth = {
    token: created.instructor.participant.authToken,
    participantId: created.instructor.participant.id,
    sessionId: created.session.id
  };
  writeStorage(STORAGE_KEYS.instructor, state.auth);
  showToast(`세션이 준비되었습니다. 참가 코드: ${created.session.joinCode}`);
  await restoreInstructor();
}

dom.bootstrapForm.addEventListener('submit', async (event) => {
  event.preventDefault();
  try {
    clearError();
    await createOrReset('/api/sessions');
  } catch (error) {
    showError(formatErrorMessage(error));
  }
});

document.querySelector('#reset-session').addEventListener('click', async () => {
  try {
    clearError();
    await createOrReset('/api/sessions/reset');
  } catch (error) {
    showError(formatErrorMessage(error));
  }
});

dom.restoreTokenForm.addEventListener('submit', async (event) => {
  event.preventDefault();
  const token = dom.restoreTokenInput.value.trim();
  if (!token) {
    showError('세션을 복원하려면 강사용 토큰을 붙여 넣으세요.');
    return;
  }

  state.auth = { token };
  writeStorage(STORAGE_KEYS.instructor, state.auth);
  await restoreInstructor();
});

dom.logout.addEventListener('click', () => {
  clearInstructorState();
});

async function patchState(status) {
  const sessionId = state.me?.principal?.sessionId || state.auth?.sessionId || state.dashboard?.session?.id;
  await apiFetch(`/api/sessions/${sessionId}/state`, {
    method: 'PATCH',
    token: state.auth.token,
    body: { status }
  });
  await loadInstructorSession();
}

dom.openButton.addEventListener('click', async () => {
  try {
    clearError();
    await patchState('OPEN');
  } catch (error) {
    showError(formatErrorMessage(error));
  }
});

dom.pauseButton.addEventListener('click', async () => {
  try {
    clearError();
    await patchState('PAUSED');
  } catch (error) {
    showError(formatErrorMessage(error));
  }
});

dom.closeButton.addEventListener('click', async () => {
  try {
    clearError();
    await patchState('CLOSED');
  } catch (error) {
    showError(formatErrorMessage(error));
  }
});

dom.finalizeButton.addEventListener('click', async () => {
  try {
    clearError();
    const sessionId = state.me?.principal?.sessionId || state.dashboard?.session?.id;
    await apiFetch(`/api/sessions/${sessionId}/finalize`, {
      method: 'POST',
      token: state.auth.token
    });
    showToast('최종 청산가가 공개되었습니다.');
    await loadInstructorSession();
  } catch (error) {
    showError(formatErrorMessage(error));
  }
});

restoreInstructor();
