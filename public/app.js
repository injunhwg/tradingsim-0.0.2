export const STORAGE_KEYS = {
  student: 'trading-sim.student',
  instructor: 'trading-sim.instructor',
  bootstrapSecret: 'trading-sim.bootstrap-secret'
};

export const ORDER_BOOK_VISIBLE_LEVELS = 6;

const SESSION_STATUS_LABELS = {
  OPEN: '진행 중',
  PAUSED: '일시정지',
  CLOSED: '종료'
};

const ORDER_STATUS_LABELS = {
  OPEN: '대기',
  PARTIALLY_FILLED: '부분 체결',
  FILLED: '체결 완료',
  CANCELLED: '취소됨',
  REJECTED: '거부됨'
};

const ORDER_SIDE_LABELS = {
  BUY: '매수',
  SELL: '매도'
};

const ORDER_TYPE_LABELS = {
  MARKET: '시장가',
  LIMIT: '지정가'
};

const PUBLIC_INFO_TYPE_LABELS = {
  SAR: 'SAR 보고서',
  EPS: 'EPS 발표'
};

const SCHEDULE_STATUS_LABELS = {
  PENDING: '대기',
  RELEASED: '공개됨',
  CANCELLED: '취소됨'
};

const CARD_COLOR_LABELS = {
  BLACK: '검정',
  RED: '빨강'
};

const API_ERROR_LABELS = {
  ACTIVE_SESSION_ALREADY_EXISTS: '이미 진행 중인 세션이 있습니다.',
  ACCOUNT_NOT_FOUND: '계좌 정보를 찾을 수 없습니다.',
  AUTH_REQUIRED: '인증이 필요합니다.',
  BORROW_LIMIT_BREACH: '차입 한도를 초과했습니다.',
  EPS_RELEASE_UNAVAILABLE: 'EPS 정보를 지금은 공개할 수 없습니다.',
  FORBIDDEN: '이 작업을 수행할 권한이 없습니다.',
  IDEMPOTENCY_KEY_REUSED_WITH_DIFFERENT_PAYLOAD: '같은 요청 키를 다른 주문 내용과 함께 다시 사용할 수 없습니다.',
  INTERNAL_SERVER_ERROR: '서버에서 예기치 못한 오류가 발생했습니다.',
  INVALID_BOOTSTRAP_SECRET: '초기 관리자 비밀키가 올바르지 않습니다.',
  INVALID_DISPLAY_NAME: '이름을 입력하세요.',
  INVALID_IDEMPOTENCY_KEY: '요청 키가 올바르지 않습니다.',
  INVALID_JOIN_CODE: '참가 코드를 입력하세요.',
  INVALID_LIMIT_PRICE: '가격을 올바르게 입력하세요.',
  INVALID_ORDER_ID: '주문 번호가 올바르지 않습니다.',
  INVALID_ORDER_TYPE: '주문 유형이 올바르지 않습니다.',
  INVALID_PARTICIPANT_ID: '참가자 정보가 올바르지 않습니다.',
  INVALID_QUANTITY: '수량을 올바르게 입력하세요.',
  INVALID_ROLE: '역할 정보가 올바르지 않습니다.',
  INVALID_SESSION_ID: '세션 정보가 올바르지 않습니다.',
  INVALID_SESSION_STATUS: '세션 상태 값이 올바르지 않습니다.',
  INVALID_STOCK_ID: '종목 정보가 올바르지 않습니다.',
  INVALID_SIDE: '매수/매도 구분이 올바르지 않습니다.',
  INVALID_TOKEN: '로그인 토큰이 올바르지 않습니다.',
  JOIN_CODE_ALREADY_EXISTS: '참가 코드가 충돌했습니다. 다시 시도하세요.',
  NEGATIVE_RESERVATION: '예약 수량 계산 중 오류가 발생했습니다.',
  NO_ACTIVE_CARDS: '활성 카드가 부족하여 힌트를 제공할 수 없습니다.',
  ORDER_NOT_FOUND: '주문을 찾을 수 없습니다.',
  ORDER_OWNERSHIP_ERROR: '본인의 주문만 취소할 수 있습니다.',
  PARTICIPANT_NOT_FOUND: '참가자를 찾을 수 없습니다.',
  SESSION_CLOSED: '이 세션은 이미 종료되었습니다.',
  SESSION_NOT_FOUND: '세션을 찾을 수 없습니다.',
  SESSION_NOT_OPEN: '세션이 진행 중일 때만 주문할 수 있습니다.',
  SHORT_LIMIT_BREACH: '공매도 한도를 초과했습니다.',
  UNIQUE_CONSTRAINT_VIOLATION: '중복된 데이터가 있어 요청을 처리할 수 없습니다.'
};

export function readStorage(key) {
  try {
    const raw = window.localStorage.getItem(key);
    return raw ? JSON.parse(raw) : null;
  } catch (_error) {
    return null;
  }
}

export function writeStorage(key, value) {
  window.localStorage.setItem(key, JSON.stringify(value));
}

export function clearStorage(key) {
  window.localStorage.removeItem(key);
}

export function formatMoney(cents) {
  return new Intl.NumberFormat('ko-KR', {
    style: 'currency',
    currency: 'USD',
    currencyDisplay: 'narrowSymbol'
  }).format((Number(cents) || 0) / 100);
}

export function formatPrice(cents) {
  if (cents === null || cents === undefined) {
    return '-';
  }

  return formatMoney(cents);
}

export function formatSessionStatus(status) {
  return SESSION_STATUS_LABELS[status] || status || '-';
}

export function formatOrderStatus(status) {
  return ORDER_STATUS_LABELS[status] || status || '-';
}

export function formatOrderSide(side) {
  return ORDER_SIDE_LABELS[side] || side || '-';
}

export function formatOrderType(orderType) {
  return ORDER_TYPE_LABELS[orderType] || orderType || '-';
}

export function formatScheduleStatus(status) {
  return SCHEDULE_STATUS_LABELS[status] || status || '-';
}

export function formatPublicInfoType(infoType) {
  return PUBLIC_INFO_TYPE_LABELS[infoType] || infoType || '-';
}

export function formatCardColor(color) {
  return CARD_COLOR_LABELS[color] || color || '-';
}

export function formatStockLabel(stock) {
  if (!stock) {
    return '-';
  }

  return stock.displayName || stock.stockDisplayName || stock.stockKey || '-';
}

export function formatErrorMessage(error) {
  if (error?.code && API_ERROR_LABELS[error.code]) {
    return API_ERROR_LABELS[error.code];
  }

  if (typeof error?.message === 'string' && /[가-힣]/.test(error.message)) {
    return error.message;
  }

  return '요청을 처리하는 중 문제가 발생했습니다.';
}

export function formatSignedMoney(cents) {
  const value = Number(cents) || 0;
  const prefix = value > 0 ? '+' : '';
  return `${prefix}${formatMoney(value)}`;
}

export function formatPercentFromBps(bps) {
  return `${((Number(bps) || 0) / 100).toFixed(2)}%`;
}

export function formatTime(value) {
  if (!value) {
    return '-';
  }

  return new Intl.DateTimeFormat('ko-KR', {
    hour: 'numeric',
    minute: '2-digit',
    second: '2-digit'
  }).format(new Date(value));
}

export function dollarsToCents(value) {
  const parsed = Number.parseFloat(value);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return null;
  }

  return Math.round(parsed * 100);
}

export function formatDuration(totalSeconds) {
  const safeSeconds = Math.max(0, Number(totalSeconds) || 0);
  const minutes = Math.floor(safeSeconds / 60);
  const seconds = safeSeconds % 60;
  return `${String(minutes).padStart(2, '0')}:${String(seconds).padStart(2, '0')}`;
}

export function makeIdempotencyKey() {
  if (window.crypto?.randomUUID) {
    return window.crypto.randomUUID();
  }

  return `${Date.now()}-${Math.random().toString(16).slice(2)}`;
}

export async function apiFetch(path, { method = 'GET', token, headers = {}, body } = {}) {
  const requestHeaders = { ...headers };
  if (token) {
    requestHeaders.authorization = `Bearer ${token}`;
  }

  const options = {
    method,
    headers: requestHeaders
  };

  if (body !== undefined) {
    requestHeaders['content-type'] = requestHeaders['content-type'] || 'application/json';
    options.body = typeof body === 'string' ? body : JSON.stringify(body);
  }

  const response = await fetch(path, options);
  const text = await response.text();
  const data = text ? JSON.parse(text) : null;

  if (!response.ok) {
    const error = new Error(data?.error?.message || `요청이 실패했습니다. 상태 코드: ${response.status}`);
    error.status = response.status;
    error.code = data?.error?.code;
    error.details = data?.error?.details;
    throw error;
  }

  return data;
}

export function setConnectionPill(element, status, text) {
  element.textContent = text;
  element.className = 'pill';

  if (status === 'connected') {
    element.classList.add('connected');
    return;
  }

  if (status === 'error') {
    element.classList.add('error');
    return;
  }

  if (status === 'reconnecting') {
    element.classList.add('reconnecting');
    return;
  }

  element.classList.add('neutral');
}

export function renderTableRows(tbody, rows, fallback) {
  tbody.innerHTML = rows.length > 0 ? rows.join('') : `<tr><td colspan="99" class="muted">${fallback}</td></tr>`;
}

export function renderListItems(list, items, fallback) {
  list.innerHTML = items.length > 0 ? items.join('') : `<li class="muted">${fallback}</li>`;
}

export function renderOrderBookRows(tbody, bids = [], asks = []) {
  if (bids.length === 0 && asks.length === 0) {
    renderTableRows(tbody, [], '활성 매수호가와 매도호가가 없습니다.');
    return;
  }

  const rows = Array.from({ length: ORDER_BOOK_VISIBLE_LEVELS }, (_value, index) => {
    const bid = bids[index];
    const ask = asks[index];
    const emptyCell = '&nbsp;';

    return `
      <tr class="${index === 0 ? 'inside-market-row' : ''}">
        <td>${bid ? bid.orderCount : emptyCell}</td>
        <td>${bid ? bid.totalQty : emptyCell}</td>
        <td class="order-book-bid-price">${bid ? formatPrice(bid.priceCents) : emptyCell}</td>
        <td class="order-book-ask-price">${ask ? formatPrice(ask.priceCents) : emptyCell}</td>
        <td>${ask ? ask.totalQty : emptyCell}</td>
        <td>${ask ? ask.orderCount : emptyCell}</td>
      </tr>
    `;
  });

  tbody.innerHTML = rows.join('');
}

export function showPanel(panel, message) {
  panel.hidden = false;
  panel.textContent = message;
}

export function hidePanel(panel) {
  panel.hidden = true;
  panel.textContent = '';
}

export function debounce(fn, waitMs) {
  let timer = null;

  return (...args) => {
    window.clearTimeout(timer);
    timer = window.setTimeout(() => {
      fn(...args);
    }, waitMs);
  };
}

export class RealtimeClient {
  constructor({ token, onEvent, onStatusChange }) {
    this.token = token;
    this.onEvent = onEvent;
    this.onStatusChange = onStatusChange;
    this.socket = null;
    this.reconnectAttempts = 0;
    this.shouldReconnect = true;
  }

  connect() {
    this.shouldReconnect = true;
    this.#open();
  }

  disconnect() {
    this.shouldReconnect = false;
    if (this.socket) {
      this.socket.close();
      this.socket = null;
    }
  }

  #open() {
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const url = `${protocol}//${window.location.host}/ws?token=${encodeURIComponent(this.token)}`;
    this.onStatusChange?.(this.reconnectAttempts === 0 ? 'connecting' : 'reconnecting');
    this.socket = new WebSocket(url);

    this.socket.addEventListener('open', () => {
      this.reconnectAttempts = 0;
      this.onStatusChange?.('connected');
    });

    this.socket.addEventListener('message', (event) => {
      const payload = JSON.parse(event.data);
      this.onEvent?.(payload);
    });

    this.socket.addEventListener('close', () => {
      this.onStatusChange?.('disconnected');
      if (!this.shouldReconnect) {
        return;
      }

      this.reconnectAttempts += 1;
      const delay = Math.min(5000, 500 * this.reconnectAttempts);
      window.setTimeout(() => this.#open(), delay);
    });

    this.socket.addEventListener('error', () => {
      this.onStatusChange?.('error');
    });
  }
}
