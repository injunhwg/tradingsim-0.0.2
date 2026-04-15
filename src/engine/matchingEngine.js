const crypto = require('node:crypto');

const { initializeSchema } = require('../db/schema');
const { buildTradableStocks } = require('../services/classroomGame');

const STARTING_CASH_CENTS = 20000;
const MAX_BORROW_CENTS = 20000;
const STARTING_POSITION_QTY = 5;
const MAX_SHORT_QTY = 5;

class EngineError extends Error {
  constructor(code, message, details = {}) {
    super(message);
    this.name = 'EngineError';
    this.code = code;
    this.details = details;
  }
}

class SessionMutex {
  constructor() {
    this.tails = new Map();
  }

  async withLock(key, handler) {
    const previousTail = this.tails.get(key) || Promise.resolve();
    let releaseTail;
    const nextTail = new Promise((resolve) => {
      releaseTail = resolve;
    });

    this.tails.set(key, nextTail);
    await previousTail;

    try {
      return await handler();
    } finally {
      releaseTail();

      if (this.tails.get(key) === nextTail) {
        this.tails.delete(key);
      }
    }
  }
}

const sharedSessionMutex = new SessionMutex();

function generateOpaqueToken() {
  return crypto.randomBytes(24).toString('hex');
}

function generateJoinCode() {
  return crypto.randomBytes(3).toString('hex').toUpperCase();
}

function normalizeSide(value) {
  return String(value || '').toUpperCase();
}

function normalizeOrderType(value) {
  return String(value || '').toUpperCase();
}

function stableStringify(value) {
  if (value === null || typeof value !== 'object') {
    return JSON.stringify(value);
  }

  if (Array.isArray(value)) {
    return `[${value.map((item) => stableStringify(item)).join(',')}]`;
  }

  const keys = Object.keys(value).sort();
  return `{${keys.map((key) => `${JSON.stringify(key)}:${stableStringify(value[key])}`).join(',')}}`;
}

function hashRequest(value) {
  return crypto.createHash('sha256').update(stableStringify(value)).digest('hex');
}

function parseJsonColumn(value) {
  if (value === null || value === undefined) {
    return null;
  }

  if (typeof value === 'string') {
    return JSON.parse(value);
  }

  return value;
}

function assertAccountInvariants(account) {
  if (account.cash_cents < -MAX_BORROW_CENTS) {
    throw new EngineError('BORROW_LIMIT_BREACH', 'Account cash fell below the borrowing limit.', {
      participantId: account.participant_id,
      cashCents: account.cash_cents
    });
  }

  if (account.reserved_buy_cents < 0) {
    throw new EngineError('NEGATIVE_RESERVATION', 'Reservation tracking became negative.', {
      participantId: account.participant_id,
      reservedBuyCents: account.reserved_buy_cents
    });
  }
}

function assertHoldingInvariants(holding) {
  if (holding.position_qty < -MAX_SHORT_QTY) {
    throw new EngineError('SHORT_LIMIT_BREACH', 'Account position fell below the short limit.', {
      participantId: holding.participant_id,
      sessionStockId: holding.session_stock_id,
      positionQty: holding.position_qty
    });
  }

  if (holding.reserved_sell_qty < 0) {
    throw new EngineError('NEGATIVE_RESERVATION', 'Reservation tracking became negative.', {
      participantId: holding.participant_id,
      sessionStockId: holding.session_stock_id,
      reservedSellQty: holding.reserved_sell_qty
    });
  }
}

function validateOrderInput(input) {
  const side = normalizeSide(input.side);
  const orderType = normalizeOrderType(input.orderType);
  const quantity = Number(input.quantity);
  const hasLimitPrice = input.limitPriceCents !== null && input.limitPriceCents !== undefined;
  const limitPriceCents = hasLimitPrice ? Number(input.limitPriceCents) : null;

  if (!Number.isInteger(input.sessionId) || input.sessionId <= 0) {
    throw new EngineError('INVALID_SESSION_ID', 'sessionId must be a positive integer.');
  }

  if (!Number.isInteger(input.participantId) || input.participantId <= 0) {
    throw new EngineError('INVALID_PARTICIPANT_ID', 'participantId must be a positive integer.');
  }

  if (!Number.isInteger(input.sessionStockId) || input.sessionStockId <= 0) {
    throw new EngineError('INVALID_STOCK_ID', 'sessionStockId must be a positive integer.');
  }

  if (typeof input.idempotencyKey !== 'string' || input.idempotencyKey.trim() === '') {
    throw new EngineError('INVALID_IDEMPOTENCY_KEY', 'idempotencyKey is required.');
  }

  if (!['BUY', 'SELL'].includes(side)) {
    throw new EngineError('INVALID_SIDE', 'side must be BUY or SELL.');
  }

  if (!['MARKET', 'LIMIT'].includes(orderType)) {
    throw new EngineError('INVALID_ORDER_TYPE', 'orderType must be MARKET or LIMIT.');
  }

  if (!Number.isInteger(quantity) || quantity <= 0) {
    throw new EngineError('INVALID_QUANTITY', 'quantity must be a positive integer.');
  }

  if (orderType === 'MARKET' && hasLimitPrice) {
    throw new EngineError('INVALID_LIMIT_PRICE', 'Market orders cannot include a limit price.');
  }

  if (orderType === 'LIMIT') {
    if (!Number.isInteger(limitPriceCents) || limitPriceCents <= 0) {
      throw new EngineError('INVALID_LIMIT_PRICE', 'Limit orders require a positive integer price in cents.');
    }
  }

  return {
    sessionId: input.sessionId,
    sessionStockId: input.sessionStockId,
    participantId: input.participantId,
    idempotencyKey: input.idempotencyKey.trim(),
    side,
    orderType,
    quantity,
    limitPriceCents
  };
}

function buildOrderResponse(order, fills, idempotencyHit = false) {
  return {
    idempotencyHit,
    order: {
      id: order.id,
      sessionId: order.session_id,
      sessionStockId: order.session_stock_id,
      participantId: order.participant_id,
      side: order.side,
      orderType: order.order_type,
      limitPriceCents: order.limit_price_cents,
      originalQty: order.original_qty,
      remainingQty: order.remaining_qty,
      filledQty: order.original_qty - order.remaining_qty,
      status: order.status,
      rejectionReason: order.rejection_reason,
      cancelReason: order.cancel_reason,
      createdAt: order.created_at,
      updatedAt: order.updated_at
    },
    fills: fills.map((fill) => ({
      id: fill.id,
      sessionStockId: fill.session_stock_id,
      buyOrderId: fill.buy_order_id,
      sellOrderId: fill.sell_order_id,
      aggressorOrderId: fill.aggressor_order_id,
      restingOrderId: fill.resting_order_id,
      priceCents: fill.price_cents,
      qty: fill.qty,
      executedAt: fill.executed_at
    }))
  };
}

function cloneRow(row) {
  return { ...row };
}

class MatchingEngine {
  constructor({ db, lockManager = sharedSessionMutex } = {}) {
    if (!db) {
      throw new Error('A database instance is required.');
    }

    this.db = db;
    this.lockManager = lockManager;
  }

  async initialize() {
    await initializeSchema(this.db);
  }

  async createSession({ status = 'OPEN', sessionName, joinCode, referencePriceCents } = {}) {
    const resolvedSessionName =
      typeof sessionName === 'string' && sessionName.trim() ? sessionName.trim() : 'Classroom Session';
    const resolvedJoinCode =
      typeof joinCode === 'string' && joinCode.trim() ? joinCode.trim().toUpperCase() : generateJoinCode();
    const resolvedReferencePriceCents =
      Number.isInteger(referencePriceCents) && referencePriceCents > 0 ? referencePriceCents : 1000;

    return this.db.withTransaction(async (tx) => {
      const result = await tx.query(
        `INSERT INTO market_sessions (session_name, join_code, status, reference_price_cents)
         VALUES ($1, $2, $3, $4)
         RETURNING *`,
        [resolvedSessionName, resolvedJoinCode, status, resolvedReferencePriceCents]
      );

      const session = result.rows[0];
      for (const stock of buildTradableStocks(resolvedReferencePriceCents)) {
        await tx.query(
          `INSERT INTO session_stocks (
             session_id,
             stock_key,
             display_name,
             sort_order,
             reference_price_cents,
             initial_position_qty
           )
           VALUES ($1, $2, $3, $4, $5, $6)`,
          [
            session.id,
            stock.stockKey,
            stock.displayName,
            stock.sortOrder,
            stock.referencePriceCents,
            stock.initialPositionQty
          ]
        );
      }

      return session;
    });
  }

  async createParticipant({ sessionId, externalId, displayName, role = 'STUDENT', authToken } = {}) {
    const normalizedRole = String(role || 'STUDENT').toUpperCase();
    if (!['STUDENT', 'INSTRUCTOR'].includes(normalizedRole)) {
      throw new EngineError('INVALID_ROLE', 'role must be STUDENT or INSTRUCTOR.');
    }

    const resolvedExternalId =
      typeof externalId === 'string' && externalId.trim() ? externalId.trim() : generateOpaqueToken();
    const resolvedDisplayName =
      typeof displayName === 'string' && displayName.trim() ? displayName.trim() : 'Participant';
    const resolvedAuthToken =
      typeof authToken === 'string' && authToken.trim() ? authToken.trim() : generateOpaqueToken();

    const inserted = await this.db.withTransaction(async (tx) => {
      const participantResult = await tx.query(
        `INSERT INTO participants (session_id, external_id, display_name, role, auth_token)
         VALUES ($1, $2, $3, $4, $5)
         RETURNING *`,
        [sessionId, resolvedExternalId, resolvedDisplayName, normalizedRole, resolvedAuthToken]
      );

      const participant = participantResult.rows[0];

      const accountResult = await tx.query(
        `INSERT INTO accounts (participant_id, cash_cents, position_qty, reserved_sell_qty)
         VALUES ($1, $2, 0, 0)
         RETURNING *`,
        [participant.id, STARTING_CASH_CENTS]
      );

      const sessionStocksResult = await tx.query(
        `SELECT *
         FROM session_stocks
         WHERE session_id = $1
         ORDER BY sort_order ASC, id ASC`,
        [sessionId]
      );

      const holdings = [];
      for (const sessionStock of sessionStocksResult.rows) {
        const holdingResult = await tx.query(
          `INSERT INTO account_holdings (
             participant_id,
             session_stock_id,
             position_qty,
             reserved_sell_qty
           )
           VALUES ($1, $2, $3, 0)
           RETURNING *`,
          [participant.id, sessionStock.id, sessionStock.initial_position_qty]
        );
        holdings.push(holdingResult.rows[0]);
      }

      return {
        participant,
        account: accountResult.rows[0],
        holdings
      };
    });

    return inserted;
  }

  async listSessionStocks(sessionId) {
    const result = await this.db.query(
      `SELECT *
       FROM session_stocks
       WHERE session_id = $1
       ORDER BY sort_order ASC, id ASC`,
      [sessionId]
    );

    return result.rows;
  }

  async submitOrder(input) {
    const request = validateOrderInput(input);
    const requestHash = hashRequest({
      sessionStockId: request.sessionStockId,
      side: request.side,
      orderType: request.orderType,
      quantity: request.quantity,
      limitPriceCents: request.limitPriceCents
    });

    return this.lockManager.withLock(`session:${request.sessionId}`, async () => {
      return this.db.withTransaction(async (tx) => {
        const session = await this.#loadSessionForUpdate(tx, request.sessionId);
        if (session.status !== 'OPEN') {
          throw new EngineError('SESSION_NOT_OPEN', 'Orders can only be submitted while the session is OPEN.', {
            sessionId: request.sessionId,
            status: session.status
          });
        }

        await this.#loadSessionStockForUpdate(tx, request.sessionStockId, request.sessionId);

        const idempotency = await this.#reserveIdempotencyKey(tx, request, requestHash);
        if (idempotency.response) {
          return {
            ...idempotency.response,
            idempotencyHit: true
          };
        }

        const account = await this.#loadAccountForUpdate(tx, request.participantId, request.sessionId);
        const holding = await this.#loadHoldingForUpdate(tx, request.participantId, request.sessionStockId);
        const order = await this.#insertInitialOrder(tx, request);
        const fills = [];

        const buyExposureCapacity = account.cash_cents + MAX_BORROW_CENTS - account.reserved_buy_cents;
        const sellCapacity = holding.position_qty + MAX_SHORT_QTY - holding.reserved_sell_qty;

        if (request.orderType === 'LIMIT' && request.side === 'BUY') {
          const requiredExposure = request.quantity * request.limitPriceCents;
          if (requiredExposure > buyExposureCapacity) {
            order.status = 'REJECTED';
            order.rejection_reason = 'INSUFFICIENT_BUYING_POWER';
            await this.#persistOrder(tx, order);
            const response = buildOrderResponse(order, fills);
            await this.#storeIdempotencyResponse(tx, request, response);
            return response;
          }
        }

        if (request.orderType === 'LIMIT' && request.side === 'SELL') {
          if (request.quantity > sellCapacity) {
            order.status = 'REJECTED';
            order.rejection_reason = 'INSUFFICIENT_SHORT_CAPACITY';
            await this.#persistOrder(tx, order);
            const response = buildOrderResponse(order, fills);
            await this.#storeIdempotencyResponse(tx, request, response);
            return response;
          }
        }

        const cachedAccounts = new Map([[account.participant_id, account]]);
        const cachedHoldings = new Map([[holding.participant_id, holding]]);
        const contraOrders = await this.#loadRestingContraOrders(tx, request);
        let marketBuyBudget = request.side === 'BUY' && request.orderType === 'MARKET' ? buyExposureCapacity : null;
        let marketSellCapacity = request.side === 'SELL' && request.orderType === 'MARKET' ? sellCapacity : null;
        let stoppedByRisk = false;

        for (const restingOrder of contraOrders) {
          if (order.remaining_qty === 0) {
            break;
          }

          const restingPrice = restingOrder.limit_price_cents;
          const restingRemaining = restingOrder.remaining_qty;

          let fillQty = Math.min(order.remaining_qty, restingRemaining);

          if (request.side === 'BUY' && request.orderType === 'MARKET') {
            const maxAffordableQty = Math.floor(marketBuyBudget / restingPrice);
            if (maxAffordableQty <= 0) {
              stoppedByRisk = true;
              break;
            }

            fillQty = Math.min(fillQty, maxAffordableQty);
          }

          if (request.side === 'SELL' && request.orderType === 'MARKET') {
            if (marketSellCapacity <= 0) {
              stoppedByRisk = true;
              break;
            }

            fillQty = Math.min(fillQty, marketSellCapacity);
          }

          if (fillQty <= 0) {
            break;
          }

          const contraAccount = await this.#loadOrReuseAccount(tx, cachedAccounts, restingOrder.participant_id, request.sessionId);
          const contraHolding = await this.#loadOrReuseHolding(
            tx,
            cachedHoldings,
            restingOrder.participant_id,
            request.sessionStockId
          );
          this.#applyFill({
            aggressorOrder: order,
            aggressorAccount: account,
            aggressorHolding: holding,
            restingOrder,
            restingAccount: contraAccount,
            restingHolding: contraHolding,
            fillQty,
            fills,
            side: request.side
          });

          if (marketBuyBudget !== null) {
            marketBuyBudget -= restingPrice * fillQty;
          }

          if (marketSellCapacity !== null) {
            marketSellCapacity -= fillQty;
          }
        }

        if (request.orderType === 'LIMIT' && order.remaining_qty > 0) {
          if (request.side === 'BUY') {
            account.reserved_buy_cents += order.remaining_qty * request.limitPriceCents;
          } else {
            holding.reserved_sell_qty += order.remaining_qty;
          }
        }

        assertAccountInvariants(account);
        assertHoldingInvariants(holding);

        for (const cachedAccount of cachedAccounts.values()) {
          assertAccountInvariants(cachedAccount);
          await this.#persistAccount(tx, cachedAccount);
        }

        for (const cachedHolding of cachedHoldings.values()) {
          assertHoldingInvariants(cachedHolding);
          await this.#persistHolding(tx, cachedHolding);
        }

        for (const restingOrder of contraOrders) {
          if (restingOrder.__dirty) {
            await this.#persistOrder(tx, restingOrder);
          }
        }

        if (order.status !== 'REJECTED') {
          if (order.remaining_qty === 0) {
            order.status = 'FILLED';
            order.cancel_reason = null;
          } else if (request.orderType === 'MARKET') {
            order.status = 'CANCELLED';
            order.cancel_reason = stoppedByRisk ? 'RISK_LIMIT_REMAINDER_CANCELLED' : 'UNFILLED_MARKET_REMAINDER';
          } else if (order.remaining_qty < order.original_qty) {
            order.status = 'PARTIALLY_FILLED';
          } else {
            order.status = 'OPEN';
          }
        }

        await this.#persistOrder(tx, order);

        const insertedFills = [];
        for (const pendingFill of fills) {
          const fillResult = await tx.query(
            `INSERT INTO fills (
              session_id,
              session_stock_id,
              buy_order_id,
              sell_order_id,
              aggressor_order_id,
              resting_order_id,
              price_cents,
              qty
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            RETURNING *`,
            [
              request.sessionId,
              request.sessionStockId,
              pendingFill.buy_order_id,
              pendingFill.sell_order_id,
              pendingFill.aggressor_order_id,
              pendingFill.resting_order_id,
              pendingFill.price_cents,
              pendingFill.qty
            ]
          );

          insertedFills.push(fillResult.rows[0]);
        }

        const response = buildOrderResponse(order, insertedFills);
        await this.#storeIdempotencyResponse(tx, request, response);
        return response;
      });
    });
  }

  async cancelOrder({ sessionId, participantId, orderId }) {
    if (!Number.isInteger(sessionId) || sessionId <= 0) {
      throw new EngineError('INVALID_SESSION_ID', 'sessionId must be a positive integer.');
    }

    if (!Number.isInteger(participantId) || participantId <= 0) {
      throw new EngineError('INVALID_PARTICIPANT_ID', 'participantId must be a positive integer.');
    }

    if (!Number.isInteger(orderId) || orderId <= 0) {
      throw new EngineError('INVALID_ORDER_ID', 'orderId must be a positive integer.');
    }

    return this.lockManager.withLock(`session:${sessionId}`, async () => {
      return this.db.withTransaction(async (tx) => {
        await this.#loadSessionForUpdate(tx, sessionId);
        const order = await this.#loadOrderForUpdate(tx, orderId, sessionId);

        if (order.participant_id !== participantId) {
          throw new EngineError('ORDER_OWNERSHIP_ERROR', 'Participants can only cancel their own orders.', {
            participantId,
            orderId
          });
        }

        if (!['OPEN', 'PARTIALLY_FILLED'].includes(order.status)) {
          return buildOrderResponse(order, []);
        }

        const account = await this.#loadAccountForUpdate(tx, participantId, sessionId);
        const holding = await this.#loadHoldingForUpdate(tx, participantId, order.session_stock_id);

        if (order.order_type === 'LIMIT' && order.remaining_qty > 0) {
          if (order.side === 'BUY') {
            account.reserved_buy_cents -= order.remaining_qty * order.limit_price_cents;
          } else {
            holding.reserved_sell_qty -= order.remaining_qty;
          }
        }

        assertAccountInvariants(account);
        assertHoldingInvariants(holding);
        await this.#persistAccount(tx, account);
        await this.#persistHolding(tx, holding);

        order.status = 'CANCELLED';
        order.cancel_reason = 'USER_CANCELLED';
        await this.#persistOrder(tx, order);

        return buildOrderResponse(order, []);
      });
    });
  }

  async getAccount(participantId) {
    const result = await this.db.query(
      `SELECT *
       FROM accounts
       WHERE participant_id = $1`,
      [participantId]
    );

    return result.rows[0] || null;
  }

  async getHoldings(participantId) {
    const result = await this.db.query(
      `SELECT *
       FROM account_holdings
       WHERE participant_id = $1
       ORDER BY session_stock_id ASC`,
      [participantId]
    );

    return result.rows;
  }

  async getOrder(orderId) {
    const result = await this.db.query(
      `SELECT *
       FROM orders
       WHERE id = $1`,
      [orderId]
    );

    return result.rows[0] || null;
  }

  async listOrders(sessionId) {
    const result = await this.db.query(
      `SELECT *
       FROM orders
       WHERE session_id = $1
       ORDER BY id ASC`,
      [sessionId]
    );

    return result.rows;
  }

  async listOpenOrders(sessionId) {
    const result = await this.db.query(
      `SELECT *
       FROM orders
       WHERE session_id = $1
         AND status IN ('OPEN', 'PARTIALLY_FILLED')
       ORDER BY id ASC`,
      [sessionId]
    );

    return result.rows;
  }

  async listFills(sessionId) {
    const result = await this.db.query(
      `SELECT *
       FROM fills
       WHERE session_id = $1
       ORDER BY id ASC`,
      [sessionId]
    );

    return result.rows;
  }

  async close() {
    await this.db.close();
  }

  async #reserveIdempotencyKey(tx, request, requestHash) {
    const inserted = await tx.query(
      `INSERT INTO idempotency_keys (
         session_id,
         participant_id,
         idempotency_key,
         request_hash
       )
       VALUES ($1, $2, $3, $4)
       ON CONFLICT (participant_id, idempotency_key) DO NOTHING
       RETURNING *`,
      [request.sessionId, request.participantId, request.idempotencyKey, requestHash]
    );

    if (inserted.rowCount === 1) {
      return { response: null };
    }

    const existingResult = await tx.query(
      `SELECT *
       FROM idempotency_keys
       WHERE participant_id = $1
         AND idempotency_key = $2
       FOR UPDATE`,
      [request.participantId, request.idempotencyKey]
    );

    const existing = existingResult.rows[0];
    if (!existing) {
      throw new EngineError('IDEMPOTENCY_LOOKUP_FAILED', 'Failed to load an existing idempotency record.');
    }

    if (existing.request_hash !== requestHash) {
      throw new EngineError(
        'IDEMPOTENCY_KEY_REUSED_WITH_DIFFERENT_PAYLOAD',
        'The same idempotency key cannot be reused with a different payload.',
        {
          participantId: request.participantId,
          idempotencyKey: request.idempotencyKey
        }
      );
    }

    return {
      response: parseJsonColumn(existing.response_json)
    };
  }

  async #storeIdempotencyResponse(tx, request, response) {
    await tx.query(
      `UPDATE idempotency_keys
       SET response_json = $4::jsonb,
           updated_at = NOW()
       WHERE participant_id = $1
         AND idempotency_key = $2
         AND session_id = $3`,
      [request.participantId, request.idempotencyKey, request.sessionId, JSON.stringify(response)]
    );
  }

  async #loadSessionForUpdate(tx, sessionId) {
    const result = await tx.query(
      `SELECT *
       FROM market_sessions
       WHERE id = $1
       FOR UPDATE`,
      [sessionId]
    );

    const session = result.rows[0];
    if (!session) {
      throw new EngineError('SESSION_NOT_FOUND', 'The market session does not exist.', { sessionId });
    }

    return session;
  }

  async #loadAccountForUpdate(tx, participantId, sessionId) {
    const result = await tx.query(
      `SELECT a.*
       FROM accounts a
       INNER JOIN participants p ON p.id = a.participant_id
       WHERE a.participant_id = $1
         AND p.session_id = $2
       FOR UPDATE`,
      [participantId, sessionId]
    );

    const account = result.rows[0];
    if (!account) {
      throw new EngineError('ACCOUNT_NOT_FOUND', 'The participant account does not exist.', {
        participantId,
        sessionId
      });
    }

    return cloneRow(account);
  }

  async #loadOrReuseAccount(tx, cache, participantId, sessionId) {
    if (cache.has(participantId)) {
      return cache.get(participantId);
    }

    const account = await this.#loadAccountForUpdate(tx, participantId, sessionId);
    cache.set(participantId, account);
    return account;
  }

  async #loadHoldingForUpdate(tx, participantId, sessionStockId) {
    const result = await tx.query(
      `SELECT *
       FROM account_holdings
       WHERE participant_id = $1
         AND session_stock_id = $2
       FOR UPDATE`,
      [participantId, sessionStockId]
    );

    const holding = result.rows[0];
    if (!holding) {
      throw new EngineError('ACCOUNT_NOT_FOUND', 'The participant holding does not exist.', {
        participantId,
        sessionStockId
      });
    }

    return cloneRow(holding);
  }

  async #loadOrReuseHolding(tx, cache, participantId, sessionStockId) {
    if (cache.has(participantId)) {
      return cache.get(participantId);
    }

    const holding = await this.#loadHoldingForUpdate(tx, participantId, sessionStockId);
    cache.set(participantId, holding);
    return holding;
  }

  async #insertInitialOrder(tx, request) {
    const result = await tx.query(
      `INSERT INTO orders (
         session_id,
         session_stock_id,
         participant_id,
         side,
         order_type,
         limit_price_cents,
         original_qty,
         remaining_qty,
         status
       )
       VALUES ($1, $2, $3, $4, $5, $6, $7, $7, 'OPEN')
       RETURNING *`,
      [
        request.sessionId,
        request.sessionStockId,
        request.participantId,
        request.side,
        request.orderType,
        request.limitPriceCents,
        request.quantity
      ]
    );

    return cloneRow(result.rows[0]);
  }

  async #loadRestingContraOrders(tx, request) {
    const params = [request.sessionId];
    let sql = `
      SELECT *
      FROM orders
      WHERE session_id = $1
        AND session_stock_id = $2
        AND side = $3
        AND order_type = 'LIMIT'
        AND status IN ('OPEN', 'PARTIALLY_FILLED')
    `;

    params.push(request.sessionStockId);
    params.push(request.side === 'BUY' ? 'SELL' : 'BUY');

    if (request.side === 'BUY' && request.orderType === 'LIMIT') {
      params.push(request.limitPriceCents);
      sql += ` AND limit_price_cents <= $4`;
    }

    if (request.side === 'SELL' && request.orderType === 'LIMIT') {
      params.push(request.limitPriceCents);
      sql += ` AND limit_price_cents >= $4`;
    }

    if (request.side === 'BUY') {
      sql += ` ORDER BY limit_price_cents ASC, id ASC`;
    } else {
      sql += ` ORDER BY limit_price_cents DESC, id ASC`;
    }

    sql += ` FOR UPDATE`;

    const result = await tx.query(sql, params);
    return result.rows.map((row) => cloneRow(row));
  }

  #applyFill({
    aggressorOrder,
    aggressorAccount,
    aggressorHolding,
    restingOrder,
    restingAccount,
    restingHolding,
    fillQty,
    fills,
    side
  }) {
    const tradePrice = restingOrder.limit_price_cents;

    if (side === 'BUY') {
      aggressorAccount.cash_cents -= tradePrice * fillQty;
      aggressorHolding.position_qty += fillQty;

      restingAccount.cash_cents += tradePrice * fillQty;
      restingHolding.position_qty -= fillQty;
      restingHolding.reserved_sell_qty -= fillQty;

      fills.push({
        session_stock_id: aggressorOrder.session_stock_id,
        buy_order_id: aggressorOrder.id,
        sell_order_id: restingOrder.id,
        aggressor_order_id: aggressorOrder.id,
        resting_order_id: restingOrder.id,
        price_cents: tradePrice,
        qty: fillQty
      });
    } else {
      aggressorAccount.cash_cents += tradePrice * fillQty;
      aggressorHolding.position_qty -= fillQty;

      restingAccount.cash_cents -= tradePrice * fillQty;
      restingHolding.position_qty += fillQty;
      restingAccount.reserved_buy_cents -= tradePrice * fillQty;

      fills.push({
        session_stock_id: aggressorOrder.session_stock_id,
        buy_order_id: restingOrder.id,
        sell_order_id: aggressorOrder.id,
        aggressor_order_id: aggressorOrder.id,
        resting_order_id: restingOrder.id,
        price_cents: tradePrice,
        qty: fillQty
      });
    }

    aggressorOrder.remaining_qty -= fillQty;
    restingOrder.remaining_qty -= fillQty;
    restingOrder.__dirty = true;

    if (restingOrder.remaining_qty === 0) {
      restingOrder.status = 'FILLED';
    } else {
      restingOrder.status = 'PARTIALLY_FILLED';
    }

    assertAccountInvariants(aggressorAccount);
    assertAccountInvariants(restingAccount);
    assertHoldingInvariants(aggressorHolding);
    assertHoldingInvariants(restingHolding);
  }

  async #persistAccount(tx, account) {
    await tx.query(
      `UPDATE accounts
       SET cash_cents = $2,
           reserved_buy_cents = $3,
           updated_at = NOW()
       WHERE participant_id = $1`,
      [
        account.participant_id,
        account.cash_cents,
        account.reserved_buy_cents
      ]
    );
  }

  async #persistHolding(tx, holding) {
    await tx.query(
      `UPDATE account_holdings
       SET position_qty = $3,
           reserved_sell_qty = $4,
           updated_at = NOW()
       WHERE participant_id = $1
         AND session_stock_id = $2`,
      [holding.participant_id, holding.session_stock_id, holding.position_qty, holding.reserved_sell_qty]
    );
  }

  async #persistOrder(tx, order) {
    const result = await tx.query(
      `UPDATE orders
       SET remaining_qty = $2,
           status = $3,
           rejection_reason = $4,
           cancel_reason = $5,
           updated_at = NOW()
       WHERE id = $1
       RETURNING *`,
      [
        order.id,
        order.remaining_qty,
        order.status,
        order.rejection_reason || null,
        order.cancel_reason || null
      ]
    );

    Object.assign(order, result.rows[0]);
  }

  async #loadOrderForUpdate(tx, orderId, sessionId) {
    const result = await tx.query(
      `SELECT *
       FROM orders
       WHERE id = $1
         AND session_id = $2
       FOR UPDATE`,
      [orderId, sessionId]
    );

    const order = result.rows[0];
    if (!order) {
      throw new EngineError('ORDER_NOT_FOUND', 'The order does not exist.', { orderId, sessionId });
    }

    return cloneRow(order);
  }

  async #loadSessionStockForUpdate(tx, sessionStockId, sessionId) {
    const result = await tx.query(
      `SELECT *
       FROM session_stocks
       WHERE id = $1
         AND session_id = $2
       FOR UPDATE`,
      [sessionStockId, sessionId]
    );

    const sessionStock = result.rows[0];
    if (!sessionStock) {
      throw new EngineError('INVALID_STOCK_ID', 'The stock does not exist for this session.', {
        sessionId,
        sessionStockId
      });
    }

    return sessionStock;
  }
}

module.exports = {
  EngineError,
  MatchingEngine,
  STARTING_CASH_CENTS,
  STARTING_POSITION_QTY,
  MAX_BORROW_CENTS,
  MAX_SHORT_QTY
};
