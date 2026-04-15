const crypto = require('node:crypto');

const {
  EngineError,
  MatchingEngine,
  MAX_BORROW_CENTS,
  STARTING_CASH_CENTS,
  STARTING_POSITION_QTY
} = require('../engine/matchingEngine');
const {
  DEFAULT_GAME_DURATION_SECONDS,
  DEFAULT_PEEK_PRICE_CENTS,
  DEFAULT_REFERENCE_PRICE_CENTS,
  buildTradableStocks,
  SAR_BONUS_CENTS,
  buildPublicInfoSchedule,
  computeBorrowFeeCents,
  computeElapsedSeconds,
  computeRemainingSeconds,
  createShuffledDeck,
  formatCard,
  formatEpsMessage,
  formatSarMessage,
  hasSessionStarted,
  pickRandomItem,
  sampleWithoutReplacement,
  settleCashCents
} = require('./classroomGame');

const ORDER_BOOK_VISIBLE_LEVELS = 6;

class HttpError extends Error {
  constructor(status, code, message, details = {}) {
    super(message);
    this.name = 'HttpError';
    this.status = status;
    this.code = code;
    this.details = details;
  }
}

function randomToken() {
  return crypto.randomBytes(24).toString('hex');
}

function randomJoinCode() {
  return crypto.randomBytes(3).toString('hex').toUpperCase();
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

function formatSession(row, now = new Date()) {
  if (!row) {
    return null;
  }

  return {
    id: row.id,
    sessionName: row.session_name,
    joinCode: row.join_code,
    status: row.status,
    referencePriceCents: row.reference_price_cents,
    startedAt: row.started_at || null,
    openedAt: row.opened_at || null,
    totalDurationSeconds: row.total_duration_seconds || DEFAULT_GAME_DURATION_SECONDS,
    elapsedSeconds: computeElapsedSeconds(row, now),
    remainingSeconds: computeRemainingSeconds(row, now),
    liquidationValueCents: row.liquidation_revealed_at ? row.liquidation_value_cents : null,
    liquidationRevealedAt: row.liquidation_revealed_at || null,
    createdAt: row.created_at,
    updatedAt: row.updated_at
  };
}

function formatSessionStock(row) {
  if (!row) {
    return null;
  }

  return {
    id: row.id,
    sessionId: row.session_id,
    stockKey: row.stock_key,
    displayName: row.display_name,
    sortOrder: row.sort_order,
    referencePriceCents: row.reference_price_cents,
    initialPositionQty: row.initial_position_qty,
    liquidationValueCents: row.liquidation_value_cents ?? null,
    createdAt: row.created_at,
    updatedAt: row.updated_at
  };
}

function formatParticipant(row) {
  if (!row) {
    return null;
  }

  return {
    id: row.id,
    sessionId: row.session_id,
    externalId: row.external_id,
    displayName: row.display_name,
    role: row.role,
    authToken: row.auth_token,
    createdAt: row.created_at
  };
}

function formatHolding(row) {
  if (!row) {
    return null;
  }

  return {
    participantId: row.participant_id,
    sessionStockId: row.session_stock_id,
    stockKey: row.stock_key || null,
    stockDisplayName: row.display_name || null,
    positionQty: row.position_qty,
    reservedSellQty: row.reserved_sell_qty,
    updatedAt: row.updated_at
  };
}

function formatAccount(row, holdings = []) {
  if (!row) {
    return null;
  }

  return {
    participantId: row.participant_id,
    cashCents: row.cash_cents,
    positionQty: holdings.reduce((sum, holding) => sum + (Number(holding.positionQty) || 0), 0),
    reservedBuyCents: row.reserved_buy_cents,
    reservedSellQty: holdings.reduce((sum, holding) => sum + (Number(holding.reservedSellQty) || 0), 0),
    holdings,
    updatedAt: row.updated_at
  };
}

function formatOrder(row) {
  if (!row) {
    return null;
  }

  return {
    id: row.id,
    sessionId: row.session_id,
    sessionStockId: row.session_stock_id,
    stockKey: row.stock_key || null,
    stockDisplayName: row.display_name || null,
    participantId: row.participant_id,
    side: row.side,
    orderType: row.order_type,
    limitPriceCents: row.limit_price_cents,
    originalQty: row.original_qty,
    remainingQty: row.remaining_qty,
    filledQty: row.original_qty - row.remaining_qty,
    status: row.status,
    rejectionReason: row.rejection_reason,
    cancelReason: row.cancel_reason,
    createdAt: row.created_at,
    updatedAt: row.updated_at
  };
}

function formatFill(row) {
  if (!row) {
    return null;
  }

  return {
    id: row.id,
    sessionId: row.session_id,
    sessionStockId: row.session_stock_id,
    stockKey: row.stock_key || null,
    stockDisplayName: row.display_name || null,
    buyOrderId: row.buy_order_id,
    sellOrderId: row.sell_order_id,
    aggressorOrderId: row.aggressor_order_id,
    restingOrderId: row.resting_order_id,
    priceCents: row.price_cents,
    qty: row.qty,
    executedAt: row.executed_at
  };
}

function formatAnnouncement(row) {
  if (!row) {
    return null;
  }

  return {
    id: row.id,
    sessionId: row.session_id,
    sessionStockId: row.session_stock_id || null,
    stockKey: row.stock_key || null,
    stockDisplayName: row.display_name || null,
    actorParticipantId: row.actor_participant_id,
    announcementType: row.announcement_type || 'PUBLIC_INFO',
    message: row.message,
    payload: parseJsonColumn(row.payload_json),
    createdAt: row.created_at
  };
}

function formatScheduledPublicInfo(row) {
  if (!row) {
    return null;
  }

  return {
    id: row.id,
    sessionId: row.session_id,
    sessionStockId: row.session_stock_id || null,
    stockKey: row.stock_key || null,
    stockDisplayName: row.display_name || null,
    infoKey: row.info_key,
    infoType: row.info_type,
    sequenceNo: row.sequence_no,
    scheduledOffsetSeconds: row.scheduled_offset_seconds,
    status: row.status,
    releasedAt: row.released_at,
    payload: parseJsonColumn(row.payload_json),
    announcementId: row.announcement_id,
    createdAt: row.created_at
  };
}

function formatPeek(row) {
  if (!row) {
    return null;
  }

  const payload = parseJsonColumn(row.payload_json) || null;
  const contributionsCents = Array.isArray(payload?.contributionsCents)
    ? payload.contributionsCents.map((value) => Number(value))
    : row.contribution_cents !== null && row.contribution_cents !== undefined
      ? [Number(row.contribution_cents)]
      : [];

  return {
    id: row.id,
    sessionId: row.session_id,
    sessionStockId: row.session_stock_id || null,
    stockKey: row.stock_key || null,
    stockDisplayName: row.display_name || null,
    participantId: row.participant_id,
    priceCents: row.price_cents,
    createdAt: row.created_at,
    contributions: contributionsCents.map((value) => Math.round(value / 100))
  };
}

function sumCardContributionCents(cards) {
  return cards.reduce((sum, card) => sum + (Number(card.contributionCents) || 0), 0);
}

function makeEvent({ type, sessionId, scope = 'PUBLIC', participantId = null, payload = {} }) {
  return {
    type,
    scope,
    sessionId,
    participantId,
    sentAt: new Date().toISOString(),
    payload
  };
}

function uniqueIntegers(values) {
  return [...new Set(values.filter((value) => Number.isInteger(value)))];
}

class GameService {
  constructor({ db, config, engine } = {}) {
    if (!db) {
      throw new Error('db is required');
    }

    this.db = db;
    this.config = config || {};
    this.engine = engine || new MatchingEngine({ db });
  }

  async initialize() {
    await this.engine.initialize();
    await this.#ensureMultiStockData();
  }

  async restoreRuntimeState() {
    const activeResult = await this.db.query(
      `SELECT *
       FROM market_sessions
       WHERE status IN ('OPEN', 'PAUSED')
       ORDER BY id DESC
       LIMIT 1`
    );

    return {
      activeSession: formatSession(activeResult.rows[0] || null)
    };
  }

  async healthCheck() {
    await this.db.query('SELECT 1 AS ok');
    return { ok: true };
  }

  async createGameSession({ sessionName, referencePriceCents } = {}) {
    const payload = await this.#createGameSessionBundle({ sessionName, referencePriceCents });

    return {
      ...payload,
      events: [
        makeEvent({
          type: 'game.state',
          sessionId: payload.session.id,
          payload: {
            session: payload.session,
            stocks: payload.stocks
          }
        })
      ]
    };
  }

  async resetGameSession({ sessionName, referencePriceCents } = {}) {
    await this.db.withTransaction(async (tx) => {
      const closedSessions = await tx.query(
        `UPDATE market_sessions
         SET status = 'CLOSED',
             opened_at = NULL,
             updated_at = NOW()
         WHERE status IN ('OPEN', 'PAUSED')
         RETURNING id`
      );

      for (const row of closedSessions.rows) {
        await tx.query(
          `UPDATE scheduled_announcements
           SET status = 'CANCELLED'
           WHERE session_id = $1
             AND status = 'SCHEDULED'`,
          [row.id]
        );

        await tx.query(
          `UPDATE scheduled_public_info
           SET status = 'CANCELLED'
           WHERE session_id = $1
             AND status = 'PENDING'`,
          [row.id]
        );
      }
    });

    return this.createGameSession({ sessionName, referencePriceCents });
  }

  async joinSession({ joinCode, displayName }) {
    const resolvedJoinCode = String(joinCode || '').trim().toUpperCase();
    const resolvedDisplayName = String(displayName || '').trim();

    if (!resolvedJoinCode) {
      throw new HttpError(400, 'INVALID_JOIN_CODE', 'joinCode is required.');
    }

    if (!resolvedDisplayName) {
      throw new HttpError(400, 'INVALID_DISPLAY_NAME', 'displayName is required.');
    }

    const sessionResult = await this.db.query(
      `SELECT *
       FROM market_sessions
       WHERE join_code = $1`,
      [resolvedJoinCode]
    );

    const session = sessionResult.rows[0];
    if (!session) {
      throw new HttpError(404, 'SESSION_NOT_FOUND', 'No game session matches that join code.');
    }

    if (session.status === 'CLOSED') {
      throw new HttpError(409, 'SESSION_CLOSED', 'This game session is closed.');
    }

    const participantBundle = await this.engine.createParticipant({
      sessionId: session.id,
      externalId: `student-${randomToken()}`,
      displayName: resolvedDisplayName,
      role: 'STUDENT',
      authToken: randomToken()
    });

    return {
      session: formatSession(session),
      stocks: (await this.#getSessionStocks(session.id)).map((row) => formatSessionStock(row)),
      participant: formatParticipant(participantBundle.participant),
      account: formatAccount(
        participantBundle.account,
        (participantBundle.holdings || []).map((row) => formatHolding(row))
      )
    };
  }

  async authenticateParticipant(authToken) {
    const token = String(authToken || '').trim();
    if (!token) {
      throw new HttpError(401, 'AUTH_REQUIRED', 'A bearer token is required.');
    }

    const result = await this.db.query(
      `SELECT
         p.*,
         s.session_name,
         s.join_code,
         s.status AS session_status,
         s.reference_price_cents,
         s.started_at AS session_started_at,
         s.opened_at AS session_opened_at,
         s.elapsed_open_seconds AS session_elapsed_open_seconds,
         s.total_duration_seconds AS session_total_duration_seconds,
         s.liquidation_value_cents AS session_liquidation_value_cents,
         s.liquidation_revealed_at AS session_liquidation_revealed_at,
         s.created_at AS session_created_at,
         s.updated_at AS session_updated_at
       FROM participants p
       INNER JOIN market_sessions s ON s.id = p.session_id
       WHERE p.auth_token = $1`,
      [token]
    );

    const row = result.rows[0];
    if (!row) {
      throw new HttpError(401, 'INVALID_TOKEN', 'The bearer token is invalid.');
    }

    return {
      participantId: row.id,
      sessionId: row.session_id,
      displayName: row.display_name,
      role: row.role,
      authToken: row.auth_token,
      session: formatSession({
        id: row.session_id,
        session_name: row.session_name,
        join_code: row.join_code,
        status: row.session_status,
        reference_price_cents: row.reference_price_cents,
        started_at: row.session_started_at,
        opened_at: row.session_opened_at,
        elapsed_open_seconds: row.session_elapsed_open_seconds,
        total_duration_seconds: row.session_total_duration_seconds,
        liquidation_value_cents: row.session_liquidation_value_cents,
        liquidation_revealed_at: row.session_liquidation_revealed_at,
        created_at: row.session_created_at,
        updated_at: row.session_updated_at
      })
    };
  }

  async getOrderBookSnapshot(sessionId) {
    const session = await this.#loadSession(sessionId);
    const stocks = await this.#getSessionStocks(sessionId);
    const [stockSnapshots, announcements, liquidation] = await Promise.all([
      Promise.all(stocks.map((stock) => this.#getStockOrderBookSnapshot(session, stock))),
      this.#getAnnouncements(sessionId, this.config.ANNOUNCEMENT_HISTORY_LIMIT || 20),
      this.#getLiquidationSummary(session, stocks)
    ]);

    return {
      session: formatSession(session),
      stocks: stocks.map((row) => formatSessionStock(row)),
      liquidation,
      stockSnapshots,
      announcements: announcements.map((row) => formatAnnouncement(row))
    };
  }

  async getPlayerAccountState(participantId, sessionId) {
    const participant = await this.#loadParticipant(participantId, sessionId);
    const session = await this.#loadSession(sessionId);
    const stocks = await this.#getSessionStocks(sessionId);
    const account = await this.engine.getAccount(participantId);
    const holdings = await this.#getParticipantHoldings(participantId, sessionId);
    const [ordersResult, fillsResult, peeksResult] = await Promise.all([
      this.db.query(
        `SELECT o.*, ss.stock_key, ss.display_name
         FROM orders o
         INNER JOIN session_stocks ss ON ss.id = o.session_stock_id
         WHERE o.participant_id = $1
         ORDER BY o.id DESC`,
        [participantId]
      ),
      this.db.query(
        `SELECT
           f.*,
           ss.stock_key,
           ss.display_name,
           buy_orders.participant_id AS buy_participant_id,
           sell_orders.participant_id AS sell_participant_id
         FROM fills f
         INNER JOIN session_stocks ss ON ss.id = f.session_stock_id
         INNER JOIN orders buy_orders ON buy_orders.id = f.buy_order_id
         INNER JOIN orders sell_orders ON sell_orders.id = f.sell_order_id
         WHERE f.session_id = $1
           AND ($2 IN (buy_orders.participant_id, sell_orders.participant_id))
         ORDER BY f.id DESC
         LIMIT $3`,
        [sessionId, participantId, this.config.RECENT_TRADES_LIMIT || 20]
      ),
      this.db.query(
        `SELECT
           pp.*,
           ss.stock_key,
           ss.display_name,
           sc.contribution_cents
         FROM private_peeks pp
         INNER JOIN session_stocks ss ON ss.id = pp.session_stock_id
         LEFT JOIN session_cards sc ON sc.id = pp.card_id
         WHERE pp.participant_id = $1
         ORDER BY pp.id DESC
         LIMIT 3`,
        [participantId]
      )
    ]);

    return {
      session: formatSession(session),
      stocks: stocks.map((row) => formatSessionStock(row)),
      participant: formatParticipant(participant),
      account: formatAccount(account, holdings),
      orders: ordersResult.rows.map((row) => formatOrder(row)),
      recentFills: fillsResult.rows.map((row) => ({
        ...formatFill(row),
        side: row.buy_participant_id === participantId ? 'BUY' : 'SELL'
      })),
      privatePeeks: peeksResult.rows.map((row) => formatPeek(row))
    };
  }

  async getLeaderboard(sessionId) {
    const session = await this.#loadSession(sessionId);
    const stocks = await this.#getSessionStocks(sessionId);
    const markets = await Promise.all(
      stocks.map(async (stock) => ({
        stock,
        market: await this.#getMarketStats(session, stock)
      }))
    );
    const marketStatsByStockId = new Map(markets.map((entry) => [entry.stock.id, entry.market]));
    const initialPortfolioValueCents =
      STARTING_CASH_CENTS +
      stocks.reduce(
        (sum, stock) => sum + Number(stock.initial_position_qty || 0) * Number(stock.reference_price_cents || 0),
        0
      );

    const result = await this.db.query(
      `SELECT
         p.id AS participant_id,
         p.display_name,
         a.cash_cents
       FROM participants p
       INNER JOIN accounts a ON a.participant_id = p.id
       WHERE p.session_id = $1
         AND p.role = 'STUDENT'
       ORDER BY p.id ASC`,
      [sessionId]
    );
    const holdings = await this.#getStudentHoldings(sessionId);
    const holdingsByParticipantId = holdings.reduce((map, holding) => {
      const bucket = map.get(holding.participantId) || [];
      bucket.push(holding);
      map.set(holding.participantId, bucket);
      return map;
    }, new Map());

    const rows = result.rows.map((row) => ({
      participantId: row.participant_id,
      displayName: row.display_name,
      cashCents: row.cash_cents,
      settledCashCents: session.liquidation_revealed_at
        ? settleCashCents(row.cash_cents, this.config.BORROW_INTEREST_BPS)
        : row.cash_cents,
      borrowingFeeCents: session.liquidation_revealed_at
        ? computeBorrowFeeCents(row.cash_cents, this.config.BORROW_INTEREST_BPS)
        : 0,
      holdings: holdingsByParticipantId.get(row.participant_id) || [],
      portfolioValueCents:
        (session.liquidation_revealed_at
          ? settleCashCents(row.cash_cents, this.config.BORROW_INTEREST_BPS)
          : row.cash_cents) +
        (holdingsByParticipantId.get(row.participant_id) || []).reduce((sum, holding) => {
          const market = marketStatsByStockId.get(holding.sessionStockId);
          return sum + holding.positionQty * (market?.markPriceCents ?? 0);
        }, 0),
      returnBps: Math.round(
        (((session.liquidation_revealed_at
          ? settleCashCents(row.cash_cents, this.config.BORROW_INTEREST_BPS)
          : row.cash_cents) +
          (holdingsByParticipantId.get(row.participant_id) || []).reduce((sum, holding) => {
            const market = marketStatsByStockId.get(holding.sessionStockId);
            return sum + holding.positionQty * (market?.markPriceCents ?? 0);
          }, 0) -
          initialPortfolioValueCents) *
          10000) /
          initialPortfolioValueCents
      )
    }));

    rows.sort((left, right) => right.portfolioValueCents - left.portfolioValueCents || left.displayName.localeCompare(right.displayName));

    return {
      session: formatSession(session),
      stocks: stocks.map((row) => formatSessionStock(row)),
      markets: markets.map((entry) => ({
        stock: formatSessionStock(entry.stock),
        market: entry.market
      })),
      liquidation: await this.#getLiquidationSummary(session, stocks),
      leaderboard: rows.map((row, index) => ({
        ...row,
        positionQty: row.holdings.reduce((sum, holding) => sum + holding.positionQty, 0),
        rank: index + 1
      }))
    };
  }

  async submitOrder({ sessionId, participantId, sessionStockId, idempotencyKey, side, orderType, quantity, limitPriceCents }) {
    const resolvedStock = await this.#resolveRequestedStock(sessionId, sessionStockId);
    const response = await this.engine.submitOrder({
      sessionId,
      sessionStockId: resolvedStock.id,
      participantId,
      idempotencyKey,
      side,
      orderType,
      quantity,
      limitPriceCents
    });

    const events = await this.#buildPostTradeEvents({
      sessionId,
      submitterParticipantId: participantId,
      orderResponse: response
    });

    return {
      ...response,
      events
    };
  }

  async cancelOrder({ sessionId, participantId, orderId }) {
    const response = await this.engine.cancelOrder({
      sessionId,
      participantId,
      orderId
    });

    const snapshot = await this.getOrderBookSnapshot(sessionId);
    const accountState = await this.getPlayerAccountState(participantId, sessionId);

    return {
      ...response,
      events: [
        makeEvent({
          type: 'orderbook.updated',
          sessionId,
          payload: snapshot
        }),
        makeEvent({
          type: 'account.updated',
          sessionId,
          scope: 'PRIVATE',
          participantId,
          payload: accountState
        })
      ]
    };
  }

  async purchasePeek({ sessionId, participantId, sessionStockId }) {
    const peekPriceCents = this.config.PEEK_PRICE_CENTS || DEFAULT_PEEK_PRICE_CENTS;
    const resolvedStock = await this.#resolveRequestedStock(sessionId, sessionStockId);
    const peekResult = await this.engine.lockManager.withLock(`session:${sessionId}`, async () =>
      this.db.withTransaction(async (tx) => {
        const session = await this.#loadSessionForUpdate(tx, sessionId);
        if (session.status === 'CLOSED') {
          throw new HttpError(409, 'SESSION_CLOSED', 'Private peeks are unavailable after the session closes.');
        }

        const participant = await this.#loadParticipantForUpdate(tx, participantId, sessionId);
        if (participant.role !== 'STUDENT') {
          throw new HttpError(403, 'FORBIDDEN', 'Only students can buy private peeks.');
        }

        const accountResult = await tx.query(
          `SELECT *
           FROM accounts
           WHERE participant_id = $1
           FOR UPDATE`,
          [participantId]
        );
        const account = accountResult.rows[0];
        const nextCashCents = account.cash_cents - peekPriceCents;
        if (nextCashCents < -MAX_BORROW_CENTS) {
          throw new HttpError(409, 'BORROW_LIMIT_BREACH', 'Buying this peek would exceed the borrowing limit.');
        }

        const sessionStock = await this.#loadSessionStockForUpdateTx(tx, resolvedStock.id, sessionId);
        const activeCardsResult = await tx.query(
          `SELECT *
           FROM session_cards
           WHERE session_id = $1
             AND session_stock_id = $2
             AND state = 'ACTIVE'
           ORDER BY id ASC
           FOR UPDATE`,
          [sessionId, sessionStock.id]
        );
        const chosenCards = sampleWithoutReplacement(activeCardsResult.rows, 3);
        if (chosenCards.length < 3) {
          throw new HttpError(409, 'NO_ACTIVE_CARDS', 'At least three active cards are required to buy a private peek.');
        }

        const payloadJson = {
          sampleCardIds: chosenCards.map((card) => card.id),
          contributionsCents: chosenCards.map((card) => card.contribution_cents)
        };

        const insertedPeek = await tx.query(
          `INSERT INTO private_peeks (session_id, session_stock_id, participant_id, card_id, price_cents, payload_json)
           VALUES ($1, $2, $3, NULL, $4, $5::jsonb)
           RETURNING *`,
          [sessionId, sessionStock.id, participantId, peekPriceCents, JSON.stringify(payloadJson)]
        );

        await tx.query(
          `UPDATE accounts
           SET cash_cents = cash_cents - $2,
               updated_at = NOW()
           WHERE participant_id = $1`,
          [participantId, peekPriceCents]
        );

        const updatedAccount = await tx.query(
          `SELECT *
           FROM accounts
           WHERE participant_id = $1`,
          [participantId]
        );

        return {
          session,
          account: updatedAccount.rows[0],
          peek: {
            ...insertedPeek.rows[0],
            stock_key: sessionStock.stock_key,
            display_name: sessionStock.display_name,
            payload_json: payloadJson
          }
        };
      })
    );

    const accountState = await this.getPlayerAccountState(participantId, sessionId);
    const instructorsResult = await this.db.query(
      `SELECT id
       FROM participants
       WHERE session_id = $1
         AND role = 'INSTRUCTOR'`,
      [sessionId]
    );
    const leaderboard = await this.getLeaderboard(sessionId);

    return {
      peek: formatPeek(peekResult.peek),
      account: formatAccount(peekResult.account, accountState.account.holdings),
      events: [
        makeEvent({
          type: 'account.updated',
          sessionId,
          scope: 'PRIVATE',
          participantId,
          payload: accountState
        }),
        makeEvent({
          type: 'peek.revealed',
          sessionId,
          scope: 'PRIVATE',
          participantId,
          payload: {
            peek: formatPeek(peekResult.peek)
          }
        }),
        ...instructorsResult.rows.map((row) =>
          makeEvent({
            type: 'leaderboard.updated',
            sessionId,
            scope: 'PRIVATE',
            participantId: row.id,
            payload: leaderboard
          })
        )
      ]
    };
  }

  async setSessionState({ sessionId, actorParticipantId, status }) {
    const actor = await this.#loadParticipant(actorParticipantId, sessionId);
    if (actor.role !== 'INSTRUCTOR') {
      throw new HttpError(403, 'FORBIDDEN', 'Only the instructor can change the game state.');
    }

    const normalizedStatus = String(status || '').toUpperCase();
    if (!['OPEN', 'PAUSED', 'CLOSED'].includes(normalizedStatus)) {
      throw new HttpError(400, 'INVALID_SESSION_STATUS', 'status must be OPEN, PAUSED, or CLOSED.');
    }

    const now = new Date();
    const session = await this.db.withTransaction(async (tx) => {
      const currentSession = await this.#loadSessionForUpdate(tx, sessionId);
      const nextElapsedSeconds = this.#computeClosedFormElapsedSeconds(currentSession, now);
      const nextStartedAt =
        normalizedStatus === 'OPEN' ? currentSession.started_at || now.toISOString() : currentSession.started_at;
      const nextOpenedAt = normalizedStatus === 'OPEN' ? now.toISOString() : null;

      const updatedResult = await tx.query(
        `UPDATE market_sessions
         SET status = $2,
             started_at = $3,
             opened_at = $4,
             elapsed_open_seconds = $5,
             updated_at = NOW()
         WHERE id = $1
         RETURNING *`,
        [sessionId, normalizedStatus, nextStartedAt, nextOpenedAt, nextElapsedSeconds]
      );

      if (normalizedStatus === 'CLOSED') {
        await tx.query(
          `UPDATE scheduled_public_info
           SET status = 'CANCELLED'
           WHERE session_id = $1
             AND status = 'PENDING'`,
          [sessionId]
        );
      }

      return updatedResult.rows[0];
    });

    const events = [
      makeEvent({
        type: 'game.state',
        sessionId,
        payload: {
          session: formatSession(session),
          stocks: (await this.#getSessionStocks(sessionId)).map((row) => formatSessionStock(row))
        }
      })
    ];

    if (normalizedStatus === 'OPEN') {
      events.push(...(await this.processDueAnnouncements()));
    }

    return {
      session: formatSession(session),
      events
    };
  }

  async processDueAnnouncements() {
    const publicInfoEvents = await this.#processDuePublicInfo();
    const expiredSessionEvents = await this.#processExpiredSessions();
    return [...publicInfoEvents, ...expiredSessionEvents];
  }

  async getStudents(sessionId, connectedParticipantIds = []) {
    const session = await this.#loadSession(sessionId);
    const stocks = await this.#getSessionStocks(sessionId);
    const connectedSet = new Set(connectedParticipantIds);
    const studentsResult = await this.db.query(
      `SELECT
         p.id,
         p.display_name,
         p.created_at,
         a.cash_cents,
         a.reserved_buy_cents
       FROM participants p
       INNER JOIN accounts a ON a.participant_id = p.id
       WHERE p.session_id = $1
         AND p.role = 'STUDENT'
       ORDER BY p.display_name ASC`,
      [sessionId]
    );
    const [holdings, openOrdersResult] = await Promise.all([
      this.#getStudentHoldings(sessionId),
      this.db.query(
        `SELECT participant_id, COUNT(*) AS open_order_count
         FROM orders
         WHERE session_id = $1
           AND status IN ('OPEN', 'PARTIALLY_FILLED')
         GROUP BY participant_id`,
        [sessionId]
      )
    ]);
    const holdingsByParticipantId = holdings.reduce((map, holding) => {
      const bucket = map.get(holding.participantId) || [];
      bucket.push(holding);
      map.set(holding.participantId, bucket);
      return map;
    }, new Map());
    const openOrdersByParticipantId = new Map(
      openOrdersResult.rows.map((row) => [row.participant_id, Number(row.open_order_count || 0)])
    );

    return {
      session: formatSession(session),
      stocks: stocks.map((row) => formatSessionStock(row)),
      students: studentsResult.rows.map((row) => {
        const studentHoldings = holdingsByParticipantId.get(row.id) || [];
        return {
        participantId: row.id,
        displayName: row.display_name,
        createdAt: row.created_at,
        cashCents: row.cash_cents,
        positionQty: studentHoldings.reduce((sum, holding) => sum + holding.positionQty, 0),
        holdings: studentHoldings,
        reservedBuyCents: row.reserved_buy_cents,
        reservedSellQty: studentHoldings.reduce((sum, holding) => sum + holding.reservedSellQty, 0),
        openOrderCount: openOrdersByParticipantId.get(row.id) || 0,
        connected: connectedSet.has(row.id)
      };
      })
    };
  }

  async getInstructorDashboard(sessionId, connectedParticipantIds = []) {
    const [session, orderBook, leaderboard, students, publicInfoSchedule] = await Promise.all([
      this.getSessionState(sessionId),
      this.getOrderBookSnapshot(sessionId),
      this.getLeaderboard(sessionId),
      this.getStudents(sessionId, connectedParticipantIds),
      this.#getScheduledPublicInfo(sessionId)
    ]);

    return {
      session: session.session,
      stocks: orderBook.stocks,
      liquidation: orderBook.liquidation,
      orderBook,
      leaderboard: leaderboard.leaderboard,
      students: students.students,
      publicInfoSchedule
    };
  }

  async finalizeSession({ sessionId, actorParticipantId }) {
    const actor = await this.#loadParticipant(actorParticipantId, sessionId);
    if (actor.role !== 'INSTRUCTOR') {
      throw new HttpError(403, 'FORBIDDEN', 'Only the instructor can finalize the game.');
    }

    const session = await this.db.withTransaction(async (tx) => {
      const currentSession = await this.#loadSessionForUpdate(tx, sessionId);
      if (currentSession.liquidation_revealed_at) {
        return currentSession;
      }

      const liquidationValues = await this.#getCurrentLiquidationValuesByStockTx(tx, sessionId);
      const liquidationValueCents = [...liquidationValues.values()].reduce((sum, value) => sum + value, 0);
      for (const [sessionStockId, valueCents] of liquidationValues.entries()) {
        await tx.query(
          `UPDATE session_stocks
           SET liquidation_value_cents = $2,
               updated_at = NOW()
           WHERE id = $1`,
          [sessionStockId, valueCents]
        );
      }
      const nextElapsedSeconds = this.#computeClosedFormElapsedSeconds(currentSession, new Date());
      const updated = await tx.query(
        `UPDATE market_sessions
         SET status = 'CLOSED',
             opened_at = NULL,
             elapsed_open_seconds = $2,
             liquidation_value_cents = $3,
             liquidation_revealed_at = NOW(),
             updated_at = NOW()
         WHERE id = $1
         RETURNING *`,
        [sessionId, nextElapsedSeconds, liquidationValueCents]
      );

      await tx.query(
        `UPDATE scheduled_public_info
         SET status = 'CANCELLED'
         WHERE session_id = $1
           AND status = 'PENDING'`,
        [sessionId]
      );

      return updated.rows[0];
    });

    const stocks = (await this.#getSessionStocks(sessionId)).map((row) => formatSessionStock(row));
    const leaderboard = await this.getLeaderboard(sessionId);
    return {
      session: formatSession(session),
      leaderboard,
      events: [
        makeEvent({
          type: 'game.state',
          sessionId,
          payload: {
            session: formatSession(session),
            stocks
          }
        }),
        makeEvent({
          type: 'leaderboard.final',
          sessionId,
          payload: leaderboard
        })
      ]
    };
  }

  async getSessionState(sessionId) {
    const session = await this.#loadSession(sessionId);
    const stocks = await this.#getSessionStocks(sessionId);
    return {
      session: formatSession(session),
      stocks: stocks.map((row) => formatSessionStock(row))
    };
  }

  async getRealtimeBootstrap(principal) {
    const [snapshot, accountState] = await Promise.all([
      this.getOrderBookSnapshot(principal.sessionId),
      this.getPlayerAccountState(principal.participantId, principal.sessionId)
    ]);

    return {
      ready: makeEvent({
        type: 'connection.ready',
        sessionId: principal.sessionId,
        scope: 'PRIVATE',
        participantId: principal.participantId,
        payload: {
          participantId: principal.participantId,
          role: principal.role,
          sessionId: principal.sessionId
        }
      }),
      session: makeEvent({
        type: 'game.state',
        sessionId: principal.sessionId,
        payload: {
          session: snapshot.session,
          stocks: snapshot.stocks
        }
      }),
      orderBook: makeEvent({
        type: 'orderbook.snapshot',
        sessionId: principal.sessionId,
        payload: snapshot
      }),
      account: makeEvent({
        type: 'account.updated',
        sessionId: principal.sessionId,
        scope: 'PRIVATE',
        participantId: principal.participantId,
        payload: accountState
      }),
      leaderboard: principal.role === 'INSTRUCTOR'
        ? makeEvent({
            type: 'leaderboard.updated',
            sessionId: principal.sessionId,
            scope: 'PRIVATE',
            participantId: principal.participantId,
            payload: await this.getLeaderboard(principal.sessionId)
          })
        : null
    };
  }

  async getParticipantSummary(participantId, sessionId) {
    const participant = await this.#loadParticipant(participantId, sessionId);
    const stocks = await this.#getSessionStocks(sessionId);
    return {
      participant: formatParticipant(participant),
      stocks: stocks.map((row) => formatSessionStock(row))
    };
  }

  async getAuthContext(authToken) {
    const principal = await this.authenticateParticipant(authToken);
    const stocks = await this.#getSessionStocks(principal.sessionId);
    return {
      principal,
      stocks: stocks.map((row) => formatSessionStock(row))
    };
  }

  async getPeekAvailability() {
    return {
      supported: true,
      priceCents: this.config.PEEK_PRICE_CENTS || DEFAULT_PEEK_PRICE_CENTS
    };
  }

  async #createGameSessionBundle({ sessionName, referencePriceCents } = {}) {
    const resolvedSessionName =
      typeof sessionName === 'string' && sessionName.trim() ? sessionName.trim() : 'Classroom Session';
    const resolvedReferencePriceCents =
      Number.isInteger(referencePriceCents) && referencePriceCents > 0
        ? referencePriceCents
        : this.config.DEFAULT_REFERENCE_PRICE_CENTS || DEFAULT_REFERENCE_PRICE_CENTS;
    const resolvedDurationSeconds =
      Number.isInteger(this.config.GAME_DURATION_SECONDS) && this.config.GAME_DURATION_SECONDS > 0
        ? this.config.GAME_DURATION_SECONDS
        : DEFAULT_GAME_DURATION_SECONDS;

    const bundle = await this.db.withTransaction(async (tx) => {
      const sessionResult = await tx.query(
        `INSERT INTO market_sessions (
           session_name,
           join_code,
           status,
           reference_price_cents,
           total_duration_seconds
         )
         VALUES ($1, $2, 'PAUSED', $3, $4)
         RETURNING *`,
        [resolvedSessionName, randomJoinCode(), resolvedReferencePriceCents, resolvedDurationSeconds]
      );
      const session = sessionResult.rows[0];

      const sessionStocks = [];
      for (const stock of buildTradableStocks(resolvedReferencePriceCents)) {
        const stockResult = await tx.query(
          `INSERT INTO session_stocks (
             session_id,
             stock_key,
             display_name,
             sort_order,
             reference_price_cents,
             initial_position_qty
           )
           VALUES ($1, $2, $3, $4, $5, $6)
           RETURNING *`,
          [
            session.id,
            stock.stockKey,
            stock.displayName,
            stock.sortOrder,
            stock.referencePriceCents,
            stock.initialPositionQty
          ]
        );
        sessionStocks.push(stockResult.rows[0]);
      }

      const instructor = await this.#insertParticipantWithAccount(tx, {
        sessionId: session.id,
        sessionStocks,
        externalId: `instructor-${randomToken()}`,
        displayName: 'Instructor',
        role: 'INSTRUCTOR',
        authToken: randomToken()
      });

      for (const sessionStock of sessionStocks) {
        for (const card of createShuffledDeck()) {
          await tx.query(
            `INSERT INTO session_cards (
               session_id,
               session_stock_id,
               deck_order,
               rank,
               suit,
               color,
               label,
               base_value_cents,
               contribution_cents,
               state
             )
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
            [
              session.id,
              sessionStock.id,
              card.deckOrder,
              card.rank,
              card.suit,
              card.color,
              card.label,
              card.baseValueCents,
              card.contributionCents,
              card.state
            ]
          );
        }

        for (const scheduledItem of buildPublicInfoSchedule(resolvedDurationSeconds)) {
          await tx.query(
            `INSERT INTO scheduled_public_info (
               session_id,
               session_stock_id,
               info_key,
               info_type,
               sequence_no,
               scheduled_offset_seconds
             )
             VALUES ($1, $2, $3, $4, $5, $6)`,
            [
              session.id,
              sessionStock.id,
              `${sessionStock.stock_key}_${scheduledItem.infoKey}`,
              scheduledItem.infoType,
              scheduledItem.sequenceNo,
              scheduledItem.scheduledOffsetSeconds
            ]
          );
        }
      }

      return {
        session,
        sessionStocks,
        instructor
      };
    });

    return {
      session: formatSession(bundle.session),
      stocks: bundle.sessionStocks.map((row) => formatSessionStock(row)),
      instructor: {
        participant: formatParticipant(bundle.instructor.participant),
        account: formatAccount(
          bundle.instructor.account,
          bundle.instructor.holdings.map((row) => formatHolding(row))
        )
      }
    };
  }

  async #insertParticipantWithAccount(tx, { sessionId, sessionStocks, externalId, displayName, role, authToken }) {
    const participantResult = await tx.query(
      `INSERT INTO participants (session_id, external_id, display_name, role, auth_token)
       VALUES ($1, $2, $3, $4, $5)
       RETURNING *`,
      [sessionId, externalId, displayName, role, authToken]
    );
    const participant = participantResult.rows[0];

    const accountResult = await tx.query(
      `INSERT INTO accounts (participant_id, cash_cents, position_qty, reserved_sell_qty)
       VALUES ($1, $2, 0, 0)
       RETURNING *`,
      [participant.id, STARTING_CASH_CENTS]
    );

    const holdings = [];
    for (const sessionStock of sessionStocks || []) {
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
  }

  async #getScheduledPublicInfo(sessionId) {
    const result = await this.db.query(
      `SELECT spi.*, ss.stock_key, ss.display_name
       FROM scheduled_public_info spi
       INNER JOIN session_stocks ss ON ss.id = spi.session_stock_id
       WHERE spi.session_id = $1
       ORDER BY spi.scheduled_offset_seconds ASC, ss.sort_order ASC, spi.id ASC`,
      [sessionId]
    );

    return result.rows.map((row) => formatScheduledPublicInfo(row));
  }

  async #getLiquidationSummary(session, sessionStocks = null) {
    const stocks = sessionStocks || (await this.#getSessionStocks(session.id));

    if (!session?.liquidation_revealed_at) {
      return {
        revealed: false,
        valueCents: null,
        revealedAt: null,
        stocks: stocks.map((stock) => ({
          stock: formatSessionStock(stock),
          valueCents: null,
          cards: []
        }))
      };
    }

    const cardsResult = await this.db.query(
      `SELECT sc.*, ss.stock_key, ss.display_name
       FROM session_cards sc
       INNER JOIN session_stocks ss ON ss.id = sc.session_stock_id
       WHERE sc.session_id = $1
         AND sc.state = 'ACTIVE'
       ORDER BY ss.sort_order ASC, sc.deck_order ASC`,
      [session.id]
    );
    const cardsByStockId = cardsResult.rows.reduce((map, row) => {
      const bucket = map.get(row.session_stock_id) || [];
      bucket.push(formatCard(row));
      map.set(row.session_stock_id, bucket);
      return map;
    }, new Map());

    return {
      revealed: true,
      valueCents: session.liquidation_value_cents,
      revealedAt: session.liquidation_revealed_at,
      stocks: stocks.map((stock) => ({
        stock: formatSessionStock(stock),
        valueCents: stock.liquidation_value_cents ?? null,
        cards: cardsByStockId.get(stock.id) || []
      }))
    };
  }

  async #processDuePublicInfo() {
    const now = new Date();
    const releasedAnnouncements = await this.db.withTransaction(async (tx) => {
      const dueResult = await tx.query(
        `SELECT
           spi.*,
           ss.stock_key,
           ss.display_name,
           ms.started_at,
           ms.opened_at,
           ms.elapsed_open_seconds
         FROM scheduled_public_info spi
         INNER JOIN session_stocks ss ON ss.id = spi.session_stock_id
         INNER JOIN market_sessions ms ON ms.id = spi.session_id
         WHERE spi.status = 'PENDING'
           AND ms.status IN ('OPEN', 'PAUSED')
           AND ms.started_at IS NOT NULL
         ORDER BY spi.session_id ASC, spi.scheduled_offset_seconds ASC, spi.id ASC
         FOR UPDATE`
      );

      const released = [];

      for (const row of dueResult.rows) {
        const elapsedSeconds = this.#computeClosedFormElapsedSeconds(row, now);
        if (elapsedSeconds < row.scheduled_offset_seconds) {
          continue;
        }

        const release =
          row.info_type === 'SAR'
            ? await this.#releaseSarTx(tx, row)
            : await this.#releaseEpsTx(tx, row);

        const announcementResult = await tx.query(
          `INSERT INTO announcements (
             session_id,
             session_stock_id,
             actor_participant_id,
             message,
             announcement_type,
             payload_json
           )
           VALUES ($1, $2, NULL, $3, $4, $5::jsonb)
           RETURNING *`,
          [row.session_id, row.session_stock_id, release.message, row.info_type, JSON.stringify(release.publicPayload)]
        );
        const announcement = {
          ...announcementResult.rows[0],
          stock_key: row.stock_key,
          display_name: row.display_name
        };

        await tx.query(
          `UPDATE scheduled_public_info
           SET status = 'RELEASED',
               released_at = NOW(),
               announcement_id = $2,
               payload_json = $3::jsonb
           WHERE id = $1`,
          [row.id, announcement.id, JSON.stringify(release.auditPayload)]
        );

        released.push(announcement);
      }

      return released;
    });

    return releasedAnnouncements.map((row) =>
      makeEvent({
        type: 'announcement.created',
        sessionId: row.session_id,
        payload: {
          announcement: formatAnnouncement(row)
        }
      })
    );
  }

  async #processExpiredSessions() {
    const now = new Date();
    const finalizedSessions = await this.db.withTransaction(async (tx) => {
      const result = await tx.query(
        `SELECT *
         FROM market_sessions
         WHERE status IN ('OPEN', 'PAUSED')
           AND started_at IS NOT NULL
           AND liquidation_revealed_at IS NULL
         ORDER BY id ASC
         FOR UPDATE`
      );

      const finalized = [];

      for (const session of result.rows) {
        if (this.#computeClosedFormElapsedSeconds(session, now) < Number(session.total_duration_seconds || 0)) {
          continue;
        }

        const liquidationValues = await this.#getCurrentLiquidationValuesByStockTx(tx, session.id);
        const liquidationValueCents = [...liquidationValues.values()].reduce((sum, value) => sum + value, 0);
        for (const [sessionStockId, valueCents] of liquidationValues.entries()) {
          await tx.query(
            `UPDATE session_stocks
             SET liquidation_value_cents = $2,
                 updated_at = NOW()
             WHERE id = $1`,
            [sessionStockId, valueCents]
          );
        }
        const closedResult = await tx.query(
          `UPDATE market_sessions
           SET status = 'CLOSED',
               opened_at = NULL,
               elapsed_open_seconds = $2,
               liquidation_value_cents = $3,
               liquidation_revealed_at = NOW(),
               updated_at = NOW()
           WHERE id = $1
           RETURNING *`,
          [session.id, this.#computeClosedFormElapsedSeconds(session, now), liquidationValueCents]
        );

        await tx.query(
          `UPDATE scheduled_public_info
           SET status = 'CANCELLED'
           WHERE session_id = $1
             AND status = 'PENDING'`,
          [session.id]
        );

        finalized.push(closedResult.rows[0]);
      }

      return finalized;
    });

    const events = [];

    for (const session of finalizedSessions) {
      const stocks = (await this.#getSessionStocks(session.id)).map((row) => formatSessionStock(row));
      const leaderboard = await this.getLeaderboard(session.id);
      events.push(
        makeEvent({
          type: 'game.state',
          sessionId: session.id,
          payload: {
            session: formatSession(session),
            stocks
          }
        }),
        makeEvent({
          type: 'leaderboard.final',
          sessionId: session.id,
          payload: leaderboard
        })
      );
    }

    return events;
  }

  async #releaseSarTx(tx, scheduledInfo) {
    const activeCardsResult = await tx.query(
      `SELECT *
       FROM session_cards
       WHERE session_id = $1
         AND session_stock_id = $2
         AND state = 'ACTIVE'
       ORDER BY id ASC
       FOR UPDATE`,
      [scheduledInfo.session_id, scheduledInfo.session_stock_id]
    );
    const chosenCards = sampleWithoutReplacement(activeCardsResult.rows, 5);
    const fiveCardSumCents = chosenCards.reduce((sum, card) => sum + card.contribution_cents, 0);
    const reportTotalCents = fiveCardSumCents + SAR_BONUS_CENTS;

    return {
      message: formatSarMessage(scheduledInfo.sequence_no, reportTotalCents, scheduledInfo.display_name),
      publicPayload: {
        sessionStockId: scheduledInfo.session_stock_id,
        stockKey: scheduledInfo.stock_key,
        stockDisplayName: scheduledInfo.display_name,
        infoType: 'SAR',
        sequenceNo: scheduledInfo.sequence_no,
        reportTotalCents
      },
      auditPayload: {
        sessionStockId: scheduledInfo.session_stock_id,
        stockKey: scheduledInfo.stock_key,
        infoType: 'SAR',
        sequenceNo: scheduledInfo.sequence_no,
        reportTotalCents,
        selectedCardIds: chosenCards.map((card) => card.id)
      }
    };
  }

  async #releaseEpsTx(tx, scheduledInfo) {
    const activeCardsResult = await tx.query(
      `SELECT *
       FROM session_cards
       WHERE session_id = $1
         AND session_stock_id = $2
         AND state = 'ACTIVE'
       ORDER BY id ASC
       FOR UPDATE`,
      [scheduledInfo.session_id, scheduledInfo.session_stock_id]
    );
    const deckCardResult = await tx.query(
      `SELECT *
       FROM session_cards
       WHERE session_id = $1
         AND session_stock_id = $2
         AND state = 'DECK'
       ORDER BY deck_order ASC
       LIMIT 1
       FOR UPDATE`,
      [scheduledInfo.session_id, scheduledInfo.session_stock_id]
    );

    const removedCard = pickRandomItem(activeCardsResult.rows);
    const addedCard = deckCardResult.rows[0];
    if (!removedCard || !addedCard) {
      throw new HttpError(409, 'EPS_RELEASE_UNAVAILABLE', 'Unable to release EPS information for this session.');
    }

    await tx.query(
      `UPDATE session_cards
       SET state = 'REMOVED',
           updated_at = NOW()
       WHERE id = $1`,
      [removedCard.id]
    );
    await tx.query(
      `UPDATE session_cards
       SET state = 'ACTIVE',
           updated_at = NOW()
       WHERE id = $1`,
      [addedCard.id]
    );

    const deltaCents = addedCard.contribution_cents - removedCard.contribution_cents;
    return {
      message: formatEpsMessage(scheduledInfo.sequence_no, deltaCents, scheduledInfo.display_name),
      publicPayload: {
        sessionStockId: scheduledInfo.session_stock_id,
        stockKey: scheduledInfo.stock_key,
        stockDisplayName: scheduledInfo.display_name,
        infoType: 'EPS',
        sequenceNo: scheduledInfo.sequence_no,
        deltaCents
      },
      auditPayload: {
        sessionStockId: scheduledInfo.session_stock_id,
        stockKey: scheduledInfo.stock_key,
        infoType: 'EPS',
        sequenceNo: scheduledInfo.sequence_no,
        deltaCents,
        removedCardId: removedCard.id,
        addedCardId: addedCard.id
      }
    };
  }

  async #getRecentFills(sessionId, limit, sessionStockId = null) {
    const params = [sessionId];
    let stockFilter = '';
    if (Number.isInteger(sessionStockId)) {
      params.push(sessionStockId);
      stockFilter = ` AND f.session_stock_id = $2`;
    }

    params.push(limit);
    const result = await this.db.query(
      `SELECT f.*, ss.stock_key, ss.display_name
       FROM fills f
       INNER JOIN session_stocks ss ON ss.id = f.session_stock_id
       WHERE f.session_id = $1
       ${stockFilter}
       ORDER BY id DESC
       LIMIT $${params.length}`,
      params
    );

    return result.rows.reverse();
  }

  async #getAnnouncements(sessionId, limit) {
    const result = await this.db.query(
      `SELECT a.*, ss.stock_key, ss.display_name
       FROM announcements a
       LEFT JOIN session_stocks ss ON ss.id = a.session_stock_id
       WHERE a.session_id = $1
       ORDER BY id DESC
       LIMIT $2`,
      [sessionId, limit]
    );

    return result.rows.reverse();
  }

  async #getMarketStats(session, stock) {
    const [lastFillResult, volumeResult] = await Promise.all([
      this.db.query(
        `SELECT price_cents
         FROM fills
         WHERE session_id = $1
           AND session_stock_id = $2
         ORDER BY id DESC
         LIMIT 1`,
        [session.id, stock.id]
      ),
      this.db.query(
        `SELECT COALESCE(SUM(qty), 0) AS traded_qty
         FROM fills
         WHERE session_id = $1
           AND session_stock_id = $2`,
        [session.id, stock.id]
      )
    ]);

    const lastTradePriceCents = lastFillResult.rows[0]?.price_cents ?? stock.reference_price_cents;
    const markPriceCents = session.liquidation_revealed_at
      ? stock.liquidation_value_cents ?? lastTradePriceCents
      : lastTradePriceCents;
    return {
      lastTradePriceCents,
      totalVolumeQty: Number(volumeResult.rows[0]?.traded_qty || 0),
      markPriceCents
    };
  }

  async #buildPostTradeEvents({ sessionId, submitterParticipantId, orderResponse }) {
    const snapshot = await this.getOrderBookSnapshot(sessionId);
    const marketState = {
      session: snapshot.session,
      stocks: snapshot.stocks,
      stockSnapshots: snapshot.stockSnapshots
    };
    const events = [
      makeEvent({
        type: 'orderbook.updated',
        sessionId,
        payload: snapshot
      }),
      makeEvent({
        type: 'game.state',
        sessionId,
        payload: marketState
      })
    ];

    for (const fill of orderResponse.fills) {
      events.push(
        makeEvent({
          type: 'trade.executed',
          sessionId,
          payload: {
            fill
          }
        })
      );
    }

    const orderIds = uniqueIntegers([
      ...orderResponse.fills.map((fill) => fill.buyOrderId),
      ...orderResponse.fills.map((fill) => fill.sellOrderId),
      orderResponse.order.id
    ]);

    const impactedParticipants = new Set([submitterParticipantId]);
    if (orderIds.length > 0) {
      const placeholders = orderIds.map((_, index) => `$${index + 1}`).join(', ');
      const participantsResult = await this.db.query(
        `SELECT DISTINCT participant_id
         FROM orders
         WHERE id IN (${placeholders})`,
        orderIds
      );

      for (const row of participantsResult.rows) {
        impactedParticipants.add(row.participant_id);
      }
    }

    const impactedParticipantIds = [...impactedParticipants];
    const accountStates = await Promise.all(
      impactedParticipantIds.map((participantId) => this.getPlayerAccountState(participantId, sessionId))
    );

    for (const accountState of accountStates) {
      const participantId = accountState.participant.id;
      const relevantFills = orderResponse.fills.filter(
        (fill) =>
          accountState.orders.some((order) => order.id === fill.buyOrderId || order.id === fill.sellOrderId)
      );

      events.push(
        makeEvent({
          type: 'account.updated',
          sessionId,
          scope: 'PRIVATE',
          participantId,
          payload: accountState
        })
      );

      if (relevantFills.length > 0) {
        events.push(
          makeEvent({
            type: 'player.fill',
            sessionId,
            scope: 'PRIVATE',
            participantId,
            payload: {
              fills: relevantFills
            }
          })
        );
      }
    }

    const instructorsResult = await this.db.query(
      `SELECT id
       FROM participants
       WHERE session_id = $1
         AND role = 'INSTRUCTOR'`,
      [sessionId]
    );
    const leaderboard = await this.getLeaderboard(sessionId);

    for (const instructor of instructorsResult.rows) {
      events.push(
        makeEvent({
          type: 'leaderboard.updated',
          sessionId,
          scope: 'PRIVATE',
          participantId: instructor.id,
          payload: leaderboard
        })
      );
    }

    return events;
  }

  async #ensureMultiStockData() {
    const sessionsResult = await this.db.query(
      `SELECT *
       FROM market_sessions
       ORDER BY id ASC`
    );

    for (const session of sessionsResult.rows) {
      await this.db.withTransaction(async (tx) => {
        const configuredStocks = buildTradableStocks(session.reference_price_cents);
        const existingStocksResult = await tx.query(
          `SELECT *
           FROM session_stocks
           WHERE session_id = $1
           ORDER BY sort_order ASC, id ASC
           FOR UPDATE`,
          [session.id]
        );

        const existingByKey = new Map(existingStocksResult.rows.map((row) => [row.stock_key, row]));
        for (const configuredStock of configuredStocks) {
          if (existingByKey.has(configuredStock.stockKey)) {
            continue;
          }

          const insertedStock = await tx.query(
            `INSERT INTO session_stocks (
               session_id,
               stock_key,
               display_name,
               sort_order,
               reference_price_cents,
               initial_position_qty
             )
             VALUES ($1, $2, $3, $4, $5, $6)
             RETURNING *`,
            [
              session.id,
              configuredStock.stockKey,
              configuredStock.displayName,
              configuredStock.sortOrder,
              configuredStock.referencePriceCents,
              configuredStock.initialPositionQty
            ]
          );
          existingByKey.set(configuredStock.stockKey, insertedStock.rows[0]);
        }

        const stocks = [...existingByKey.values()].sort(
          (left, right) => left.sort_order - right.sort_order || left.id - right.id
        );
        const primaryStock = stocks[0];
        if (!primaryStock) {
          return;
        }

        await tx.query(
          `UPDATE session_cards
           SET session_stock_id = $2
           WHERE session_id = $1
             AND session_stock_id IS NULL`,
          [session.id, primaryStock.id]
        );
        await tx.query(
          `UPDATE orders
           SET session_stock_id = $2
           WHERE session_id = $1
             AND session_stock_id IS NULL`,
          [session.id, primaryStock.id]
        );
        await tx.query(
          `UPDATE fills
           SET session_stock_id = $2
           WHERE session_id = $1
             AND session_stock_id IS NULL`,
          [session.id, primaryStock.id]
        );
        await tx.query(
          `UPDATE private_peeks
           SET session_stock_id = $2
           WHERE session_id = $1
             AND session_stock_id IS NULL`,
          [session.id, primaryStock.id]
        );
        await tx.query(
          `UPDATE announcements
           SET session_stock_id = $2
           WHERE session_id = $1
             AND session_stock_id IS NULL
             AND announcement_type IN ('SAR', 'EPS')`,
          [session.id, primaryStock.id]
        );
        await tx.query(
          `UPDATE scheduled_public_info
           SET session_stock_id = $2,
               info_key = $3 || info_key
           WHERE session_id = $1
             AND session_stock_id IS NULL`,
          [session.id, primaryStock.id, `${primaryStock.stock_key}_`]
        );

        for (const stock of stocks) {
          const cardCountResult = await tx.query(
            `SELECT COUNT(*) AS count
             FROM session_cards
             WHERE session_stock_id = $1`,
            [stock.id]
          );
          if (Number(cardCountResult.rows[0]?.count || 0) === 0) {
            for (const card of createShuffledDeck()) {
              await tx.query(
                `INSERT INTO session_cards (
                   session_id,
                   session_stock_id,
                   deck_order,
                   rank,
                   suit,
                   color,
                   label,
                   base_value_cents,
                   contribution_cents,
                   state
                 )
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
                [
                  session.id,
                  stock.id,
                  card.deckOrder,
                  card.rank,
                  card.suit,
                  card.color,
                  card.label,
                  card.baseValueCents,
                  card.contributionCents,
                  card.state
                ]
              );
            }
          }

          const scheduledInfoResult = await tx.query(
            `SELECT info_type, sequence_no
             FROM scheduled_public_info
             WHERE session_stock_id = $1`,
            [stock.id]
          );
          const existingInfoKeys = new Set(
            scheduledInfoResult.rows.map((row) => `${row.info_type}:${row.sequence_no}`)
          );
          for (const scheduledItem of buildPublicInfoSchedule(session.total_duration_seconds || DEFAULT_GAME_DURATION_SECONDS)) {
            const mapKey = `${scheduledItem.infoType}:${scheduledItem.sequenceNo}`;
            if (existingInfoKeys.has(mapKey)) {
              continue;
            }

            await tx.query(
              `INSERT INTO scheduled_public_info (
                 session_id,
                 session_stock_id,
                 info_key,
                 info_type,
                 sequence_no,
                 scheduled_offset_seconds
               )
               VALUES ($1, $2, $3, $4, $5, $6)`,
              [
                session.id,
                stock.id,
                `${stock.stock_key}_${scheduledItem.infoKey}`,
                scheduledItem.infoType,
                scheduledItem.sequenceNo,
                scheduledItem.scheduledOffsetSeconds
              ]
            );
          }
        }

        const participantsResult = await tx.query(
          `SELECT
             p.id AS participant_id,
             a.position_qty,
             a.reserved_sell_qty
           FROM participants p
           INNER JOIN accounts a ON a.participant_id = p.id
           WHERE p.session_id = $1
           ORDER BY p.id ASC
           FOR UPDATE`,
          [session.id]
        );

        for (const participant of participantsResult.rows) {
          const holdingsResult = await tx.query(
            `SELECT session_stock_id
             FROM account_holdings
             WHERE participant_id = $1
             FOR UPDATE`,
            [participant.participant_id]
          );
          const existingHoldingIds = new Set(holdingsResult.rows.map((row) => row.session_stock_id));
          const isLegacyParticipant = existingHoldingIds.size === 0;

          for (const stock of stocks) {
            if (existingHoldingIds.has(stock.id)) {
              continue;
            }

            const initialPositionQty = isLegacyParticipant
              ? stock.id === primaryStock.id
                ? participant.position_qty
                : 0
              : 0;
            const reservedSellQty = isLegacyParticipant && stock.id === primaryStock.id ? participant.reserved_sell_qty : 0;

            await tx.query(
              `INSERT INTO account_holdings (
                 participant_id,
                 session_stock_id,
                 position_qty,
                 reserved_sell_qty
               )
               VALUES ($1, $2, $3, $4)`,
              [participant.participant_id, stock.id, initialPositionQty, reservedSellQty]
            );
          }
        }

        if (session.liquidation_revealed_at) {
          const liquidationValues = await this.#getCurrentLiquidationValuesByStockTx(tx, session.id);
          for (const [sessionStockId, valueCents] of liquidationValues.entries()) {
            await tx.query(
              `UPDATE session_stocks
               SET liquidation_value_cents = $2,
                   updated_at = NOW()
               WHERE id = $1`,
              [sessionStockId, valueCents]
            );
          }
        }
      });
    }
  }

  async close() {
    await this.engine.close();
  }

  async #getSessionStocks(sessionId) {
    const result = await this.db.query(
      `SELECT *
       FROM session_stocks
       WHERE session_id = $1
       ORDER BY sort_order ASC, id ASC`,
      [sessionId]
    );

    return result.rows;
  }

  async #getParticipantHoldings(participantId, sessionId) {
    const result = await this.db.query(
      `SELECT ah.*, ss.stock_key, ss.display_name
       FROM account_holdings ah
       INNER JOIN session_stocks ss ON ss.id = ah.session_stock_id
       INNER JOIN participants p ON p.id = ah.participant_id
       WHERE ah.participant_id = $1
         AND p.session_id = $2
       ORDER BY ss.sort_order ASC, ah.session_stock_id ASC`,
      [participantId, sessionId]
    );

    return result.rows.map((row) => formatHolding(row));
  }

  async #getStudentHoldings(sessionId) {
    const result = await this.db.query(
      `SELECT ah.*, ss.stock_key, ss.display_name
       FROM account_holdings ah
       INNER JOIN participants p ON p.id = ah.participant_id
       INNER JOIN session_stocks ss ON ss.id = ah.session_stock_id
       WHERE p.session_id = $1
         AND p.role = 'STUDENT'
       ORDER BY p.id ASC, ss.sort_order ASC, ah.session_stock_id ASC`,
      [sessionId]
    );

    return result.rows.map((row) => formatHolding(row));
  }

  async #resolveRequestedStock(sessionId, sessionStockId = null) {
    const stocks = await this.#getSessionStocks(sessionId);
    if (stocks.length === 0) {
      throw new HttpError(404, 'INVALID_STOCK_ID', '이 세션에 거래 가능한 종목이 없습니다.');
    }

    if (Number.isInteger(sessionStockId)) {
      const stock = stocks.find((entry) => entry.id === sessionStockId);
      if (!stock) {
        throw new HttpError(404, 'INVALID_STOCK_ID', '선택한 종목을 찾을 수 없습니다.');
      }

      return stock;
    }

    return stocks[0];
  }

  async #getStockOrderBookSnapshot(session, stock) {
    const [bidsResult, asksResult, recentTrades, market] = await Promise.all([
      this.db.query(
        `SELECT
           limit_price_cents AS price_cents,
           SUM(remaining_qty) AS total_qty,
           COUNT(*) AS order_count
         FROM orders
         WHERE session_id = $1
           AND session_stock_id = $2
           AND side = 'BUY'
           AND order_type = 'LIMIT'
           AND status IN ('OPEN', 'PARTIALLY_FILLED')
         GROUP BY limit_price_cents
         ORDER BY limit_price_cents DESC
         LIMIT $3`,
        [session.id, stock.id, ORDER_BOOK_VISIBLE_LEVELS]
      ),
      this.db.query(
        `SELECT
           limit_price_cents AS price_cents,
           SUM(remaining_qty) AS total_qty,
           COUNT(*) AS order_count
         FROM orders
         WHERE session_id = $1
           AND session_stock_id = $2
           AND side = 'SELL'
           AND order_type = 'LIMIT'
           AND status IN ('OPEN', 'PARTIALLY_FILLED')
         GROUP BY limit_price_cents
         ORDER BY limit_price_cents ASC
         LIMIT $3`,
        [session.id, stock.id, ORDER_BOOK_VISIBLE_LEVELS]
      ),
      this.#getRecentFills(session.id, this.config.RECENT_TRADES_LIMIT || 20, stock.id),
      this.#getMarketStats(session, stock)
    ]);

    return {
      stock: formatSessionStock(stock),
      market,
      bids: bidsResult.rows.map((row) => ({
        priceCents: row.price_cents,
        totalQty: Number(row.total_qty),
        orderCount: Number(row.order_count)
      })),
      asks: asksResult.rows.map((row) => ({
        priceCents: row.price_cents,
        totalQty: Number(row.total_qty),
        orderCount: Number(row.order_count)
      })),
      recentTrades: recentTrades.map((row) => formatFill(row))
    };
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
      throw new HttpError(404, 'SESSION_NOT_FOUND', 'The game session does not exist.');
    }

    return session;
  }

  async #loadParticipantForUpdate(tx, participantId, sessionId) {
    const result = await tx.query(
      `SELECT *
       FROM participants
       WHERE id = $1
         AND session_id = $2
       FOR UPDATE`,
      [participantId, sessionId]
    );

    const participant = result.rows[0];
    if (!participant) {
      throw new HttpError(404, 'PARTICIPANT_NOT_FOUND', 'The participant does not exist.');
    }

    return participant;
  }

  async #loadSessionStockForUpdateTx(tx, sessionStockId, sessionId) {
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
      throw new HttpError(404, 'INVALID_STOCK_ID', '선택한 종목을 찾을 수 없습니다.');
    }

    return sessionStock;
  }

  async #getCurrentLiquidationValuesByStockTx(tx, sessionId) {
    const result = await tx.query(
      `SELECT session_stock_id, COALESCE(SUM(contribution_cents), 0) AS liquidation_value_cents
       FROM session_cards
       WHERE session_id = $1
         AND state = 'ACTIVE'
       GROUP BY session_stock_id`,
      [sessionId]
    );

    return new Map(
      result.rows.map((row) => [Number(row.session_stock_id), Number(row.liquidation_value_cents || 0)])
    );
  }

  #computeClosedFormElapsedSeconds(session, now = new Date()) {
    return computeElapsedSeconds(session, now);
  }

  async #loadSession(sessionId) {
    const result = await this.db.query(
      `SELECT *
       FROM market_sessions
       WHERE id = $1`,
      [sessionId]
    );

    const session = result.rows[0];
    if (!session) {
      throw new HttpError(404, 'SESSION_NOT_FOUND', 'The game session does not exist.');
    }

    return session;
  }

  async #loadParticipant(participantId, sessionId) {
    const result = await this.db.query(
      `SELECT *
       FROM participants
       WHERE id = $1
         AND session_id = $2`,
      [participantId, sessionId]
    );

    const participant = result.rows[0];
    if (!participant) {
      throw new HttpError(404, 'PARTICIPANT_NOT_FOUND', 'The participant does not exist.');
    }

    return participant;
  }

}

function translateError(error) {
  if (error instanceof HttpError) {
    return error;
  }

  if (error instanceof EngineError) {
    const statusByCode = {
      INVALID_SESSION_ID: 400,
      INVALID_STOCK_ID: 400,
      INVALID_PARTICIPANT_ID: 400,
      INVALID_IDEMPOTENCY_KEY: 400,
      INVALID_SIDE: 400,
      INVALID_ORDER_TYPE: 400,
      INVALID_QUANTITY: 400,
      INVALID_LIMIT_PRICE: 400,
      INVALID_ROLE: 400,
      SESSION_NOT_FOUND: 404,
      SESSION_NOT_OPEN: 409,
      ACCOUNT_NOT_FOUND: 404,
      ORDER_NOT_FOUND: 404,
      ORDER_OWNERSHIP_ERROR: 403,
      IDEMPOTENCY_KEY_REUSED_WITH_DIFFERENT_PAYLOAD: 409,
      BORROW_LIMIT_BREACH: 409,
      SHORT_LIMIT_BREACH: 409,
      NEGATIVE_RESERVATION: 500
    };

    return new HttpError(statusByCode[error.code] || 400, error.code, error.message, error.details);
  }

  if (error && error.code === '23505') {
    if (error.constraint === 'market_sessions_single_active_idx') {
      return new HttpError(409, 'ACTIVE_SESSION_ALREADY_EXISTS', 'Only one active classroom session is allowed at a time.');
    }

    if (error.constraint === 'market_sessions_join_code_key') {
      return new HttpError(409, 'JOIN_CODE_ALREADY_EXISTS', 'Generated join code collided. Please retry.');
    }

    return new HttpError(409, 'UNIQUE_CONSTRAINT_VIOLATION', 'A duplicate resource already exists.', {
      constraint: error.constraint
    });
  }

  return new HttpError(500, 'INTERNAL_SERVER_ERROR', 'An unexpected server error occurred.');
}

module.exports = {
  GameService,
  HttpError,
  translateError,
  makeEvent
};
