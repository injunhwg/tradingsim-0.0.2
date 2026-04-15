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

function formatAccount(row) {
  if (!row) {
    return null;
  }

  return {
    participantId: row.participant_id,
    cashCents: row.cash_cents,
    positionQty: row.position_qty,
    reservedBuyCents: row.reserved_buy_cents,
    reservedSellQty: row.reserved_sell_qty,
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
            session: payload.session
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
      participant: formatParticipant(participantBundle.participant),
      account: formatAccount(participantBundle.account)
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
    const [bidsResult, asksResult, recentTrades, announcements, stats, liquidation] = await Promise.all([
      this.db.query(
        `SELECT
           limit_price_cents AS price_cents,
           SUM(remaining_qty) AS total_qty,
           COUNT(*) AS order_count
         FROM orders
         WHERE session_id = $1
           AND side = 'BUY'
           AND order_type = 'LIMIT'
           AND status IN ('OPEN', 'PARTIALLY_FILLED')
         GROUP BY limit_price_cents
         ORDER BY limit_price_cents DESC
         LIMIT $2`,
        [sessionId, ORDER_BOOK_VISIBLE_LEVELS]
      ),
      this.db.query(
        `SELECT
           limit_price_cents AS price_cents,
           SUM(remaining_qty) AS total_qty,
           COUNT(*) AS order_count
         FROM orders
         WHERE session_id = $1
           AND side = 'SELL'
           AND order_type = 'LIMIT'
           AND status IN ('OPEN', 'PARTIALLY_FILLED')
         GROUP BY limit_price_cents
         ORDER BY limit_price_cents ASC
         LIMIT $2`,
        [sessionId, ORDER_BOOK_VISIBLE_LEVELS]
      ),
      this.#getRecentFills(sessionId, this.config.RECENT_TRADES_LIMIT || 20),
      this.#getAnnouncements(sessionId, this.config.ANNOUNCEMENT_HISTORY_LIMIT || 20),
      this.#getMarketStats(session),
      this.#getLiquidationSummary(session)
    ]);

    return {
      session: formatSession(session),
      market: stats,
      liquidation,
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
      recentTrades: recentTrades.map((row) => formatFill(row)),
      announcements: announcements.map((row) => formatAnnouncement(row))
    };
  }

  async getPlayerAccountState(participantId, sessionId) {
    const participant = await this.#loadParticipant(participantId, sessionId);
    const account = await this.engine.getAccount(participantId);
    const [ordersResult, fillsResult, peeksResult] = await Promise.all([
      this.db.query(
        `SELECT *
         FROM orders
         WHERE participant_id = $1
         ORDER BY id DESC`,
        [participantId]
      ),
      this.db.query(
        `SELECT
           f.*,
           buy_orders.participant_id AS buy_participant_id,
           sell_orders.participant_id AS sell_participant_id
         FROM fills f
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
           sc.contribution_cents
         FROM private_peeks pp
         LEFT JOIN session_cards sc ON sc.id = pp.card_id
         WHERE pp.participant_id = $1
         ORDER BY pp.id DESC
         LIMIT 3`,
        [participantId]
      )
    ]);

    return {
      participant: formatParticipant(participant),
      account: formatAccount(account),
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
    const marketStats = await this.#getMarketStats(session);
    const liquidationValueCents = session.liquidation_revealed_at ? session.liquidation_value_cents : null;
    const markPriceCents = liquidationValueCents ?? marketStats.lastTradePriceCents ?? session.reference_price_cents;
    const initialPortfolioValueCents = STARTING_CASH_CENTS + STARTING_POSITION_QTY * session.reference_price_cents;

    const result = await this.db.query(
      `SELECT
         p.id AS participant_id,
         p.display_name,
         a.cash_cents,
         a.position_qty
       FROM participants p
       INNER JOIN accounts a ON a.participant_id = p.id
       WHERE p.session_id = $1
         AND p.role = 'STUDENT'
       ORDER BY p.id ASC`,
      [sessionId]
    );

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
      positionQty: row.position_qty,
      markedPriceCents: markPriceCents,
      portfolioValueCents:
        (session.liquidation_revealed_at
          ? settleCashCents(row.cash_cents, this.config.BORROW_INTEREST_BPS)
          : row.cash_cents) +
        row.position_qty * markPriceCents,
      returnBps: Math.round(
        (((session.liquidation_revealed_at
          ? settleCashCents(row.cash_cents, this.config.BORROW_INTEREST_BPS)
          : row.cash_cents) +
          row.position_qty * markPriceCents -
          initialPortfolioValueCents) *
          10000) /
          initialPortfolioValueCents
      )
    }));

    rows.sort((left, right) => right.portfolioValueCents - left.portfolioValueCents || left.displayName.localeCompare(right.displayName));

    return {
      session: formatSession(session),
      market: marketStats,
      liquidation: await this.#getLiquidationSummary(session),
      leaderboard: rows.map((row, index) => ({
        ...row,
        rank: index + 1
      }))
    };
  }

  async submitOrder({ sessionId, participantId, idempotencyKey, side, orderType, quantity, limitPriceCents }) {
    const response = await this.engine.submitOrder({
      sessionId,
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

  async purchasePeek({ sessionId, participantId }) {
    const peekPriceCents = this.config.PEEK_PRICE_CENTS || DEFAULT_PEEK_PRICE_CENTS;
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

        const activeCardsResult = await tx.query(
          `SELECT *
           FROM session_cards
           WHERE session_id = $1
             AND state = 'ACTIVE'
           ORDER BY id ASC
           FOR UPDATE`,
          [sessionId]
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
          `INSERT INTO private_peeks (session_id, participant_id, card_id, price_cents, payload_json)
           VALUES ($1, $2, NULL, $3, $4::jsonb)
           RETURNING *`,
          [sessionId, participantId, peekPriceCents, JSON.stringify(payloadJson)]
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
      account: formatAccount(peekResult.account),
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
          session: formatSession(session)
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
    const connectedSet = new Set(connectedParticipantIds);
    const result = await this.db.query(
      `SELECT
         p.id,
         p.display_name,
         p.created_at,
         a.cash_cents,
         a.position_qty,
         a.reserved_buy_cents,
         a.reserved_sell_qty,
         COUNT(o.id) FILTER (WHERE o.status IN ('OPEN', 'PARTIALLY_FILLED')) AS open_order_count
       FROM participants p
       INNER JOIN accounts a ON a.participant_id = p.id
       LEFT JOIN orders o ON o.participant_id = p.id
       WHERE p.session_id = $1
         AND p.role = 'STUDENT'
       GROUP BY p.id, p.display_name, p.created_at, a.cash_cents, a.position_qty, a.reserved_buy_cents, a.reserved_sell_qty
       ORDER BY p.display_name ASC`,
      [sessionId]
    );

    return {
      session: formatSession(session),
      students: result.rows.map((row) => ({
        participantId: row.id,
        displayName: row.display_name,
        createdAt: row.created_at,
        cashCents: row.cash_cents,
        positionQty: row.position_qty,
        reservedBuyCents: row.reserved_buy_cents,
        reservedSellQty: row.reserved_sell_qty,
        openOrderCount: Number(row.open_order_count || 0),
        connected: connectedSet.has(row.id)
      }))
    };
  }

  async getInstructorDashboard(sessionId, connectedParticipantIds = []) {
    const [session, orderBook, leaderboard, students, publicInfoSchedule, liquidationComposition] = await Promise.all([
      this.getSessionState(sessionId),
      this.getOrderBookSnapshot(sessionId),
      this.getLeaderboard(sessionId),
      this.getStudents(sessionId, connectedParticipantIds),
      this.#getScheduledPublicInfo(sessionId),
      this.#getInstructorLiquidationComposition(sessionId)
    ]);

    return {
      session: session.session,
      market: orderBook.market,
      liquidation: orderBook.liquidation,
      liquidationComposition,
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

      const liquidationValueCents = await this.#getCurrentLiquidationValueCentsTx(tx, sessionId);
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

    const leaderboard = await this.getLeaderboard(sessionId);
    return {
      session: formatSession(session),
      leaderboard,
      events: [
        makeEvent({
          type: 'game.state',
          sessionId,
          payload: {
            session: formatSession(session)
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
    return {
      session: formatSession(session)
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
          session: snapshot.session
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
    return {
      participant: formatParticipant(participant)
    };
  }

  async getAuthContext(authToken) {
    const principal = await this.authenticateParticipant(authToken);
    return {
      principal
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

      const instructor = await this.#insertParticipantWithAccount(tx, {
        sessionId: session.id,
        externalId: `instructor-${randomToken()}`,
        displayName: 'Instructor',
        role: 'INSTRUCTOR',
        authToken: randomToken()
      });

      for (const card of createShuffledDeck()) {
        await tx.query(
          `INSERT INTO session_cards (
             session_id,
             deck_order,
             rank,
             suit,
             color,
             label,
             base_value_cents,
             contribution_cents,
             state
           )
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
          [
            session.id,
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
             info_key,
             info_type,
             sequence_no,
             scheduled_offset_seconds
           )
           VALUES ($1, $2, $3, $4, $5)`,
          [
            session.id,
            scheduledItem.infoKey,
            scheduledItem.infoType,
            scheduledItem.sequenceNo,
            scheduledItem.scheduledOffsetSeconds
          ]
        );
      }

      return {
        session,
        instructor
      };
    });

    return {
      session: formatSession(bundle.session),
      instructor: {
        participant: formatParticipant(bundle.instructor.participant),
        account: formatAccount(bundle.instructor.account)
      }
    };
  }

  async #insertParticipantWithAccount(tx, { sessionId, externalId, displayName, role, authToken }) {
    const participantResult = await tx.query(
      `INSERT INTO participants (session_id, external_id, display_name, role, auth_token)
       VALUES ($1, $2, $3, $4, $5)
       RETURNING *`,
      [sessionId, externalId, displayName, role, authToken]
    );
    const participant = participantResult.rows[0];

    const accountResult = await tx.query(
      `INSERT INTO accounts (participant_id, cash_cents, position_qty)
       VALUES ($1, $2, $3)
       RETURNING *`,
      [participant.id, STARTING_CASH_CENTS, STARTING_POSITION_QTY]
    );

    return {
      participant,
      account: accountResult.rows[0]
    };
  }

  async #getScheduledPublicInfo(sessionId) {
    const result = await this.db.query(
      `SELECT *
       FROM scheduled_public_info
       WHERE session_id = $1
       ORDER BY scheduled_offset_seconds ASC, id ASC`,
      [sessionId]
    );

    return result.rows.map((row) => formatScheduledPublicInfo(row));
  }

  async #getLiquidationSummary(session) {
    if (!session?.liquidation_revealed_at) {
      return {
        revealed: false,
        valueCents: null,
        revealedAt: null,
        cards: []
      };
    }

    const cardsResult = await this.db.query(
      `SELECT *
       FROM session_cards
       WHERE session_id = $1
         AND state = 'ACTIVE'
       ORDER BY deck_order ASC`,
      [session.id]
    );

    return {
      revealed: true,
      valueCents: session.liquidation_value_cents,
      revealedAt: session.liquidation_revealed_at,
      cards: cardsResult.rows.map((row) => formatCard(row))
    };
  }

  async #getInstructorLiquidationComposition(sessionId) {
    const cardsResult = await this.db.query(
      `SELECT *
       FROM session_cards
       WHERE session_id = $1
         AND state = 'ACTIVE'
       ORDER BY deck_order ASC`,
      [sessionId]
    );
    const cards = cardsResult.rows.map((row) => formatCard(row));

    return {
      currentValueCents: sumCardContributionCents(cards),
      cardCount: cards.length,
      cards
    };
  }

  async #processDuePublicInfo() {
    const now = new Date();
    const releasedAnnouncements = await this.db.withTransaction(async (tx) => {
      const dueResult = await tx.query(
        `SELECT
           spi.*,
           ms.started_at,
           ms.opened_at,
           ms.elapsed_open_seconds
         FROM scheduled_public_info spi
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
             actor_participant_id,
             message,
             announcement_type,
             payload_json
           )
           VALUES ($1, NULL, $2, $3, $4::jsonb)
           RETURNING *`,
          [row.session_id, release.message, row.info_type, JSON.stringify(release.publicPayload)]
        );
        const announcement = announcementResult.rows[0];

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

        const liquidationValueCents = await this.#getCurrentLiquidationValueCentsTx(tx, session.id);
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
      const leaderboard = await this.getLeaderboard(session.id);
      events.push(
        makeEvent({
          type: 'game.state',
          sessionId: session.id,
          payload: {
            session: formatSession(session)
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
         AND state = 'ACTIVE'
       ORDER BY id ASC
       FOR UPDATE`,
      [scheduledInfo.session_id]
    );
    const chosenCards = sampleWithoutReplacement(activeCardsResult.rows, 5);
    const fiveCardSumCents = chosenCards.reduce((sum, card) => sum + card.contribution_cents, 0);
    const reportTotalCents = fiveCardSumCents + SAR_BONUS_CENTS;

    return {
      message: formatSarMessage(scheduledInfo.sequence_no, reportTotalCents),
      publicPayload: {
        infoType: 'SAR',
        sequenceNo: scheduledInfo.sequence_no,
        reportTotalCents
      },
      auditPayload: {
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
         AND state = 'ACTIVE'
       ORDER BY id ASC
       FOR UPDATE`,
      [scheduledInfo.session_id]
    );
    const deckCardResult = await tx.query(
      `SELECT *
       FROM session_cards
       WHERE session_id = $1
         AND state = 'DECK'
       ORDER BY deck_order ASC
       LIMIT 1
       FOR UPDATE`,
      [scheduledInfo.session_id]
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
      message: formatEpsMessage(scheduledInfo.sequence_no, deltaCents),
      publicPayload: {
        infoType: 'EPS',
        sequenceNo: scheduledInfo.sequence_no,
        deltaCents
      },
      auditPayload: {
        infoType: 'EPS',
        sequenceNo: scheduledInfo.sequence_no,
        deltaCents,
        removedCardId: removedCard.id,
        addedCardId: addedCard.id
      }
    };
  }

  async #getRecentFills(sessionId, limit) {
    const result = await this.db.query(
      `SELECT *
       FROM fills
       WHERE session_id = $1
       ORDER BY id DESC
       LIMIT $2`,
      [sessionId, limit]
    );

    return result.rows.reverse();
  }

  async #getAnnouncements(sessionId, limit) {
    const result = await this.db.query(
      `SELECT *
       FROM announcements
       WHERE session_id = $1
       ORDER BY id DESC
       LIMIT $2`,
      [sessionId, limit]
    );

    return result.rows.reverse();
  }

  async #getMarketStats(session) {
    const [lastFillResult, volumeResult] = await Promise.all([
      this.db.query(
        `SELECT price_cents
         FROM fills
         WHERE session_id = $1
         ORDER BY id DESC
         LIMIT 1`,
        [session.id]
      ),
      this.db.query(
        `SELECT COALESCE(SUM(qty), 0) AS traded_qty
         FROM fills
         WHERE session_id = $1`,
        [session.id]
      )
    ]);

    const lastTradePriceCents = lastFillResult.rows[0]?.price_cents ?? session.reference_price_cents;
    const markPriceCents = session.liquidation_revealed_at ? session.liquidation_value_cents : lastTradePriceCents;
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
      market: snapshot.market
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

  async close() {
    await this.engine.close();
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

  async #getCurrentLiquidationValueCentsTx(tx, sessionId) {
    const result = await tx.query(
      `SELECT COALESCE(SUM(contribution_cents), 0) AS liquidation_value_cents
       FROM session_cards
       WHERE session_id = $1
         AND state = 'ACTIVE'`,
      [sessionId]
    );

    return Number(result.rows[0]?.liquidation_value_cents || 0);
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
