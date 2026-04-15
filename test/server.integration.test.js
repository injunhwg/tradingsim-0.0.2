const test = require('node:test');
const assert = require('node:assert/strict');
const { once } = require('node:events');

const { WebSocket } = require('ws');

const { createPGliteDatabase, createRuntime } = require('../src');

async function createServerHarness(configOverrides = {}) {
  const db = await createPGliteDatabase();
  const runtime = await createRuntime({
    db,
    config: {
      HOST: '127.0.0.1',
      PORT: 0,
      BOOTSTRAP_ADMIN_SECRET: 'bootstrap-secret',
      DEFAULT_REFERENCE_PRICE_CENTS: 1000,
      RECENT_TRADES_LIMIT: 20,
      ANNOUNCEMENT_HISTORY_LIMIT: 20,
      ANNOUNCEMENT_POLL_INTERVAL_MS: 25,
      ...configOverrides
    }
  });

  const address = await runtime.start();
  const baseUrl = `http://127.0.0.1:${address.port}`;
  const wsBaseUrl = `ws://127.0.0.1:${address.port}/ws`;

  return {
    runtime,
    baseUrl,
    wsBaseUrl,
    async close() {
      await runtime.stop();
    }
  };
}

async function requestJson(url, options = {}) {
  const response = await fetch(url, options);
  const body = await response.json();
  return { response, body };
}

async function joinStudent(baseUrl, joinCode, displayName) {
  return requestJson(`${baseUrl}/api/sessions/join`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({
      joinCode,
      displayName
    })
  });
}

function authHeaders(token, extra = {}) {
  return {
    authorization: `Bearer ${token}`,
    'content-type': 'application/json',
    ...extra
  };
}

function createEventCollector(socket) {
  const queue = [];
  const waiters = [];

  socket.on('message', (raw) => {
    const event = JSON.parse(raw.toString());
    const waiterIndex = waiters.findIndex((waiter) => waiter.predicate(event));

    if (waiterIndex >= 0) {
      const [waiter] = waiters.splice(waiterIndex, 1);
      waiter.resolve(event);
      return;
    }

    queue.push(event);
  });

  return {
    async waitFor(predicate, timeoutMs = 4000) {
      const queued = queue.findIndex((event) => predicate(event));
      if (queued >= 0) {
        const [event] = queue.splice(queued, 1);
        return event;
      }

      return new Promise((resolve, reject) => {
        const timer = setTimeout(() => {
          const waiterIndex = waiters.findIndex((waiter) => waiter.resolve === resolve);
          if (waiterIndex >= 0) {
            waiters.splice(waiterIndex, 1);
          }

          reject(
            new Error(
              `Timed out waiting for websocket event. Buffered events: ${queue.map((event) => event.type).join(', ')}`
            )
          );
        }, timeoutMs);

        waiters.push({
          predicate,
          resolve: (event) => {
            clearTimeout(timer);
            resolve(event);
          }
        });
      });
    }
  };
}

test('REST API supports a full classroom trading flow', async () => {
  const harness = await createServerHarness();

  try {
    const studentPage = await fetch(`${harness.baseUrl}/student`);
    const instructorPage = await fetch(`${harness.baseUrl}/instructor`);
    assert.equal(studentPage.status, 200);
    assert.equal(instructorPage.status, 200);
    assert.match(await studentPage.text(), /교실 시장에서 거래하세요/);
    assert.match(await instructorPage.text(), /교실 시장을 운영하세요/);

    const createResult = await requestJson(`${harness.baseUrl}/api/sessions`, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'x-admin-secret': 'bootstrap-secret'
      },
      body: JSON.stringify({
        sessionName: 'Investments 101',
        referencePriceCents: 1000
      })
    });

    assert.equal(createResult.response.status, 201);
    assert.equal(createResult.body.session.status, 'PAUSED');
    assert.equal(createResult.body.session.totalDurationSeconds, 900);

    const instructorToken = createResult.body.instructor.participant.authToken;
    const joinCode = createResult.body.session.joinCode;
    const sessionId = createResult.body.session.id;

    const sellerJoin = await requestJson(`${harness.baseUrl}/api/sessions/join`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        joinCode,
        displayName: 'Seller'
      })
    });

    const buyerJoin = await requestJson(`${harness.baseUrl}/api/sessions/join`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        joinCode,
        displayName: 'Buyer'
      })
    });

    assert.equal(sellerJoin.response.status, 201);
    assert.equal(buyerJoin.response.status, 201);

    const sellerToken = sellerJoin.body.participant.authToken;
    const buyerToken = buyerJoin.body.participant.authToken;
    const dashboardResult = await requestJson(`${harness.baseUrl}/api/instructor/dashboard`, {
      headers: {
        authorization: `Bearer ${instructorToken}`
      }
    });

    assert.equal(dashboardResult.response.status, 200);
    assert.equal(dashboardResult.body.students.length, 2);

    const openResult = await requestJson(`${harness.baseUrl}/api/sessions/${sessionId}/state`, {
      method: 'PATCH',
      headers: authHeaders(instructorToken),
      body: JSON.stringify({ status: 'OPEN' })
    });

    assert.equal(openResult.response.status, 200);
    assert.equal(openResult.body.session.status, 'OPEN');

    const sellOrder = await requestJson(`${harness.baseUrl}/api/orders`, {
      method: 'POST',
      headers: authHeaders(sellerToken, { 'idempotency-key': 'sell-1' }),
      body: JSON.stringify({
        side: 'SELL',
        orderType: 'LIMIT',
        quantity: 2,
        limitPriceCents: 1000
      })
    });

    assert.equal(sellOrder.response.status, 200);
    assert.equal(sellOrder.body.order.status, 'OPEN');

    const buyOrder = await requestJson(`${harness.baseUrl}/api/orders`, {
      method: 'POST',
      headers: authHeaders(buyerToken, { 'idempotency-key': 'buy-1' }),
      body: JSON.stringify({
        side: 'BUY',
        orderType: 'MARKET',
        quantity: 1
      })
    });

    assert.equal(buyOrder.response.status, 200);
    assert.equal(buyOrder.body.fills.length, 1);
    assert.equal(buyOrder.body.fills[0].priceCents, 1000);

    const accountResult = await requestJson(`${harness.baseUrl}/api/me/account`, {
      headers: {
        authorization: `Bearer ${buyerToken}`
      }
    });

    assert.equal(accountResult.response.status, 200);
    assert.equal(accountResult.body.account.positionQty, 6);
    assert.equal(accountResult.body.account.cashCents, 19000);

    const bookResult = await requestJson(`${harness.baseUrl}/api/order-book`, {
      headers: {
        authorization: `Bearer ${buyerToken}`
      }
    });

    assert.equal(bookResult.response.status, 200);
    assert.equal(bookResult.body.asks.length, 1);
    assert.equal(bookResult.body.asks[0].totalQty, 1);
    assert.equal(bookResult.body.market.lastTradePriceCents, 1000);

    const leaderboardResult = await requestJson(`${harness.baseUrl}/api/leaderboard`, {
      headers: {
        authorization: `Bearer ${buyerToken}`
      }
    });

    assert.equal(leaderboardResult.response.status, 200);
    assert.equal(leaderboardResult.body.leaderboard.length, 2);

    const cancelResult = await requestJson(
      `${harness.baseUrl}/api/orders/${sellOrder.body.order.id}/cancel`,
      {
        method: 'POST',
        headers: {
          authorization: `Bearer ${sellerToken}`
        }
      }
    );

    assert.equal(cancelResult.response.status, 200);
    assert.equal(cancelResult.body.order.status, 'CANCELLED');
  } finally {
    await harness.close();
  }
});

test('instructor reset preserves the automatic public info schedule and removes manual announcement routes', async () => {
  const harness = await createServerHarness();

  try {
    const createResult = await requestJson(`${harness.baseUrl}/api/sessions`, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'x-admin-secret': 'bootstrap-secret'
      },
      body: JSON.stringify({
        sessionName: 'First Session',
        referencePriceCents: 1000
      })
    });

    const firstSessionId = createResult.body.session.id;

    const resetResult = await requestJson(`${harness.baseUrl}/api/sessions/reset`, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'x-admin-secret': 'bootstrap-secret'
      },
      body: JSON.stringify({
        sessionName: 'Second Session',
        referencePriceCents: 1250
      })
    });

    assert.equal(resetResult.response.status, 201);
    assert.notEqual(resetResult.body.session.id, firstSessionId);
    assert.equal(resetResult.body.session.status, 'PAUSED');

    const instructorToken = resetResult.body.instructor.participant.authToken;
    const sessionId = resetResult.body.session.id;

    await requestJson(`${harness.baseUrl}/api/sessions/${sessionId}/state`, {
      method: 'PATCH',
      headers: authHeaders(instructorToken),
      body: JSON.stringify({ status: 'OPEN' })
    });

    const dashboard = await requestJson(`${harness.baseUrl}/api/instructor/dashboard`, {
      headers: {
        authorization: `Bearer ${instructorToken}`
      }
    });

    assert.equal(dashboard.response.status, 200);
    assert.equal(dashboard.body.publicInfoSchedule.length, 6);
    assert.ok(dashboard.body.publicInfoSchedule.every((item) => ['SAR', 'EPS'].includes(item.infoType)));

    const manualAnnouncementAttempt = await requestJson(`${harness.baseUrl}/api/sessions/${sessionId}/announcements`, {
      method: 'POST',
      headers: authHeaders(instructorToken),
      body: JSON.stringify({
        message: 'Manual announcement'
      })
    });

    assert.equal(manualAnnouncementAttempt.response.status, 404);
  } finally {
    await harness.close();
  }
});

test('automatic public info schedule scales with the configured game duration', async () => {
  const harness = await createServerHarness({ GAME_DURATION_SECONDS: 600 });

  try {
    const createResult = await requestJson(`${harness.baseUrl}/api/sessions`, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'x-admin-secret': 'bootstrap-secret'
      },
      body: JSON.stringify({
        sessionName: 'Short Session',
        referencePriceCents: 1000
      })
    });

    const instructorToken = createResult.body.instructor.participant.authToken;
    const dashboard = await requestJson(`${harness.baseUrl}/api/instructor/dashboard`, {
      headers: {
        authorization: `Bearer ${instructorToken}`
      }
    });

    assert.equal(dashboard.response.status, 200);
    assert.deepEqual(
      dashboard.body.publicInfoSchedule.map((item) => [item.infoType, item.sequenceNo, item.scheduledOffsetSeconds]),
      [
        ['SAR', 1, 0],
        ['EPS', 1, 120],
        ['EPS', 2, 240],
        ['SAR', 2, 300],
        ['EPS', 3, 360],
        ['EPS', 4, 480]
      ]
    );
  } finally {
    await harness.close();
  }
});

test('order book snapshot keeps the inside market plus the next five price levels per side', async () => {
  const harness = await createServerHarness();

  try {
    const createResult = await requestJson(`${harness.baseUrl}/api/sessions`, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'x-admin-secret': 'bootstrap-secret'
      },
      body: JSON.stringify({
        sessionName: 'Depth Window',
        referencePriceCents: 1000
      })
    });

    const instructorToken = createResult.body.instructor.participant.authToken;
    const joinCode = createResult.body.session.joinCode;
    const sessionId = createResult.body.session.id;

    await requestJson(`${harness.baseUrl}/api/sessions/${sessionId}/state`, {
      method: 'PATCH',
      headers: authHeaders(instructorToken),
      body: JSON.stringify({ status: 'OPEN' })
    });

    const bidPrices = [990, 980, 970, 960, 950, 940, 930];
    const askPrices = [1010, 1020, 1030, 1040, 1050, 1060, 1070];

    for (const [index, priceCents] of bidPrices.entries()) {
      const joinResult = await joinStudent(harness.baseUrl, joinCode, `Buyer ${index + 1}`);
      const token = joinResult.body.participant.authToken;

      await requestJson(`${harness.baseUrl}/api/orders`, {
        method: 'POST',
        headers: authHeaders(token, { 'idempotency-key': `bid-${index + 1}` }),
        body: JSON.stringify({
          side: 'BUY',
          orderType: 'LIMIT',
          quantity: 1,
          limitPriceCents: priceCents
        })
      });
    }

    for (const [index, priceCents] of askPrices.entries()) {
      const joinResult = await joinStudent(harness.baseUrl, joinCode, `Seller ${index + 1}`);
      const token = joinResult.body.participant.authToken;

      await requestJson(`${harness.baseUrl}/api/orders`, {
        method: 'POST',
        headers: authHeaders(token, { 'idempotency-key': `ask-${index + 1}` }),
        body: JSON.stringify({
          side: 'SELL',
          orderType: 'LIMIT',
          quantity: 1,
          limitPriceCents: priceCents
        })
      });
    }

    const snapshot = await requestJson(`${harness.baseUrl}/api/order-book`, {
      headers: {
        authorization: `Bearer ${instructorToken}`
      }
    });

    assert.equal(snapshot.response.status, 200);
    assert.deepEqual(snapshot.body.bids.map((level) => level.priceCents), [990, 980, 970, 960, 950, 940]);
    assert.deepEqual(snapshot.body.asks.map((level) => level.priceCents), [1010, 1020, 1030, 1040, 1050, 1060]);
    assert.equal(snapshot.body.bids[0].totalQty, 1);
    assert.equal(snapshot.body.bids[0].orderCount, 1);
    assert.equal(snapshot.body.asks[0].totalQty, 1);
    assert.equal(snapshot.body.asks[0].orderCount, 1);
  } finally {
    await harness.close();
  }
});

test('WebSocket broadcasts live game updates from committed database state', async () => {
  const harness = await createServerHarness();

  try {
    const createResult = await requestJson(`${harness.baseUrl}/api/sessions`, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'x-admin-secret': 'bootstrap-secret'
      },
      body: JSON.stringify({
        sessionName: 'Live Class',
        referencePriceCents: 1000
      })
    });

    const instructorToken = createResult.body.instructor.participant.authToken;
    const joinCode = createResult.body.session.joinCode;
    const sessionId = createResult.body.session.id;

    const sellerJoin = await requestJson(`${harness.baseUrl}/api/sessions/join`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        joinCode,
        displayName: 'Seller'
      })
    });
    const buyerJoin = await requestJson(`${harness.baseUrl}/api/sessions/join`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        joinCode,
        displayName: 'Buyer'
      })
    });

    const sellerToken = sellerJoin.body.participant.authToken;
    const buyerToken = buyerJoin.body.participant.authToken;

    await requestJson(`${harness.baseUrl}/api/sessions/${sessionId}/state`, {
      method: 'PATCH',
      headers: authHeaders(instructorToken),
      body: JSON.stringify({ status: 'OPEN' })
    });

    const socket = new WebSocket(`${harness.wsBaseUrl}?token=${buyerToken}`);
    const events = createEventCollector(socket);
    await once(socket, 'open');

    const readyEvent = await events.waitFor((event) => event.type === 'connection.ready');
    const snapshotEvent = await events.waitFor((event) => event.type === 'orderbook.snapshot');
    assert.equal(readyEvent.payload.role, 'STUDENT');
    assert.equal(snapshotEvent.payload.session.id, sessionId);

    await requestJson(`${harness.baseUrl}/api/orders`, {
      method: 'POST',
      headers: authHeaders(sellerToken, { 'idempotency-key': 'seller-live-1' }),
      body: JSON.stringify({
        side: 'SELL',
        orderType: 'LIMIT',
        quantity: 1,
        limitPriceCents: 1000
      })
    });

    await requestJson(`${harness.baseUrl}/api/orders`, {
      method: 'POST',
      headers: authHeaders(buyerToken, { 'idempotency-key': 'buyer-live-1' }),
      body: JSON.stringify({
        side: 'BUY',
        orderType: 'MARKET',
        quantity: 1
      })
    });

    const tradeEvent = await events.waitFor((event) => event.type === 'trade.executed');
    const fillEvent = await events.waitFor((event) => event.type === 'player.fill');
    const accountEvent = await events.waitFor(
      (event) => event.type === 'account.updated' && event.payload.account.positionQty === 6
    );

    assert.equal(tradeEvent.payload.fill.priceCents, 1000);
    assert.equal(fillEvent.payload.fills.length, 1);
    assert.equal(accountEvent.payload.account.cashCents, 19000);

    const startedAt = new Date(Date.now() - 21 * 60 * 1000).toISOString();
    await harness.runtime.db.query(
      `UPDATE market_sessions
       SET started_at = $2,
           opened_at = $2,
           elapsed_open_seconds = 0
       WHERE id = $1`,
      [sessionId, startedAt]
    );

    const releaseEvents = await harness.runtime.gameService.processDueAnnouncements();
    harness.runtime.websocketHub.publishMany(releaseEvents);

    const announcementEvent = await events.waitFor(
      (event) => event.type === 'announcement.created' && ['SAR', 'EPS'].includes(event.payload.announcement.announcementType)
    );
    assert.ok(['SAR', 'EPS'].includes(announcementEvent.payload.announcement.announcementType));

    socket.close();
  } finally {
    await harness.close();
  }
});

test('automatic public information and private peeks follow the classroom game rules', async () => {
  const harness = await createServerHarness();

  try {
    const createResult = await requestJson(`${harness.baseUrl}/api/sessions`, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'x-admin-secret': 'bootstrap-secret'
      },
      body: JSON.stringify({
        sessionName: 'Game Rules',
        referencePriceCents: 4000
      })
    });

    const instructorToken = createResult.body.instructor.participant.authToken;
    const joinCode = createResult.body.session.joinCode;
    const sessionId = createResult.body.session.id;

    const studentJoin = await requestJson(`${harness.baseUrl}/api/sessions/join`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        joinCode,
        displayName: 'Student'
      })
    });
    const studentToken = studentJoin.body.participant.authToken;

    const meResult = await requestJson(`${harness.baseUrl}/api/me`, {
      headers: {
        authorization: `Bearer ${studentToken}`
      }
    });
    assert.equal(meResult.body.features.peeks.supported, true);
    assert.equal(meResult.body.features.peeks.priceCents, 100);

    await requestJson(`${harness.baseUrl}/api/sessions/${sessionId}/state`, {
      method: 'PATCH',
      headers: authHeaders(instructorToken),
      body: JSON.stringify({ status: 'OPEN' })
    });

    const peekResult = await requestJson(`${harness.baseUrl}/api/peeks`, {
      method: 'POST',
      headers: {
        authorization: `Bearer ${studentToken}`
      }
    });

    assert.equal(peekResult.response.status, 201);
    assert.equal(peekResult.body.peek.priceCents, 100);
    assert.equal(Array.isArray(peekResult.body.peek.contributions), true);
    assert.equal(peekResult.body.peek.contributions.length, 3);
    assert.equal('card' in peekResult.body.peek, false);
    assert.ok(peekResult.body.peek.contributions.every((value) => Number.isInteger(value)));

    const accountResult = await requestJson(`${harness.baseUrl}/api/me/account`, {
      headers: {
        authorization: `Bearer ${studentToken}`
      }
    });

    assert.equal(accountResult.body.account.cashCents, 19900);
    assert.equal(accountResult.body.privatePeeks.length, 1);
    assert.equal(accountResult.body.privatePeeks[0].contributions.length, 3);

    const startedAt = new Date(Date.now() - 21 * 60 * 1000).toISOString();
    await harness.runtime.db.query(
      `UPDATE market_sessions
       SET started_at = $2,
           opened_at = $2,
           elapsed_open_seconds = 0
       WHERE id = $1`,
      [sessionId, startedAt]
    );

    const events = await harness.runtime.gameService.processDueAnnouncements();
    harness.runtime.websocketHub.publishMany(events);

    const dashboard = await requestJson(`${harness.baseUrl}/api/instructor/dashboard`, {
      headers: {
        authorization: `Bearer ${instructorToken}`
      }
    });

    const autoAnnouncements = dashboard.body.orderBook.announcements.filter((announcement) =>
      ['SAR', 'EPS'].includes(announcement.announcementType)
    );
    const deckSize = await harness.runtime.db.query(
      `SELECT COUNT(*) AS count
       FROM session_cards
       WHERE session_id = $1`,
      [sessionId]
    );
    const duplicateLabels = await harness.runtime.db.query(
      `SELECT label, COUNT(*) AS count
       FROM session_cards
       WHERE session_id = $1
       GROUP BY label
       HAVING COUNT(*) > 2`,
      [sessionId]
    );
    const activeCards = await harness.runtime.db.query(
      `SELECT COUNT(*) AS count
       FROM session_cards
       WHERE session_id = $1
         AND state = 'ACTIVE'`,
      [sessionId]
    );
    const dashboardCardTotal = dashboard.body.liquidationComposition.cards.reduce(
      (sum, card) => sum + card.contributionCents,
      0
    );

    assert.equal(autoAnnouncements.length, 6);
    assert.equal(dashboard.body.publicInfoSchedule.filter((item) => item.status === 'RELEASED').length, 6);
    assert.equal(Number(deckSize.rows[0].count), 104);
    assert.equal(duplicateLabels.rows.length, 0);
    assert.equal(Number(activeCards.rows[0].count), 10);
    assert.equal(dashboard.body.liquidationComposition.cardCount, 10);
    assert.equal(dashboard.body.liquidationComposition.currentValueCents, dashboardCardTotal);
  } finally {
    await harness.close();
  }
});

test('student account state only returns the latest three private peeks', async () => {
  const harness = await createServerHarness();

  try {
    const createResult = await requestJson(`${harness.baseUrl}/api/sessions`, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'x-admin-secret': 'bootstrap-secret'
      },
      body: JSON.stringify({
        sessionName: 'Peek Limit',
        referencePriceCents: 4000
      })
    });

    const instructorToken = createResult.body.instructor.participant.authToken;
    const joinCode = createResult.body.session.joinCode;
    const sessionId = createResult.body.session.id;

    const studentJoin = await requestJson(`${harness.baseUrl}/api/sessions/join`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        joinCode,
        displayName: 'Student'
      })
    });
    const studentToken = studentJoin.body.participant.authToken;

    await requestJson(`${harness.baseUrl}/api/sessions/${sessionId}/state`, {
      method: 'PATCH',
      headers: authHeaders(instructorToken),
      body: JSON.stringify({ status: 'OPEN' })
    });

    const purchasedPeekIds = [];
    for (let index = 0; index < 4; index += 1) {
      const peekResult = await requestJson(`${harness.baseUrl}/api/peeks`, {
        method: 'POST',
        headers: {
          authorization: `Bearer ${studentToken}`
        }
      });
      purchasedPeekIds.push(peekResult.body.peek.id);
    }

    const accountResult = await requestJson(`${harness.baseUrl}/api/me/account`, {
      headers: {
        authorization: `Bearer ${studentToken}`
      }
    });

    assert.equal(accountResult.body.privatePeeks.length, 3);
    assert.deepEqual(
      accountResult.body.privatePeeks.map((peek) => peek.id),
      purchasedPeekIds.slice(-3).reverse()
    );
    assert.ok(accountResult.body.privatePeeks.every((peek) => peek.contributions.length === 3));
  } finally {
    await harness.close();
  }
});

test('final liquidation uses the hidden card value and applies borrowing interest', async () => {
  const harness = await createServerHarness();

  try {
    const createResult = await requestJson(`${harness.baseUrl}/api/sessions`, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'x-admin-secret': 'bootstrap-secret'
      },
      body: JSON.stringify({
        sessionName: 'Final Value',
        referencePriceCents: 4000
      })
    });

    const instructorToken = createResult.body.instructor.participant.authToken;
    const joinCode = createResult.body.session.joinCode;
    const sessionId = createResult.body.session.id;

    const sellerJoin = await requestJson(`${harness.baseUrl}/api/sessions/join`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        joinCode,
        displayName: 'Seller'
      })
    });
    const buyerJoin = await requestJson(`${harness.baseUrl}/api/sessions/join`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        joinCode,
        displayName: 'Borrower'
      })
    });

    const sellerToken = sellerJoin.body.participant.authToken;
    const buyerToken = buyerJoin.body.participant.authToken;

    await requestJson(`${harness.baseUrl}/api/sessions/${sessionId}/state`, {
      method: 'PATCH',
      headers: authHeaders(instructorToken),
      body: JSON.stringify({ status: 'OPEN' })
    });

    await requestJson(`${harness.baseUrl}/api/orders`, {
      method: 'POST',
      headers: authHeaders(sellerToken, { 'idempotency-key': 'seller-final-1' }),
      body: JSON.stringify({
        side: 'SELL',
        orderType: 'LIMIT',
        quantity: 6,
        limitPriceCents: 4000
      })
    });

    await requestJson(`${harness.baseUrl}/api/orders`, {
      method: 'POST',
      headers: authHeaders(buyerToken, { 'idempotency-key': 'buyer-final-1' }),
      body: JSON.stringify({
        side: 'BUY',
        orderType: 'MARKET',
        quantity: 6
      })
    });

    const liquidationValueResult = await harness.runtime.db.query(
      `SELECT COALESCE(SUM(contribution_cents), 0) AS liquidation_value_cents
       FROM session_cards
       WHERE session_id = $1
         AND state = 'ACTIVE'`,
      [sessionId]
    );
    const liquidationValueCents = Number(liquidationValueResult.rows[0].liquidation_value_cents);

    const finalizeResult = await requestJson(`${harness.baseUrl}/api/sessions/${sessionId}/finalize`, {
      method: 'POST',
      headers: authHeaders(instructorToken)
    });

    assert.equal(finalizeResult.response.status, 200);
    assert.equal(finalizeResult.body.leaderboard.liquidation.revealed, true);
    assert.equal(finalizeResult.body.leaderboard.liquidation.valueCents, liquidationValueCents);

    const borrower = finalizeResult.body.leaderboard.leaderboard.find((row) => row.displayName === 'Borrower');

    assert.equal(borrower.cashCents, -4000);
    assert.equal(borrower.borrowingFeeCents, 400);
    assert.equal(borrower.settledCashCents, -4400);
    assert.equal(borrower.portfolioValueCents, -4400 + 11 * liquidationValueCents);

    const postFinalizeBook = await requestJson(`${harness.baseUrl}/api/order-book`, {
      headers: {
        authorization: `Bearer ${buyerToken}`
      }
    });

    assert.equal(postFinalizeBook.body.liquidation.revealed, true);
    assert.equal(postFinalizeBook.body.market.markPriceCents, liquidationValueCents);
  } finally {
    await harness.close();
  }
});

test('elapsed game time can auto-finalize the session from persisted state', async () => {
  const harness = await createServerHarness();

  try {
    const createResult = await requestJson(`${harness.baseUrl}/api/sessions`, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'x-admin-secret': 'bootstrap-secret'
      },
      body: JSON.stringify({
        sessionName: 'Auto Close',
        referencePriceCents: 4000
      })
    });

    const instructorToken = createResult.body.instructor.participant.authToken;
    const sessionId = createResult.body.session.id;

    await requestJson(`${harness.baseUrl}/api/sessions/${sessionId}/state`, {
      method: 'PATCH',
      headers: authHeaders(instructorToken),
      body: JSON.stringify({ status: 'OPEN' })
    });

    const startedAt = new Date(Date.now() - 26 * 60 * 1000).toISOString();
    await harness.runtime.db.query(
      `UPDATE market_sessions
       SET started_at = $2,
           opened_at = $2,
           elapsed_open_seconds = 0
       WHERE id = $1`,
      [sessionId, startedAt]
    );

    const events = await harness.runtime.gameService.processDueAnnouncements();

    assert.ok(events.some((event) => event.type === 'leaderboard.final'));

    const dashboard = await requestJson(`${harness.baseUrl}/api/instructor/dashboard`, {
      headers: {
        authorization: `Bearer ${instructorToken}`
      }
    });

    assert.equal(dashboard.body.session.status, 'CLOSED');
    assert.equal(dashboard.body.liquidation.revealed, true);
  } finally {
    await harness.close();
  }
});
