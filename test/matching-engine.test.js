const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs/promises');
const os = require('node:os');
const path = require('node:path');

const {
  MatchingEngine,
  STARTING_CASH_CENTS,
  STARTING_POSITION_QTY,
  createPGliteDatabase
} = require('../src');

async function createHarness({ persistent = false, dataDir } = {}) {
  const resolvedDataDir = persistent
    ? dataDir || (await fs.mkdtemp(path.join(os.tmpdir(), 'trading-engine-')))
    : undefined;

  const db = await createPGliteDatabase(resolvedDataDir ? { dataDir: resolvedDataDir } : {});
  const engine = new MatchingEngine({ db });
  await engine.initialize();
  const session = await engine.createSession();

  async function createTrader(label) {
    return engine.createParticipant({
      sessionId: session.id,
      externalId: label,
      displayName: label
    });
  }

  return {
    dataDir: resolvedDataDir,
    db,
    engine,
    session,
    createTrader,
    async close() {
      await engine.close();
    }
  };
}

async function submit(engine, order) {
  return engine.submitOrder(order);
}

test('market order execution uses the resting order price', async () => {
  const harness = await createHarness();

  try {
    const seller = await harness.createTrader('seller');
    const buyer = await harness.createTrader('buyer');

    await submit(harness.engine, {
      sessionId: harness.session.id,
      participantId: seller.participant.id,
      idempotencyKey: 'sell-1',
      side: 'SELL',
      orderType: 'LIMIT',
      quantity: 3,
      limitPriceCents: 1200
    });

    const response = await submit(harness.engine, {
      sessionId: harness.session.id,
      participantId: buyer.participant.id,
      idempotencyKey: 'buy-1',
      side: 'BUY',
      orderType: 'MARKET',
      quantity: 2
    });

    assert.equal(response.order.status, 'FILLED');
    assert.equal(response.order.remainingQty, 0);
    assert.equal(response.fills.length, 1);
    assert.equal(response.fills[0].priceCents, 1200);
    assert.equal(response.fills[0].qty, 2);

    const buyerAccount = await harness.engine.getAccount(buyer.participant.id);
    const sellerAccount = await harness.engine.getAccount(seller.participant.id);
    const openOrders = await harness.engine.listOpenOrders(harness.session.id);

    assert.equal(buyerAccount.cash_cents, 17600);
    assert.equal(buyerAccount.position_qty, 7);
    assert.equal(sellerAccount.cash_cents, 22400);
    assert.equal(sellerAccount.position_qty, 3);
    assert.equal(sellerAccount.reserved_sell_qty, 1);
    assert.equal(openOrders.length, 1);
    assert.equal(openOrders[0].remaining_qty, 1);
  } finally {
    await harness.close();
  }
});

test('non-crossing limit orders rest on the book and reserve capacity', async () => {
  const harness = await createHarness();

  try {
    const buyer = await harness.createTrader('buyer');

    const response = await submit(harness.engine, {
      sessionId: harness.session.id,
      participantId: buyer.participant.id,
      idempotencyKey: 'rest-1',
      side: 'BUY',
      orderType: 'LIMIT',
      quantity: 4,
      limitPriceCents: 900
    });

    const buyerAccount = await harness.engine.getAccount(buyer.participant.id);
    const openOrders = await harness.engine.listOpenOrders(harness.session.id);

    assert.equal(response.order.status, 'OPEN');
    assert.equal(response.fills.length, 0);
    assert.equal(buyerAccount.cash_cents, STARTING_CASH_CENTS);
    assert.equal(buyerAccount.position_qty, STARTING_POSITION_QTY);
    assert.equal(buyerAccount.reserved_buy_cents, 3600);
    assert.equal(openOrders.length, 1);
    assert.equal(openOrders[0].side, 'BUY');
  } finally {
    await harness.close();
  }
});

test('crossing limit orders execute immediately at the resting order price', async () => {
  const harness = await createHarness();

  try {
    const seller = await harness.createTrader('seller');
    const buyer = await harness.createTrader('buyer');

    await submit(harness.engine, {
      sessionId: harness.session.id,
      participantId: seller.participant.id,
      idempotencyKey: 'sell-resting',
      side: 'SELL',
      orderType: 'LIMIT',
      quantity: 2,
      limitPriceCents: 1200
    });

    const response = await submit(harness.engine, {
      sessionId: harness.session.id,
      participantId: buyer.participant.id,
      idempotencyKey: 'buy-cross',
      side: 'BUY',
      orderType: 'LIMIT',
      quantity: 2,
      limitPriceCents: 1500
    });

    assert.equal(response.order.status, 'FILLED');
    assert.equal(response.fills.length, 1);
    assert.equal(response.fills[0].priceCents, 1200);

    const buyerAccount = await harness.engine.getAccount(buyer.participant.id);
    assert.equal(buyerAccount.cash_cents, 17600);
    assert.equal(buyerAccount.position_qty, 7);
    assert.equal(buyerAccount.reserved_buy_cents, 0);
    assert.equal((await harness.engine.listOpenOrders(harness.session.id)).length, 0);
  } finally {
    await harness.close();
  }
});

test('partial fills consume the best prices first and leave the remaining order resting', async () => {
  const harness = await createHarness();

  try {
    const sellerA = await harness.createTrader('seller-a');
    const sellerB = await harness.createTrader('seller-b');
    const buyer = await harness.createTrader('buyer');

    await submit(harness.engine, {
      sessionId: harness.session.id,
      participantId: sellerA.participant.id,
      idempotencyKey: 'sell-a',
      side: 'SELL',
      orderType: 'LIMIT',
      quantity: 2,
      limitPriceCents: 1000
    });

    await submit(harness.engine, {
      sessionId: harness.session.id,
      participantId: sellerB.participant.id,
      idempotencyKey: 'sell-b',
      side: 'SELL',
      orderType: 'LIMIT',
      quantity: 3,
      limitPriceCents: 1100
    });

    const response = await submit(harness.engine, {
      sessionId: harness.session.id,
      participantId: buyer.participant.id,
      idempotencyKey: 'buy-market',
      side: 'BUY',
      orderType: 'MARKET',
      quantity: 4
    });

    const fills = await harness.engine.listFills(harness.session.id);
    const sellerBOrder = (await harness.engine.listOrders(harness.session.id)).find((order) => order.id === response.fills[1].restingOrderId);
    const buyerAccount = await harness.engine.getAccount(buyer.participant.id);

    assert.equal(response.order.status, 'FILLED');
    assert.equal(response.fills.length, 2);
    assert.deepEqual(
      response.fills.map((fill) => [fill.priceCents, fill.qty]),
      [
        [1000, 2],
        [1100, 2]
      ]
    );
    assert.equal(fills.length, 2);
    assert.equal(buyerAccount.cash_cents, 15800);
    assert.equal(buyerAccount.position_qty, 9);
    assert.equal(sellerBOrder.status, 'PARTIALLY_FILLED');
    assert.equal(sellerBOrder.remaining_qty, 1);
  } finally {
    await harness.close();
  }
});

test('duplicate request handling returns the original response without creating extra fills', async () => {
  const harness = await createHarness();

  try {
    const seller = await harness.createTrader('seller');
    const buyer = await harness.createTrader('buyer');

    await submit(harness.engine, {
      sessionId: harness.session.id,
      participantId: seller.participant.id,
      idempotencyKey: 'sell-once',
      side: 'SELL',
      orderType: 'LIMIT',
      quantity: 1,
      limitPriceCents: 1000
    });

    const first = await submit(harness.engine, {
      sessionId: harness.session.id,
      participantId: buyer.participant.id,
      idempotencyKey: 'dup-key',
      side: 'BUY',
      orderType: 'MARKET',
      quantity: 1
    });

    const second = await submit(harness.engine, {
      sessionId: harness.session.id,
      participantId: buyer.participant.id,
      idempotencyKey: 'dup-key',
      side: 'BUY',
      orderType: 'MARKET',
      quantity: 1
    });

    assert.equal(first.order.id, second.order.id);
    assert.equal(second.idempotencyHit, true);
    assert.equal((await harness.engine.listFills(harness.session.id)).length, 1);
    assert.equal((await harness.engine.listOrders(harness.session.id)).length, 2);
  } finally {
    await harness.close();
  }
});

test('concurrent order submissions do not overfill the book', async () => {
  const harness = await createHarness();

  try {
    const seller = await harness.createTrader('seller');
    const buyers = [];

    for (let index = 0; index < 40; index += 1) {
      buyers.push(await harness.createTrader(`buyer-${index}`));
    }

    await submit(harness.engine, {
      sessionId: harness.session.id,
      participantId: seller.participant.id,
      idempotencyKey: 'sell-ten',
      side: 'SELL',
      orderType: 'LIMIT',
      quantity: 10,
      limitPriceCents: 1000
    });

    const results = await Promise.all(
      buyers.map((buyer, index) =>
        submit(harness.engine, {
          sessionId: harness.session.id,
          participantId: buyer.participant.id,
          idempotencyKey: `buy-${index}`,
          side: 'BUY',
          orderType: 'MARKET',
          quantity: 1
        })
      )
    );

    const fillCount = results.filter((result) => result.fills.length === 1).length;
    const cancelledCount = results.filter((result) => result.order.status === 'CANCELLED').length;
    const fills = await harness.engine.listFills(harness.session.id);
    const sellerAccount = await harness.engine.getAccount(seller.participant.id);

    assert.equal(fillCount, 10);
    assert.equal(cancelledCount, 30);
    assert.equal(fills.length, 10);
    assert.equal(fills.reduce((sum, fill) => sum + fill.qty, 0), 10);
    assert.equal(sellerAccount.position_qty, -5);
    assert.equal(sellerAccount.cash_cents, 30000);
    assert.equal((await harness.engine.listOpenOrders(harness.session.id)).length, 0);
  } finally {
    await harness.close();
  }
});

test('short constraint enforcement rejects orders that would exceed -5 shares', async () => {
  const harness = await createHarness();

  try {
    const seller = await harness.createTrader('seller');

    const response = await submit(harness.engine, {
      sessionId: harness.session.id,
      participantId: seller.participant.id,
      idempotencyKey: 'too-short',
      side: 'SELL',
      orderType: 'LIMIT',
      quantity: 11,
      limitPriceCents: 1000
    });

    const sellerAccount = await harness.engine.getAccount(seller.participant.id);

    assert.equal(response.order.status, 'REJECTED');
    assert.equal(response.order.rejectionReason, 'INSUFFICIENT_SHORT_CAPACITY');
    assert.equal(sellerAccount.position_qty, STARTING_POSITION_QTY);
    assert.equal(sellerAccount.reserved_sell_qty, 0);
  } finally {
    await harness.close();
  }
});

test('borrowing constraint enforcement rejects limit buys above available buying power', async () => {
  const harness = await createHarness();

  try {
    const buyer = await harness.createTrader('buyer');

    const response = await submit(harness.engine, {
      sessionId: harness.session.id,
      participantId: buyer.participant.id,
      idempotencyKey: 'too-expensive',
      side: 'BUY',
      orderType: 'LIMIT',
      quantity: 5,
      limitPriceCents: 10000
    });

    const buyerAccount = await harness.engine.getAccount(buyer.participant.id);

    assert.equal(response.order.status, 'REJECTED');
    assert.equal(response.order.rejectionReason, 'INSUFFICIENT_BUYING_POWER');
    assert.equal(buyerAccount.cash_cents, STARTING_CASH_CENTS);
    assert.equal(buyerAccount.reserved_buy_cents, 0);
  } finally {
    await harness.close();
  }
});

test('restart recovery continues matching from persisted database state', async () => {
  const firstHarness = await createHarness({ persistent: true });

  try {
    const seller = await firstHarness.createTrader('seller');
    const buyer = await firstHarness.createTrader('buyer');

    await submit(firstHarness.engine, {
      sessionId: firstHarness.session.id,
      participantId: seller.participant.id,
      idempotencyKey: 'rest-before-restart',
      side: 'SELL',
      orderType: 'LIMIT',
      quantity: 4,
      limitPriceCents: 1300
    });

    await firstHarness.close();

    const db = await createPGliteDatabase({ dataDir: firstHarness.dataDir });
    const restartedEngine = new MatchingEngine({ db });
    await restartedEngine.initialize();

    const response = await restartedEngine.submitOrder({
      sessionId: firstHarness.session.id,
      participantId: buyer.participant.id,
      idempotencyKey: 'after-restart',
      side: 'BUY',
      orderType: 'MARKET',
      quantity: 2
    });

    const openOrders = await restartedEngine.listOpenOrders(firstHarness.session.id);
    const fills = await restartedEngine.listFills(firstHarness.session.id);

    assert.equal(response.order.status, 'FILLED');
    assert.equal(response.fills.length, 1);
    assert.equal(response.fills[0].priceCents, 1300);
    assert.equal(openOrders.length, 1);
    assert.equal(openOrders[0].remaining_qty, 2);
    assert.equal(fills.length, 1);

    await restartedEngine.close();
  } finally {
    if (firstHarness.dataDir) {
      await fs.rm(firstHarness.dataDir, { recursive: true, force: true });
    }
  }
});

test('order cancellation releases reservations and removes the order from the resting book', async () => {
  const harness = await createHarness();

  try {
    const buyer = await harness.createTrader('buyer');

    const orderResponse = await submit(harness.engine, {
      sessionId: harness.session.id,
      participantId: buyer.participant.id,
      idempotencyKey: 'cancel-me',
      side: 'BUY',
      orderType: 'LIMIT',
      quantity: 3,
      limitPriceCents: 1100
    });

    const cancelResponse = await harness.engine.cancelOrder({
      sessionId: harness.session.id,
      participantId: buyer.participant.id,
      orderId: orderResponse.order.id
    });

    const buyerAccount = await harness.engine.getAccount(buyer.participant.id);

    assert.equal(cancelResponse.order.status, 'CANCELLED');
    assert.equal(cancelResponse.order.cancelReason, 'USER_CANCELLED');
    assert.equal(buyerAccount.reserved_buy_cents, 0);
    assert.equal((await harness.engine.listOpenOrders(harness.session.id)).length, 0);
  } finally {
    await harness.close();
  }
});
