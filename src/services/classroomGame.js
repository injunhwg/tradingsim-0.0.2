const crypto = require('node:crypto');

const DEFAULT_REFERENCE_PRICE_CENTS = 4000;
const DEFAULT_GAME_DURATION_SECONDS = 15 * 60;
const DEFAULT_PEEK_PRICE_CENTS = 100;
const DEFAULT_BORROW_INTEREST_BPS = 1000;
const SAR_BONUS_CENTS = 2000;
const DEFAULT_EPS_ANNOUNCEMENT_COUNT = 4;
const DEFAULT_TRADABLE_STOCKS = [
  { stockKey: 'A', displayName: '주식 A', sortOrder: 1, initialPositionQty: 5 },
  { stockKey: 'B', displayName: '주식 B', sortOrder: 2, initialPositionQty: 5 },
  { stockKey: 'C', displayName: '주식 C', sortOrder: 3, initialPositionQty: 5 }
];

const SUITS = [
  { code: 'S', color: 'BLACK' },
  { code: 'C', color: 'BLACK' },
  { code: 'H', color: 'RED' },
  { code: 'D', color: 'RED' }
];

const RANKS = [
  'A',
  '2',
  '3',
  '4',
  '5',
  '6',
  '7',
  '8',
  '9',
  '10',
  'J',
  'Q',
  'K'
];

function baseValueCentsForRank(rank) {
  if (rank === 'A') {
    return 2000;
  }

  if (['J', 'Q', 'K'].includes(rank)) {
    return 1000;
  }

  return Number.parseInt(rank, 10) * 100;
}

function contributionCentsForCard(rank, color) {
  const baseValueCents = baseValueCentsForRank(rank);
  return color === 'BLACK' ? baseValueCents * 2 : -baseValueCents;
}

function normalizePositiveInteger(value, fallback) {
  return Number.isInteger(value) && value > 0 ? value : fallback;
}

function buildPublicInfoSchedule(
  totalDurationSeconds = DEFAULT_GAME_DURATION_SECONDS,
  epsAnnouncementCount = DEFAULT_EPS_ANNOUNCEMENT_COUNT
) {
  const resolvedDurationSeconds = normalizePositiveInteger(totalDurationSeconds, DEFAULT_GAME_DURATION_SECONDS);
  const resolvedEpsAnnouncementCount = normalizePositiveInteger(
    epsAnnouncementCount,
    DEFAULT_EPS_ANNOUNCEMENT_COUNT
  );

  const sarAnnouncements = [
    { infoKey: 'SAR_1', infoType: 'SAR', sequenceNo: 1, scheduledOffsetSeconds: 0 },
    {
      infoKey: 'SAR_2',
      infoType: 'SAR',
      sequenceNo: 2,
      scheduledOffsetSeconds: Math.round(resolvedDurationSeconds / 2)
    }
  ];
  const epsAnnouncements = Array.from({ length: resolvedEpsAnnouncementCount }, (_value, index) => {
    const sequenceNo = index + 1;
    return {
      infoKey: `EPS_${sequenceNo}`,
      infoType: 'EPS',
      sequenceNo,
      scheduledOffsetSeconds: Math.round((resolvedDurationSeconds * sequenceNo) / (resolvedEpsAnnouncementCount + 1))
    };
  });

  return [...sarAnnouncements, ...epsAnnouncements].sort((left, right) => {
    if (left.scheduledOffsetSeconds !== right.scheduledOffsetSeconds) {
      return left.scheduledOffsetSeconds - right.scheduledOffsetSeconds;
    }

    const typePriority = { SAR: 0, EPS: 1 };
    return (typePriority[left.infoType] ?? 99) - (typePriority[right.infoType] ?? 99) || left.sequenceNo - right.sequenceNo;
  });
}

const PUBLIC_INFO_SCHEDULE = buildPublicInfoSchedule();

function buildTradableStocks(referencePriceCents = DEFAULT_REFERENCE_PRICE_CENTS) {
  const resolvedReferencePriceCents = normalizePositiveInteger(referencePriceCents, DEFAULT_REFERENCE_PRICE_CENTS);

  return DEFAULT_TRADABLE_STOCKS.map((stock) => ({
    ...stock,
    referencePriceCents: resolvedReferencePriceCents
  }));
}

function createShuffledDeck() {
  const cards = [];

  for (let copyIndex = 0; copyIndex < 2; copyIndex += 1) {
    for (const suit of SUITS) {
      for (const rank of RANKS) {
        cards.push({
          rank,
          suit: suit.code,
          color: suit.color,
          label: `${rank}${suit.code}`,
          baseValueCents: baseValueCentsForRank(rank),
          contributionCents: contributionCentsForCard(rank, suit.color)
        });
      }
    }
  }

  for (let index = cards.length - 1; index > 0; index -= 1) {
    const swapIndex = crypto.randomInt(index + 1);
    [cards[index], cards[swapIndex]] = [cards[swapIndex], cards[index]];
  }

  return cards.map((card, index) => ({
    ...card,
    deckOrder: index + 1,
    state: index < 10 ? 'ACTIVE' : 'DECK'
  }));
}

function sampleWithoutReplacement(items, sampleSize) {
  const pool = [...items];

  for (let index = pool.length - 1; index > 0; index -= 1) {
    const swapIndex = crypto.randomInt(index + 1);
    [pool[index], pool[swapIndex]] = [pool[swapIndex], pool[index]];
  }

  return pool.slice(0, Math.min(sampleSize, pool.length));
}

function pickRandomItem(items) {
  if (!items || items.length === 0) {
    return null;
  }

  return items[crypto.randomInt(items.length)];
}

function centsToSignedDollars(cents) {
  const absolute = Math.abs(Number(cents) || 0) / 100;
  const prefix = cents < 0 ? '-' : '+';
  return `${prefix}$${absolute.toFixed(2)}`;
}

function formatSarMessage(sequenceNo, reportTotalCents, stockDisplayName) {
  const prefix = stockDisplayName ? `${stockDisplayName} ` : '';
  return `${prefix}SAR ${sequenceNo}차: 다섯 장 합계에 $20를 더한 값은 ${centsToSignedDollars(reportTotalCents)}입니다.`;
}

function formatEpsMessage(sequenceNo, deltaCents, stockDisplayName) {
  const prefix = stockDisplayName ? `${stockDisplayName} ` : '';
  return `${prefix}EPS ${sequenceNo}차: 카드 1장이 빠지고 1장이 추가되었습니다. 순변화는 ${centsToSignedDollars(deltaCents)}입니다.`;
}

function computeElapsedSeconds(session, now = new Date()) {
  const baseElapsed = Number(session.elapsed_open_seconds || 0);

  if (!session.opened_at) {
    return baseElapsed;
  }

  const deltaMs = now.getTime() - new Date(session.opened_at).getTime();
  return Math.max(baseElapsed, baseElapsed + Math.floor(Math.max(deltaMs, 0) / 1000));
}

function computeRemainingSeconds(session, now = new Date()) {
  const totalDurationSeconds = Number(session.total_duration_seconds || DEFAULT_GAME_DURATION_SECONDS);
  return Math.max(0, totalDurationSeconds - computeElapsedSeconds(session, now));
}

function hasSessionStarted(session) {
  return Boolean(session?.started_at);
}

function computeBorrowFeeCents(cashCents, borrowInterestBps = DEFAULT_BORROW_INTEREST_BPS) {
  if ((Number(cashCents) || 0) >= 0) {
    return 0;
  }

  return Math.round(Math.abs(cashCents) * (borrowInterestBps / 10000));
}

function settleCashCents(cashCents, borrowInterestBps = DEFAULT_BORROW_INTEREST_BPS) {
  return cashCents - computeBorrowFeeCents(cashCents, borrowInterestBps);
}

function formatCard(row) {
  if (!row) {
    return null;
  }

  return {
    id: row.id,
    label: row.label,
    rank: row.rank,
    suit: row.suit,
    color: row.color,
    contributionCents: row.contribution_cents
  };
}

module.exports = {
  DEFAULT_BORROW_INTEREST_BPS,
  DEFAULT_EPS_ANNOUNCEMENT_COUNT,
  DEFAULT_GAME_DURATION_SECONDS,
  DEFAULT_PEEK_PRICE_CENTS,
  DEFAULT_REFERENCE_PRICE_CENTS,
  DEFAULT_TRADABLE_STOCKS,
  PUBLIC_INFO_SCHEDULE,
  SAR_BONUS_CENTS,
  buildTradableStocks,
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
};
