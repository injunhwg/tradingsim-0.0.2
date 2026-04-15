# Trading Simulator

This repository contains a minimal classroom trading game that is now deployable as a single Render app:

- Express server for REST API and static frontend
- WebSocket server for live classroom updates
- PostgreSQL-backed matching engine
- Student and instructor browser pages served by the same app

The MVP is intentionally narrow:

- one stock only
- one active classroom session at a time
- one Render web instance
- PostgreSQL is the source of truth
- hidden card-based liquidation value, SARs, EPS, and private peeks are now built in
- the liquidation deck is a doubled standard deck, so each exact card like `2C` can appear at most twice

## Requirements

- Node.js 24+
- PostgreSQL

## What Is Already Deployment-Ready

- Server binds to `HOST` and `PORT`
- `/healthz` checks database connectivity
- Matching, fills, orders, accounts, announcements, and session state persist in PostgreSQL
- Browser UI is served by the same Node app
- Startup restores recoverable runtime state from PostgreSQL
- Migrations are tracked in `schema_migrations` and can be applied repeatedly without duplicating work

## Remaining MVP Limits

- Presence is not persisted. Connected-student counts come from live WebSocket connections.
- Automatic public information uses a database-backed polling loop, not a separate job queue.
- There is no horizontal scaling design in this MVP.
- The timer and public-information schedule are designed for one active session and one app instance.

## Environment Variables

Copy `.env.example` to `.env` for local development.

- `NODE_ENV`
  Use `development` locally and `production` on Render.
- `HOST`
  Bind address. Use `0.0.0.0` locally and on Render.
- `PORT`
  HTTP port. Render injects this automatically.
- `DATABASE_URL`
  PostgreSQL connection string.
- `BOOTSTRAP_ADMIN_SECRET`
  Required to create or reset a session from the instructor page.
- `DEFAULT_REFERENCE_PRICE_CENTS`
  Opening classroom price. Defaults to `$40.00`.
- `RECENT_TRADES_LIMIT`
  Number of recent trades returned in snapshots.
- `ANNOUNCEMENT_HISTORY_LIMIT`
  Number of recent announcements returned in snapshots.
- `ANNOUNCEMENT_POLL_INTERVAL_MS`
  Poll interval for releasing persisted automatic SAR/EPS reports.
- `GAME_DURATION_SECONDS`
  Classroom game length. Defaults to `900` seconds (`15:00`).
- `PEEK_PRICE_CENTS`
  Cost of one private peek. Defaults to `$1.00`.
- `BORROW_INTEREST_BPS`
  Final borrowing cost in basis points for negative cash. Defaults to `1000` (`10%`).

## Local Setup

1. Install dependencies:

```bash
cd /Users/injunhwang/Library/CloudStorage/Dropbox/6_teaching/2_investments/4_interactive/trading-simulator
npm install
```

2. Run migrations:

```bash
cd /Users/injunhwang/Library/CloudStorage/Dropbox/6_teaching/2_investments/4_interactive/trading-simulator
DATABASE_URL=postgres://postgres:postgres@127.0.0.1:5432/trading_simulator \
BOOTSTRAP_ADMIN_SECRET=change-me \
npm run migrate
```

3. Start the full app:

```bash
cd /Users/injunhwang/Library/CloudStorage/Dropbox/6_teaching/2_investments/4_interactive/trading-simulator
DATABASE_URL=postgres://postgres:postgres@127.0.0.1:5432/trading_simulator \
BOOTSTRAP_ADMIN_SECRET=change-me \
NODE_ENV=development \
npm start
```

4. Open the browser UI:

- Student page: [http://127.0.0.1:3000/student](http://127.0.0.1:3000/student)
- Instructor page: [http://127.0.0.1:3000/instructor](http://127.0.0.1:3000/instructor)

5. Smoke check:

```bash
curl http://127.0.0.1:3000/healthz
```

## Exact Local Test Command

```bash
cd /Users/injunhwang/Library/CloudStorage/Dropbox/6_teaching/2_investments/4_interactive/trading-simulator
npm test
```

## Exact Production Start Command

Render start command:

```bash
npm start
```

The server also runs the tracked migration initializer during startup as a safety belt, but the intended deploy flow is:

```bash
npm run migrate
npm start
```

## Database Migration and Initialization Flow

- SQL files in `migrations/` are applied in filename order.
- Applied migration filenames are recorded in `schema_migrations`.
- `npm run migrate` is safe to run repeatedly.
- Server startup also calls the migration initializer, so restarts are safe even if the explicit migration step was missed.
- For Render, the recommended flow is `preDeployCommand: npm run migrate` plus `startCommand: npm start`.

This means the app does not depend on any correctness-critical state living only in memory.

## Persisted vs Not Persisted

Persisted in PostgreSQL:

- market sessions and session status
- join codes and participant tokens
- accounts, balances, and positions
- orders and cancellations
- immutable fills
- idempotency keys
- hidden session card deck and active card state
- automatic public information schedule and released SAR/EPS payloads
- purchased private peeks
- public announcements generated from SAR/EPS releases
- migration history

Not persisted:

- live WebSocket connections
- connected-student presence
- in-process timers
- console logs
- browser `localStorage`

## Restart Behavior

Safe across restarts:

- order book state is rebuilt from persisted orders
- accounts and fills remain intact
- SAR/EPS releases continue from persisted elapsed game time
- hidden active-card state survives restarts, so final liquidation remains correct
- browser clients can reconnect and resync from REST snapshots

What changes on restart:

- active WebSocket connections drop and must reconnect
- connected-student presence starts empty until browsers reconnect
- the automatic public-info polling loop restarts when the server process comes back up

## Frontend Routes

- `/`
- `/student`
- `/instructor`

## API Overview

### Session and auth

- `POST /api/sessions`
- `POST /api/sessions/reset`
- `POST /api/sessions/join`
- `GET /api/me`

### Instructor controls

- `PATCH /api/sessions/:sessionId/state`
- `POST /api/sessions/:sessionId/finalize`
- `GET /api/instructor/dashboard`

### Trading and snapshots

- `POST /api/orders`
- `POST /api/orders/:orderId/cancel`
- `POST /api/peeks`
- `GET /api/order-book`
- `GET /api/me/account`
- `GET /api/leaderboard`

### Health

- `GET /healthz`

## WebSocket Overview

Connect to:

```text
ws://HOST:PORT/ws?token=<participant token>
```

Event types:

- `connection.ready`
- `orderbook.snapshot`
- `orderbook.updated`
- `trade.executed`
- `player.fill`
- `peek.revealed`
- `account.updated`
- `game.state`
- `announcement.created`
- `leaderboard.updated`
- `leaderboard.final`

## Render Deployment

This repo includes [render.yaml](./render.yaml) for the simplest reliable MVP deployment:

- one Render web service
- one Render PostgreSQL database
- one web instance
- same app serves frontend, API, and WebSocket traffic

### Recommended Render Setup

1. Push this repository to GitHub or GitLab.
2. In Render, create a Blueprint from the repository.
3. Review `render.yaml`.
4. Set a strong `BOOTSTRAP_ADMIN_SECRET`.
5. Deploy.

The blueprint config uses:

- `buildCommand: npm install`
- `preDeployCommand: npm run migrate`
- `startCommand: npm start`
- `healthCheckPath: /healthz`
- `NODE_ENV=production`
- `HOST=0.0.0.0`

### Manual Render Setup

If you do not use the Blueprint:

1. Create one PostgreSQL database in Render.
2. Create one Node web service in the same region.
3. Set the web service environment variables listed above.
4. Set the build command to `npm install`.
5. Set the start command to `npm start`.
6. Run `npm run migrate` before first production start.
7. Set the health check path to `/healthz`.

### Required Render Environment Variables

- `NODE_ENV=production`
- `HOST=0.0.0.0`
- `DATABASE_URL=<Render PostgreSQL connection string>`
- `BOOTSTRAP_ADMIN_SECRET=<strong secret>`
- `DEFAULT_REFERENCE_PRICE_CENTS=4000`
- `GAME_DURATION_SECONDS=900`
- `PEEK_PRICE_CENTS=100`
- `BORROW_INTEREST_BPS=1000`
- `RECENT_TRADES_LIMIT=20`
- `ANNOUNCEMENT_HISTORY_LIMIT=20`
- `ANNOUNCEMENT_POLL_INTERVAL_MS=1000`

Render provides `PORT`; do not hardcode it.

## Free-Tier Caveats

Free Render resources are not a good fit for live class:

- free web services can spin down when idle, which adds startup delay
- free services are not recommended for production use
- pre-deploy commands are only available on paid web service instance types
- free PostgreSQL instances can expire if they are not kept active

For classroom use, use a paid web service and a paid PostgreSQL plan.

## Pre-Class Deployment Checklist

1. Deploy the latest commit and verify the deploy succeeded.
2. Confirm `npm run migrate` completed successfully in the deploy logs.
3. Open `/healthz` and confirm it returns `{"ok":true}`.
4. Open `/instructor` and create or reset the classroom session.
5. Confirm the join code is visible.
6. Join once as a test student from `/student`.
7. Submit one small test trade and confirm it appears in recent trades.
8. Confirm the leaderboard updates.
9. Confirm phones and laptops on the classroom network can open the site.
10. Keep the instructor tab open during class.

## Troubleshooting

- `DATABASE_URL is required`
  Set `DATABASE_URL` before running `npm run migrate` or `npm start`.
- `/healthz` fails
  The app can reach the web service port, but not PostgreSQL. Check the database URL and region.
- Orders fail with `SESSION_NOT_OPEN`
  Open the session from the instructor page first.
- Students cannot join
  Verify the join code belongs to the currently active session and the session has not been reset.
- WebSocket updates stop after a deploy
  Refresh the browser. REST snapshots remain authoritative and the client will reconnect.
- Scheduled announcements do not appear
  Check that the server is running and `ANNOUNCEMENT_POLL_INTERVAL_MS` is not set to an extreme value.

## Notes for Future Hardening

- Add a durable outbox and replay cursor for WebSocket events
- Move presence to a shared store if you ever scale beyond one instance
- Add real authentication if this moves beyond classroom use
