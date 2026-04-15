const http = require('node:http');

const { loadConfig } = require('./config');
const { createPgDatabase } = require('./db/database');
const { GameService } = require('./services/gameService');
const { createApp } = require('./http/createApp');
const { WebSocketHub } = require('./realtime/websocketHub');

async function createRuntime(options = {}) {
  const config = loadConfig(options.config);
  const db = options.db || createPgDatabase(config.DATABASE_URL);
  const gameService = options.gameService || new GameService({ db, config });

  await gameService.initialize();
  const restoredState = await gameService.restoreRuntimeState();

  const app = createApp({
    config,
    gameService,
    logger: options.logger || console
  });

  const server = http.createServer(app);
  const websocketHub = new WebSocketHub({
    server,
    gameService,
    logger: options.logger || console
  });
  let announcementTimer = null;
  let announcementPollInFlight = false;

  app.locals.websocketHub = websocketHub;

  return {
    app,
    server,
    db,
    config,
    gameService,
    websocketHub,
    restoredState,
    async start() {
      await new Promise((resolve) => {
        server.listen(config.PORT, config.HOST, resolve);
      });

      announcementTimer = setInterval(async () => {
        if (announcementPollInFlight) {
          return;
        }

        announcementPollInFlight = true;
        try {
          const events = await gameService.processDueAnnouncements();
          websocketHub.publishMany(events);
        } catch (error) {
          (options.logger || console).error?.('Scheduled announcement poll failed', error);
        } finally {
          announcementPollInFlight = false;
        }
      }, config.ANNOUNCEMENT_POLL_INTERVAL_MS || 1000);

      const address = server.address();
      return {
        host: typeof address === 'object' && address ? address.address : config.HOST,
        port: typeof address === 'object' && address ? address.port : config.PORT
      };
    },
    async stop() {
      if (announcementTimer) {
        clearInterval(announcementTimer);
        announcementTimer = null;
      }

      await websocketHub.close();
      await new Promise((resolve, reject) => {
        server.close((error) => {
          if (error) {
            reject(error);
            return;
          }

          resolve();
        });
      });
      await gameService.close();
    }
  };
}

module.exports = {
  createRuntime
};
