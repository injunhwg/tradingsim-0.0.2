const path = require('node:path');
const express = require('express');

const { translateError, HttpError } = require('../services/gameService');

function getBearerToken(request) {
  const header = request.headers.authorization || '';
  const prefix = 'Bearer ';
  return header.startsWith(prefix) ? header.slice(prefix.length).trim() : '';
}

function asyncRoute(handler) {
  return async (request, response, next) => {
    try {
      await handler(request, response);
    } catch (error) {
      next(error);
    }
  };
}

function createApp({ config, gameService, logger = console } = {}) {
  const app = express();
  const publicDir = path.join(__dirname, '../../public');

  app.disable('x-powered-by');
  app.set('env', config?.NODE_ENV || process.env.NODE_ENV || 'development');
  app.use(express.json());
  app.use(express.static(publicDir));

  app.get('/', (_request, response) => {
    response.sendFile(path.join(publicDir, 'index.html'));
  });

  app.get('/student', (_request, response) => {
    response.sendFile(path.join(publicDir, 'student.html'));
  });

  app.get('/instructor', (_request, response) => {
    response.sendFile(path.join(publicDir, 'instructor.html'));
  });

  app.get(
    '/healthz',
    asyncRoute(async (_request, response) => {
      const health = await gameService.healthCheck();
      response.status(200).json(health);
    })
  );

  async function authenticate(request, _response, next) {
    try {
      request.principal = await gameService.authenticateParticipant(getBearerToken(request));
      next();
    } catch (error) {
      next(error);
    }
  }

  function requireBootstrapSecret(request, _response, next) {
    if (!config.BOOTSTRAP_ADMIN_SECRET) {
      next();
      return;
    }

    const provided = request.headers['x-admin-secret'];
    if (provided !== config.BOOTSTRAP_ADMIN_SECRET) {
      next(new HttpError(401, 'INVALID_BOOTSTRAP_SECRET', 'A valid x-admin-secret header is required.'));
      return;
    }

    next();
  }

  function requireInstructor(request, _response, next) {
    if (request.principal.role !== 'INSTRUCTOR') {
      next(new HttpError(403, 'FORBIDDEN', 'Only the instructor can use this endpoint.'));
      return;
    }

    next();
  }

  function publish(events) {
    app.locals.websocketHub?.publishMany(events || []);
  }

  app.post(
    '/api/sessions',
    requireBootstrapSecret,
    asyncRoute(async (request, response) => {
      const result = await gameService.createGameSession({
        sessionName: request.body?.sessionName,
        referencePriceCents: request.body?.referencePriceCents
      });

      publish(result.events);
      response.status(201).json({
        session: result.session,
        instructor: result.instructor
      });
    })
  );

  app.post(
    '/api/sessions/reset',
    requireBootstrapSecret,
    asyncRoute(async (request, response) => {
      const result = await gameService.resetGameSession({
        sessionName: request.body?.sessionName,
        referencePriceCents: request.body?.referencePriceCents
      });

      publish(result.events);
      response.status(201).json({
        session: result.session,
        instructor: result.instructor
      });
    })
  );

  app.post(
    '/api/sessions/join',
    asyncRoute(async (request, response) => {
      const result = await gameService.joinSession({
        joinCode: request.body?.joinCode,
        displayName: request.body?.displayName
      });

      response.status(201).json(result);
    })
  );

  app.get(
    '/api/me',
    authenticate,
    asyncRoute(async (request, response) => {
      const [participantSummary, peekAvailability] = await Promise.all([
        gameService.getParticipantSummary(request.principal.participantId, request.principal.sessionId),
        gameService.getPeekAvailability()
      ]);

      response.status(200).json({
        principal: request.principal,
        participant: participantSummary.participant,
        features: {
          peeks: peekAvailability
        }
      });
    })
  );

  app.patch(
    '/api/sessions/:sessionId/state',
    authenticate,
    requireInstructor,
    asyncRoute(async (request, response) => {
      const sessionId = Number.parseInt(request.params.sessionId, 10);
      if (sessionId !== request.principal.sessionId) {
        throw new HttpError(403, 'FORBIDDEN', 'The instructor token does not belong to this session.');
      }

      const result = await gameService.setSessionState({
        sessionId,
        actorParticipantId: request.principal.participantId,
        status: request.body?.status
      });

      publish(result.events);
      response.status(200).json(result);
    })
  );

  app.post(
    '/api/sessions/:sessionId/finalize',
    authenticate,
    requireInstructor,
    asyncRoute(async (request, response) => {
      const sessionId = Number.parseInt(request.params.sessionId, 10);
      if (sessionId !== request.principal.sessionId) {
        throw new HttpError(403, 'FORBIDDEN', 'The instructor token does not belong to this session.');
      }

      const result = await gameService.finalizeSession({
        sessionId,
        actorParticipantId: request.principal.participantId
      });

      publish(result.events);
      response.status(200).json(result);
    })
  );

  app.get(
    '/api/instructor/dashboard',
    authenticate,
    requireInstructor,
    asyncRoute(async (request, response) => {
      const connectedParticipants = app.locals.websocketHub?.getPresenceSnapshot(request.principal.sessionId) || [];
      const dashboard = await gameService.getInstructorDashboard(
        request.principal.sessionId,
        connectedParticipants.map((entry) => entry.participantId)
      );

      response.status(200).json(dashboard);
    })
  );

  app.post(
    '/api/orders',
    authenticate,
    asyncRoute(async (request, response) => {
      if (request.principal.role !== 'STUDENT') {
        throw new HttpError(403, 'FORBIDDEN', 'Only students can submit orders.');
      }

      const idempotencyKey = request.headers['idempotency-key'];
      const result = await gameService.submitOrder({
        sessionId: request.principal.sessionId,
        participantId: request.principal.participantId,
        idempotencyKey,
        side: request.body?.side,
        orderType: request.body?.orderType,
        quantity: request.body?.quantity,
        limitPriceCents: request.body?.limitPriceCents
      });

      publish(result.events);
      response.status(200).json({
        idempotencyHit: result.idempotencyHit,
        order: result.order,
        fills: result.fills
      });
    })
  );

  app.post(
    '/api/orders/:orderId/cancel',
    authenticate,
    asyncRoute(async (request, response) => {
      if (request.principal.role !== 'STUDENT') {
        throw new HttpError(403, 'FORBIDDEN', 'Only students can cancel orders.');
      }

      const result = await gameService.cancelOrder({
        sessionId: request.principal.sessionId,
        participantId: request.principal.participantId,
        orderId: Number.parseInt(request.params.orderId, 10)
      });

      publish(result.events);
      response.status(200).json({
        order: result.order,
        fills: result.fills
      });
    })
  );

  app.post(
    '/api/peeks',
    authenticate,
    asyncRoute(async (request, response) => {
      if (request.principal.role !== 'STUDENT') {
        throw new HttpError(403, 'FORBIDDEN', 'Only students can buy private peeks.');
      }

      const result = await gameService.purchasePeek({
        sessionId: request.principal.sessionId,
        participantId: request.principal.participantId
      });

      publish(result.events);
      response.status(201).json({
        peek: result.peek,
        account: result.account
      });
    })
  );

  app.get(
    '/api/order-book',
    authenticate,
    asyncRoute(async (request, response) => {
      const snapshot = await gameService.getOrderBookSnapshot(request.principal.sessionId);
      response.status(200).json(snapshot);
    })
  );

  app.get(
    '/api/me/account',
    authenticate,
    asyncRoute(async (request, response) => {
      const accountState = await gameService.getPlayerAccountState(
        request.principal.participantId,
        request.principal.sessionId
      );

      response.status(200).json(accountState);
    })
  );

  app.get(
    '/api/leaderboard',
    authenticate,
    asyncRoute(async (request, response) => {
      const leaderboard = await gameService.getLeaderboard(request.principal.sessionId);
      response.status(200).json(leaderboard);
    })
  );

  app.use((request, response) => {
    response.status(404).json({
      error: {
        code: 'NOT_FOUND',
        message: `No route matches ${request.method} ${request.path}.`
      }
    });
  });

  app.use((error, _request, response, _next) => {
    const translated = translateError(error);
    if (translated.status >= 500) {
      logger.error?.(translated);
    }

    response.status(translated.status).json({
      error: {
        code: translated.code,
        message: translated.message,
        details: translated.details
      }
    });
  });

  return app;
}

module.exports = {
  createApp
};
