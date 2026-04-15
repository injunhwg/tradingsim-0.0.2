const { WebSocket, WebSocketServer } = require('ws');

class WebSocketHub {
  constructor({ server, gameService, path = '/ws', logger = console } = {}) {
    if (!server) {
      throw new Error('server is required');
    }

    if (!gameService) {
      throw new Error('gameService is required');
    }

    this.server = server;
    this.gameService = gameService;
    this.path = path;
    this.logger = logger;
    this.connections = new Set();
    this.wss = new WebSocketServer({ noServer: true });

    this.server.on('upgrade', (request, socket, head) => {
      void this.#handleUpgrade(request, socket, head);
    });

    this.wss.on('connection', (socket, request, principal) => {
      this.#registerConnection(socket, principal);
      void this.#sendBootstrap(socket, principal);

      socket.on('close', () => {
        this.connections.delete(socket);
      });
    });
  }

  publish(event) {
    const serialized = JSON.stringify(event);

    for (const socket of this.connections) {
      if (socket.readyState !== WebSocket.OPEN) {
        continue;
      }

      const context = socket.__context;
      if (!context || context.sessionId !== event.sessionId) {
        continue;
      }

      if (event.scope === 'PRIVATE' && context.participantId !== event.participantId) {
        continue;
      }

      socket.send(serialized);
    }
  }

  publishMany(events) {
    for (const event of events || []) {
      this.publish(event);
    }
  }

  getPresenceSnapshot(sessionId) {
    const connectedParticipants = [];

    for (const socket of this.connections) {
      const context = socket.__context;
      if (!context || context.sessionId !== sessionId || socket.readyState !== WebSocket.OPEN) {
        continue;
      }

      connectedParticipants.push({
        participantId: context.participantId,
        role: context.role
      });
    }

    return connectedParticipants;
  }

  async close() {
    for (const socket of this.connections) {
      socket.close();
    }

    await new Promise((resolve) => {
      this.wss.close(resolve);
    });
  }

  async #handleUpgrade(request, socket, head) {
    try {
      const url = new URL(request.url, 'http://localhost');
      if (url.pathname !== this.path) {
        socket.destroy();
        return;
      }

      const token = url.searchParams.get('token');
      const principal = await this.gameService.authenticateParticipant(token);

      this.wss.handleUpgrade(request, socket, head, (ws) => {
        this.wss.emit('connection', ws, request, principal);
      });
    } catch (error) {
      this.logger.warn?.('WebSocket authentication failed', error);
      socket.write('HTTP/1.1 401 Unauthorized\r\n\r\n');
      socket.destroy();
    }
  }

  #registerConnection(socket, principal) {
    socket.__context = {
      participantId: principal.participantId,
      sessionId: principal.sessionId,
      role: principal.role
    };

    this.connections.add(socket);
  }

  async #sendBootstrap(socket, principal) {
    try {
      const bootstrap = await this.gameService.getRealtimeBootstrap(principal);
      const events = [bootstrap.ready, bootstrap.session, bootstrap.orderBook, bootstrap.account, bootstrap.leaderboard].filter(Boolean);

      for (const event of events) {
        if (socket.readyState === WebSocket.OPEN) {
          socket.send(JSON.stringify(event));
        }
      }
    } catch (error) {
      this.logger.error?.('Failed to send WebSocket bootstrap', error);
      socket.close(1011, 'bootstrap_failed');
    }
  }
}

module.exports = {
  WebSocketHub
};
