const { createRuntime } = require('./runtime');
const { loadConfig } = require('./config');

async function main() {
  const config = loadConfig();
  if (!config.DATABASE_URL) {
    throw new Error('DATABASE_URL is required to start the server.');
  }

  const runtime = await createRuntime({ config });
  const address = await runtime.start();

  console.log(
    JSON.stringify({
      level: 'info',
      message: 'Server started',
      host: address.host,
      port: address.port,
      restoredState: runtime.restoredState
    })
  );

  const shutdown = async () => {
    try {
      await runtime.stop();
      process.exit(0);
    } catch (error) {
      console.error(error);
      process.exit(1);
    }
  };

  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
