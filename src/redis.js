// Redis connection factory. Pub/sub needs (at least) two connections: a subscriber connection
// cannot issue normal commands while subscribed, so publishers/queries use a separate one.
import Redis from 'ioredis';
import { config } from './config.js';

export function makeRedis(role = 'client') {
  const client = new Redis(config.redisUrl, {
    // Keep retrying forever during a long benchmark instead of throwing on a transient blip.
    maxRetriesPerRequest: null,
    enableReadyCheck: true,
    connectionName: `${config.instanceId}:${role}`,
  });
  client.on('error', (err) => {
    // Surface but don't crash the run on a transient hiccup.
    console.error(`[redis:${role}] ${err.message}`);
  });
  return client;
}
