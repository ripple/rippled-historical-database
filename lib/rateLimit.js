const config = require('../config');
const hbase = require('./hbase');
const Logger = require('./logger');
const log = new Logger({
  scope: 'rate-limit',
  file: config.get('logFile'),
  level: config.get('logLevel')
});
const Limiter = require('ratelimiter')
const UPDATE_INTERVAL = 10 * 60 * 1000;
const limits = {}

let redis;

if (config.get('rateLimit')) {
  redis = require('redis').createClient({
    host: config.get('redis:host'),
    port: config.get('redis:port')
  })

  redis.on('error', (e) => {
    log.error(e)
  })
}

/**
 * updateConfig
 */

function updateConfig() {
  hbase.getRow({
    table: 'control',
    rowkey: 'rate_limit'
  }, function(err, resp) {
    if (err) {
      log.error(err);
      return;
    }

    if (resp && resp.max && resp.duration) {
      limits.max = resp.max;
      limits.duration = resp.duration;
      limits.whitelist = [];
      limits.blacklist = [];

      try {
        limits.whitelist = resp.whitelist ? JSON.parse(resp.whitelist) : [];
        limits.blacklist = resp.blacklist ? JSON.parse(resp.blacklist) : [];
      } catch(e) {
        log.error(e);
      }

      log.info(limits.max +
        ' requests / ' +
        (limits.duration/1000 | 0) +
        ' seconds - whitelist:' +
        limits.whitelist.length +
        ' blacklist:' +
        limits.blacklist.length);

    } else {
      limits.max = undefined
      log.info('no rate limits set')
    }
  })
}

// periodically check for config changes
if (config.get('rateLimit')) {
  setInterval(updateConfig, UPDATE_INTERVAL);
  updateConfig();
}

module.exports.updateConfig = updateConfig;
module.exports.getLimiter = function(options) {
  return new Limiter({
    max: options.max,
    duration: options.duration,
    id: options.id,
    db: redis
  });
}

module.exports.middleware = function(req, res, next) {

  const ip = req.headers['fastly-client-ip'];

  if (!limits.max || !ip || !redis || !redis.connected) {
    return next();
  }

  if (limits.blacklist.includes(ip)) {
    return res.status(403).send({
      error: 'This IP has been banned'
    });
  }

  if (limits.whitelist.includes(ip)) {
    return next();
  }

  const limit = new Limiter({
    max: limits.max,
    duration: limits.duration,
    id: ip,
    db: redis
  });

  limit.get((err, resp) => {

    if (err) {
      return next(err);
    }

    res.set('X-RateLimit-Limit', resp.total);
    res.set('X-RateLimit-Remaining', resp.remaining - 1);
    res.set('X-RateLimit-Reset', resp.reset);

    if (resp.remaining) {
      return next();
    }

    const after = resp.reset - Date.now() / 1000;

    res.set('Retry-After', after | 0);
    res.status(429).send({
      error: 'Rate limit exceeded, retry in ' + after.toFixed(2) + 'sec'
    })
  })
}