const config = require('../config')
const limits = config.get('rateLimit')
const Limiter = require('ratelimiter')
const redis = require('redis').createClient({
  host: config.get('redis:host'),
  port: config.get('redis:port')
})

if (limits) {
  console.log('RATE LIMITS: ' +
    limits.max + ' requests / ' +
    (limits.duration/1000 | 0) +
    ' seconds')
}

redis.on('error', (e) => {
  console.log(e)
})

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

  if (!limits || !ip || !redis.connected) {
    return next()
  }

  const limit = new Limiter({
    max: limits.max || 60,
    duration: limits.duration || 60000,
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