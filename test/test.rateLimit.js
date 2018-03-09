const config = require('../config')
config.file('defaults', __dirname + '/test_config.json')

const assert = require('assert')
const request = require('request-promise')
const Server = require('../api/server')
const hbase = require('../lib/hbase');
const port = 7222
const server = new Server({ port })

function doRequest(id, full) {
  return request({
    url: 'http://localhost:' + port + '/v2/gateways',
    json: true,
    resolveWithFullResponse: full,
    headers: {
      'fastly-client-ip': id || 'ip'
    }
  })
}

describe('rate limit', function() {
  it('setup mock server', function() {
    return hbase.putRow({
      table: 'control',
      rowkey: 'rate_limit',
      columns: {
        max: 5,
        duration: 1000,
        "whitelist": ["0.0.0.0","1.1.1.1"],
        "blacklist": ["1.1.1.1","2.2.2.2"]
      }
    }).then(() => {
        require('../lib/rateLimit').updateConfig();
    })
  })

  it('succeed if limit not reached', function() {
    return doRequest(undefined, true)
    .then(resp => {
      assert.strictEqual(resp.headers['x-ratelimit-limit'], '5')
      assert.strictEqual(resp.headers['x-ratelimit-remaining'], '4')
      assert(!isNaN(resp.headers['x-ratelimit-reset']))
    })
  })

  it('return 429 when exceeding rate limit', function(done) {
    const tasks = []
    let i = 7

    while(i--) {
      tasks.push(doRequest())
    }

    return Promise.all(tasks)
    .then(resp => {
      done(Error('should fail query'))
    })
    .catch(err => {
      assert.strictEqual(err.statusCode, 429)
      assert.strictEqual(err.error.error.substring(0, 19),
                         'Rate limit exceeded')
      done()
    })
  })

  it('succeed after limit is cleared', function(done) {
    this.timeout(3000)

    setTimeout(() => {
      doRequest()
      .then(() => {
        done()
      })
      .catch(err => {
        assert(err.error)
      })
    }, 2000)
  })

  it('ensure rate limit is by IP only', function() {
    const tasks = []
    let i = 7

    while(i--) {
      tasks.push(doRequest())
    }

    return Promise.all(tasks)
    .then(Promise.reject)
    .catch(err => {
      return doRequest('different-ip')
    })
  })

  it('blacklist by IP', function() {
    return doRequest('2.2.2.2')
    .then(Promise.reject)
    .catch(err => {
      assert.strictEqual(err.error.error, 'This IP has been banned');
    })
  })

  it('whitelist by IP', function() {
    return doRequest('0.0.0.0', true)
    .then(resp => {
      assert.strictEqual(resp.headers['x-ratelimit-remaining'], undefined)
    })
  })

  it('prioritize blacklist over whitelist', function() {
    return doRequest('1.1.1.1')
    .then(Promise.reject)
    .catch(err => {
      assert.deepEqual(err.error, { error: 'This IP has been banned'});
    })
  })
})