
const config = require('../config')
const assert = require('assert')
const request = require('request-promise')
const port = config.get('port') || 7112

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
    .then(assert)
    .catch(err => {
      return doRequest('different-ip')
    })
  })
})