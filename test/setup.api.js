/* eslint no-unused-vars:0 */
'use strict'
var config = require('../config')
config.file('defaults', __dirname + '/test_config.json')

var Server = require('../api/server')
var assert = require('assert')
var request = require('request')
var port = config.get('port') || 7112
var server


server = new Server({
  postgres: undefined,
  port: port,
  cacheControl: {
    'max-age': 10,
    'stale-while-revalidate': 30,
    'stale-if-error': 300
  }
})

describe('API tests', function(done) {  
  it('should handle duplicate query params', function(done) {
    var url = 'http://localhost:' + port + '/v2/accounts/' +
        'rpjZUBy92h6worVCYERZcVCzgzgmHb17Dx/payments?type=sent&type=sent'
    request({
      url: url,
      json: true
    },
    function(err, res) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 200)
      done()
    })
  })

  it('should set default cache control', function(done) {
    var url = 'http://localhost:' + port + '/v2/accounts'
    request({
      url: url,
      json: true
    },
    function(err, res) {
      console.log(url, err)
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 200)
      assert.strictEqual(res.headers['cache-control'],
        'max-age=10, stale-while-revalidate=30, stale-if-error=300, ')
      done()
    })
  })
})
