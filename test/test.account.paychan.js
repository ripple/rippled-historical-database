'use strict'

var config = require('../config')
var request = require('request')
var assert = require('assert')
var utils = require('./utils')
var port = config.get('port') || 7111

describe('account payment channels API endpoint', function() {

  it('should get account payment channels', function(done) {
    var account = 'rnNzy3iPc7gPEAJbAdXwxY1UTBamBqTYhR'
    var url = 'http://localhost:' + port +
      '/v2/accounts/' + account + '/payment_channels'
    request({
      url: url,
      json: true
    },
    function(err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 200)
      assert.strictEqual(body.rows.length, 2)
      body.rows.forEach(function(d) {
        assert.strictEqual(d.account, account)
      })
      done()
    })
  })

  it('should filter by destination', function(done) {
    var account = 'rUeXUxaMTH1pELvD2EkiHTRcM9FsH3v4d7'
    var url = 'http://localhost:' + port +
      '/v2/accounts/' + account + '/payment_channels'
    request({
      url: url,
      json: true
    },
    function(err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 200)
      assert.strictEqual(body.rows.length, 0)
      body.rows.forEach(function(d) {
        assert.strictEqual(d.account, account)
      })
      done()
    })
  })


  it('handle pagination', function(done) {
    var url = 'http://localhost:' + port +
        '/v2/accounts/rnNzy3iPc7gPEAJbAdXwxY1UTBamBqTYhR/payment_channels?'

    utils.checkPagination(url, undefined, function(ref, i, body) {
      assert.strictEqual(body.rows.length, 1)
      assert.equal(body.rows[0].amount, ref.rows[i].amount)
      assert.equal(body.rows[0].tx_hash, ref.rows[i].tx_hash)
    }, done)
  })

  it('handle pagination (descending false)', function(done) {
    var url = 'http://localhost:' + port +
        '/v2/accounts/rnNzy3iPc7gPEAJbAdXwxY1UTBamBqTYhR/payment_channels?'

    utils.checkPagination(url, undefined, function(ref, i, body) {
      assert.strictEqual(body.rows.length, 1)
      assert.equal(body.rows[0].amount, ref.rows[i].amount)
      assert.equal(body.rows[0].tx_hash, ref.rows[i].tx_hash)
    }, done)
  })

  it('handles empty response', function(done) {
    var url = 'http://localhost:' + port +
        '/v2/accounts/rrrrUBy92h6worVCYERZcVCzgzgmHb17Dx/payment_channels'
    request({
      url: url,
      json: true
    },
    function(err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 200)
      assert.strictEqual(body.rows.length, 0)
      assert.strictEqual(body.count, 0)
      done()
    })
  })

  it('include a link header when marker is present', function(done) {
    var url = 'http://localhost:' + port +
        '/v2/accounts/rnNzy3iPc7gPEAJbAdXwxY1UTBamBqTYhR' +
        '/payment_channels?limit=1'
    var linkHeader = '<' + url +
      '&marker=rnNzy3iPc7gPEAJbAdXwxY1UTBamBqTYhR' +
      '|20170512144852|000029709909|00038>; rel="next"'

    request({
      url: url,
      json: true
    },
    function(err, res) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 200)
      assert.strictEqual(res.headers.link, linkHeader)
      done()
    })
  })
})
