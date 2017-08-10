'use strict'

var config = require('../config')
var request = require('request')
var assert = require('assert')
var utils = require('./utils')
var port = config.get('port') || 7111

describe('account escrows API endpoint', function() {

  it('should get account escrows', function(done) {
    var account = 'rGhDCgik9CwiNpcNnYHkEHcMgw2dkLgtNB'
    var url = 'http://localhost:' + port +
      '/v2/accounts/' + account + '/escrows'
    request({
      url: url,
      json: true
    },
    function(err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 200)
      assert.strictEqual(body.escrows.length, 1)
      body.escrows.forEach(function(d) {
        assert.strictEqual(d.account, account)
      })
      done()
    })
  })

  it('should filter by destination', function(done) {
    var account = 'rUeXUxaMTH1pELvD2EkiHTRcM9FsH3v4d7'
    var url = 'http://localhost:' + port +
      '/v2/accounts/' + account + '/escrows'
    request({
      url: url,
      json: true
    },
    function(err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 200)
      assert.strictEqual(body.escrows.length, 0)
      body.escrows.forEach(function(d) {
        assert.strictEqual(d.account, account)
      })
      done()
    })
  })


  it.skip('handle pagination', function(done) {
    var url = 'http://localhost:' + port +
        '/v2/accounts/rGhDCgik9CwiNpcNnYHkEHcMgw2dkLgtNB/escrows?'

    utils.checkPagination(url, undefined, function(ref, i, body) {
      assert.strictEqual(body.escrows.length, 1)
      assert.equal(body.escrows[0].amount, ref.escrows[i].amount)
      assert.equal(body.escrows[0].tx_hash, ref.escrows[i].tx_hash)
    }, done)
  })

  it.skip('handle pagination (descending false)', function(done) {
    var url = 'http://localhost:' + port +
        '/v2/accounts/rGhDCgik9CwiNpcNnYHkEHcMgw2dkLgtNB/escrows?'

    utils.checkPagination(url, undefined, function(ref, i, body) {
      assert.strictEqual(body.escrows.length, 1)
      assert.equal(body.escrows[0].amount, ref.escrows[i].amount)
      assert.equal(body.escrows[0].tx_hash, ref.escrows[i].tx_hash)
    }, done)
  })

  it('handles empty response', function(done) {
    var url = 'http://localhost:' + port +
        '/v2/accounts/rrrrUBy92h6worVCYERZcVCzgzgmHb17Dx/escrows'
    request({
      url: url,
      json: true
    },
    function(err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 200)
      assert.strictEqual(body.escrows.length, 0)
      assert.strictEqual(body.count, 0)
      done()
    })
  })

  it.skip('include a link header when marker is present', function(done) {
    var url = 'http://localhost:' + port +
        '/v2/accounts/rGhDCgik9CwiNpcNnYHkEHcMgw2dkLgtNB/escrows?limit=1'
    var linkHeader = '<' + url +
      '&marker=rpjZUBy92h6worVCYERZcVCzgzgmHb17Dx' +
      '|20150114185210|000011119940|00001> rel="next"'

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
