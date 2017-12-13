'use strict'

var config = require('../config')
var request = require('request')
var assert = require('assert')
var moment = require('moment')
var utils = require('./utils')
var port = config.get('port') || 7111

describe('payments API endpoint', function() {
  it('should should get individual payments', function(done) {
    var account = 'rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B'
    var url = 'http://localhost:' + port + '/v2/payments'
    var last = 0

    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 200)
      assert.strictEqual(typeof body, 'object')
      assert.strictEqual(body.result, 'success')
      assert.strictEqual(body.payments.length, body.count)
      assert.strictEqual(body.payments.length, 182)
      done()
    })
  })

  it('should should get individual payments by date', function(done) {
    var url = 'http://localhost:' + port + '/v2/payments'
    var start = moment.utc('2015-01-14T18:28:40')
    var end = moment.utc('2015-01-14T18:51:40')
    request({
      url: url,
      json: true,
      qs: {
        start: start.format(),
        end: end.format(),
      }
    },
    function (err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 200)
      assert.strictEqual(typeof body, 'object')
      assert.strictEqual(body.result, 'success')
      assert.strictEqual(body.payments.length, body.count)
      assert.strictEqual(body.payments.length, 37)
      body.payments.forEach(function(p) {
        assert(start.diff(moment.utc(p.executed_time))<=0, 'executed time less than start time')
        assert(end.diff(moment.utc(p.executed_time))>=0, 'executed time greater than end time')
      })
      done()
    })
  })

  it('should should get exchanges in descending order', function(done) {
    var account = 'rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B'
    var url = 'http://localhost:' + port + '/v2/payments'
    var last = Infinity

    request({
      url: url,
      json: true,
      qs: {
        descending: true
      }
    },
    function (err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 200)
      assert.strictEqual(typeof body, 'object')
      assert.strictEqual(body.result, 'success')
      assert.strictEqual(body.payments.length, body.count)
      body.payments.forEach(function(p) {
        assert(last >= p.ledger_index)
        last = p.ledger_index
      })
      done()
    })
  })

  it('should get payments by currency+issuer', function(done) {
    var issuer = 'rKiCet8SdvWxPXnAgYarFUXMh1zCPz432Y'
    var currency = 'CNY'
    var url = 'http://localhost:' + port + '/v2/payments/' +
        currency + '+' + issuer

    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 200)
      assert.strictEqual(typeof body, 'object')
      assert.strictEqual(body.result, 'success')
      assert.strictEqual(body.payments.length, body.count)
      assert.strictEqual(body.payments.length, 37)
      done()
    })
  })

  it('should make sure exchanges handles pagination correctly', function(done) {
    var url = 'http://localhost:' + port + '/v2/payments/CNY+rKiCet8SdvWxPXnAgYarFUXMh1zCPz432Y?'
    utils.checkPagination(url, undefined, function(ref, i, body) {
      assert.strictEqual(body.payments.length, 1)
      assert.equal(body.payments[0].amount, ref.payments[i].amount)
      assert.equal(body.payments[0].tx_hash, ref.payments[i].tx_hash)
    }, done)
  })

  //there will not be any aggregates to be found
  it('should should get aggregate payments', function(done) {
    var url = 'http://localhost:' + port + '/v2/payments/XRP'

    request({
      url: url,
      json: true,
      qs: {
        interval : 'day',
      }
    },
    function (err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 200)
      assert.strictEqual(typeof body, 'object')
      assert.strictEqual(body.result, 'success')
      assert.strictEqual(body.payments.length, 4)
      assert.strictEqual(body.payments[0].count, 12)
      done()
    })
  })


  it('should return an error for an invalid start date', function(done) {
    var url = 'http://localhost:' + port + '/v2/payments'

    request({
      url: url,
      json: true,
      qs: {
        start: '2015x',
      }
    },
    function (err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 400)
      assert.strictEqual(typeof body, 'object')
      assert.strictEqual(body.result, 'error')
      assert.strictEqual(body.message, 'invalid start date format')
      done()
    })
  })

  it('should return an error for an invalid end time', function(done) {
    var url = 'http://localhost:' + port + '/v2/payments'

    request({
      url: url,
      json: true,
      qs: {
        end: '2015x',
      }
    },
    function (err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 400)
      assert.strictEqual(typeof body, 'object')
      assert.strictEqual(body.result, 'error')
      assert.strictEqual(body.message, 'invalid end date format')
      done()
    })
  })


  it('should return an error for an invalid interval', function(done) {
    var url = 'http://localhost:' + port + '/v2/payments'

    request({
      url: url,
      json: true,
      qs: {
        interval: '3weeks',
      }
    },
    function (err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 400)
      assert.strictEqual(typeof body, 'object')
      assert.strictEqual(body.result, 'error')
      assert.strictEqual(body.message, 'invalid interval')
      done()
    })
  })

  it('should return an error for missing issuer', function(done) {
    var url = 'http://localhost:' + port + '/v2/payments/USD'

    request({
      url: url,
      json: true
    },
    function (err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 400)
      assert.strictEqual(typeof body, 'object')
      assert.strictEqual(body.result, 'error')
      assert.strictEqual(body.message, 'issuer is required')
      done()
    })
  })

  it('should include a link header when marker is present', function(done) {
    var url = 'http://localhost:' + port + '/v2/payments?limit=1'
    var linkHeader = '<' + url +
      '&marker=20130611200120|000001021029|00001>; rel="next"'

    request({
      url: url,
      json: true
    },
    function (err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 200)
      assert.strictEqual(res.headers.link, linkHeader)
      done()
    })
  })
})
