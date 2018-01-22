'use strict'

const config = require('../config')
const request = require('request')
const assert = require('assert')
const moment = require('moment')
const utils = require('./utils')
const hbase = require('../lib/hbase')
const port = config.get('port') || 7111

const XRPIndex = require('./mock/xrp-index.json')
const aggXRPIndex = require('./mock/agg-xrp-index.json')
const XRPIndexTicker = require('./mock/xrp-index-ticker.json')
const date = moment.utc().format('YYYYMMDDHHmmss')
const forex = {}

forex['USD|CNY|' + date] = {
  base: 'USD',
  counter: 'CNY',
  rate: '6.653197',
  date: moment.utc().format()
}


describe('setup mock data', function() {
  it('load data into hbase', function(done) {
    Promise.all([
      hbase.putRows({
        table: 'xrp_index',
        rows: XRPIndex
      }),
      hbase.putRows({
        table: 'agg_xrp_index',
        rows: aggXRPIndex
      }),
      hbase.putRows({
        table: 'agg_xrp_index',
        rows: XRPIndexTicker
      }),
      hbase.putRows({
        table: 'forex_rates',
        rows: forex
      })
    ])
    .then(() => {
      done()
    })
    .catch(err => {
      assert.ifError(err)
    })
  })
})

describe.skip('XRP Index API endpoint', function() {
  it('should should get XRP index data', function(done) {
    const url = 'http://localhost:' + port + '/v2/xrp_index'
    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 200)
      assert.strictEqual(typeof body, 'object')
      assert.strictEqual(body.result, 'success')
      assert.strictEqual(body.rows.length, 5)
      body.rows.forEach(row => {
        assert(!isNaN(Number(row.price)))
        assert(!isNaN(Number(row.volume)))
        assert(!isNaN(Number(row.counter_volume)))
        assert(!isNaN(row.count))
        assert(moment(row.date).isValid())
      })
      done()
    })
  })

  it('should should get XRP index data with currency', function(done) {
    const url = 'http://localhost:' + port + '/v2/xrp_index'
    request({
      url: url,
      json: true,
      qs: {
        currency: 'CNY'
      }
    },
    function (err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 200)
      assert.strictEqual(typeof body, 'object')
      assert.strictEqual(body.result, 'success')
      assert.strictEqual(body.rows.length, 5)
      assert.strictEqual(body.fx_rate, '6.653197')
      body.rows.forEach(row => {
        assert(!isNaN(Number(row.price)))
        assert(!isNaN(Number(row.volume)))
        assert(!isNaN(Number(row.counter_volume)))
        assert(!isNaN(row.count))
        assert(moment(row.date).isValid())
      })
      done()
    })
  })

  it('should should get XRP index data within date range', function(done) {
    const url = 'http://localhost:' + port + '/v2/xrp_index'
    const start = '2017-11-07T17:14:59Z'
    const end = '2017-11-07T17:17:00Z'
    request({
      url: url,
      json: true,
      qs: {
        start: start,
        end: end
      }
    },
    function (err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 200)
      assert.strictEqual(typeof body, 'object')
      assert.strictEqual(body.result, 'success')
      assert.strictEqual(body.rows.length, 3)
      body.rows.forEach(row => {
        assert(!isNaN(Number(row.price)))
        assert(!isNaN(Number(row.volume)))
        assert(!isNaN(Number(row.counter_volume)))
        assert(!isNaN(row.count))
        assert(moment(row.date).isValid())
        assert(moment(row.date).diff(start) > 0, 'date less than start')
        assert(moment(row.date).diff(end) <= 0, 'date greater than end')
      })
      done()
    })
  })

  it('should should get aggregated XRP index data', function(done) {
    const url = 'http://localhost:' + port + '/v2/xrp_index'

    request({
      url: url,
      json: true,
      qs: {
        interval: '5minute',
      }
    },
    function (err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 200)
      assert.strictEqual(typeof body, 'object')
      assert.strictEqual(body.result, 'success')
      assert.strictEqual(body.rows.length, 49)
      body.rows.forEach(row => {
        assert(!isNaN(Number(row.open)))
        assert(!isNaN(Number(row.high)))
        assert(!isNaN(Number(row.low)))
        assert(!isNaN(Number(row.close)))
        assert(!isNaN(Number(row.vwap)))
        assert(!isNaN(Number(row.volume)))
        assert(!isNaN(Number(row.counter_volume)))
        assert(!isNaN(row.count))
        assert(moment(row.date).isValid())
      })
      done()
    })
  })

  it('should should get aggregated XRP index data with currency', function(done) {
    const url = 'http://localhost:' + port + '/v2/xrp_index'

    request({
      url: url,
      json: true,
      qs: {
        interval: '5minute',
        currency: 'CNY'
      }
    },
    function (err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 200)
      assert.strictEqual(typeof body, 'object')
      assert.strictEqual(body.result, 'success')
      assert.strictEqual(body.rows.length, 49)
      assert.strictEqual(body.fx_rate, '6.653197')
      body.rows.forEach(row => {
        assert(!isNaN(Number(row.open)))
        assert(!isNaN(Number(row.high)))
        assert(!isNaN(Number(row.low)))
        assert(!isNaN(Number(row.close)))
        assert(!isNaN(Number(row.vwap)))
        assert(!isNaN(Number(row.volume)))
        assert(!isNaN(Number(row.counter_volume)))
        assert(!isNaN(row.count))
        assert(moment(row.date).isValid())
      })
      done()
    })
  })

  it('should handle pagination', function(done) {
    const url = 'http://localhost:' + port + '/v2/xrp_index?descending=true'

    utils.checkPagination(url, undefined, function(ref, i, body) {
      assert.strictEqual(body.rows.length, 1)
      assert.equal(body.rows[0].date, ref.rows[i].date)
    }, done)
  })

  it('should include a link header when marker is present', function(done) {
    var url = 'http://localhost:' + port + '/v2/xrp_index?limit=1'
    var linkHeader = '<' + url +
      '&marker=79828892828299>; rel="next"'

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

  it('should error on invalid start date', function(done) {
    const url = 'http://localhost:' + port + '/v2/xrp_index'

    request({
      url: url,
      json: true,
      qs: {
        interval: '5minute',
        start: '2015x'
      }
    },
    function (err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 400)
      assert.strictEqual(body.result, 'error')
      assert.strictEqual(body.message, 'invalid start date format')
      done()
    })
  })

  it('should error on invalid end date', function(done) {
    const url = 'http://localhost:' + port + '/v2/xrp_index'

    request({
      url: url,
      json: true,
      qs: {
        interval: '5minute',
        end: '2015x'
      }
    },
    function (err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 400)
      assert.strictEqual(body.result, 'error')
      assert.strictEqual(body.message, 'invalid end date format')
      done()
    })
  })

  it('should error on invalid interval', function(done) {
    const url = 'http://localhost:' + port + '/v2/xrp_index'

    request({
      url: url,
      json: true,
      qs: {
        interval: '50minute'
      }
    },
    function (err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 400)
      assert.strictEqual(body.result, 'error')
      assert.strictEqual(body.message, 'invalid interval')
      done()
    })
  })

  it('should error on invalid currency', function(done) {
    const url = 'http://localhost:' + port + '/v2/xrp_index'

    request({
      url: url,
      json: true,
      qs: {
        currency: 'BLAH'
      }
    },
    function (err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 400)
      assert.strictEqual(body.result, 'error')
      assert.strictEqual(body.message, 'exchange rate unavailable')
      done()
    })
  })
})


describe.skip('XRP Index Ticker API endpoint', function() {
  it('should should get XRP index data', function(done) {
    const url = 'http://localhost:' + port + '/v2/xrp_index/ticker'
    const fields = [
      'result',
      '1hour',
      '1day',
      '3day',
      '7day',
      '30day'
    ]
    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 200)
      assert.strictEqual(typeof body, 'object')
      assert.strictEqual(typeof body['1hour'], 'object')
      assert.strictEqual(typeof body['1day'], 'object')
      assert.strictEqual(typeof body['3day'], 'object')
      assert.strictEqual(typeof body['7day'], 'object')
      assert.strictEqual(typeof body['30day'], 'object')
      assert.strictEqual(body.result, 'success')
      Object.keys(body).forEach(key => {
        assert(fields.indexOf(key) !== -1)

        if (key === 'result') {
          return
        }

        const row = body[key]

        assert(!isNaN(Number(row.open)))
        assert(!isNaN(Number(row.high)))
        assert(!isNaN(Number(row.low)))
        assert(!isNaN(Number(row.close)))
        assert(!isNaN(Number(row.vwap)))
        assert(!isNaN(Number(row.volume)))
        assert(!isNaN(Number(row.counter_volume)))
        assert(!isNaN(row.count))
        assert(moment(row.date).isValid())
      })
      done()
    })
  })

  it('should should get XRP index data with currency', function(done) {
    const url = 'http://localhost:' + port + '/v2/xrp_index/ticker'
    const fields = [
      'result',
      'fx_rate',
      '1hour',
      '1day',
      '3day',
      '7day',
      '30day'
    ]
    request({
      url: url,
      json: true,
      qs: {
        currency: 'CNY'
      }
    },
    function (err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 200)
      assert.strictEqual(typeof body, 'object')
      assert.strictEqual(typeof body['1hour'], 'object')
      assert.strictEqual(typeof body['1day'], 'object')
      assert.strictEqual(typeof body['3day'], 'object')
      assert.strictEqual(typeof body['7day'], 'object')
      assert.strictEqual(typeof body['30day'], 'object')
      assert.strictEqual(body.result, 'success')
      assert.strictEqual(body.fx_rate, '6.653197')
      Object.keys(body).forEach(key => {
        assert(fields.indexOf(key) !== -1)

        if (key === 'result' ||
            key === 'fx_rate') {
          return
        }

        const row = body[key]
        assert(!isNaN(Number(row.open)))
        assert(!isNaN(Number(row.high)))
        assert(!isNaN(Number(row.low)))
        assert(!isNaN(Number(row.close)))
        assert(!isNaN(Number(row.vwap)))
        assert(!isNaN(Number(row.volume)))
        assert(!isNaN(Number(row.counter_volume)))
        assert(!isNaN(row.count))
        assert(moment(row.date).isValid())
      })
      done()
    })
  })

  it('should error on invalid currency', function(done) {
    const url = 'http://localhost:' + port + '/v2/xrp_index/ticker'

    request({
      url: url,
      json: true,
      qs: {
        currency: 'BLAH'
      }
    },
    function (err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 400)
      assert.strictEqual(body.result, 'error')
      assert.strictEqual(body.message, 'exchange rate unavailable')
      done()
    })
  })
})
