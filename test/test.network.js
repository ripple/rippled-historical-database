'use strict'

var config = require('../config')
var request = require('request')
var Promise = require('bluebird')
var assert = require('assert')
var moment = require('moment')
var smoment = require('../lib/smoment')
var utils = require('./utils')

var hbase = require('../lib/hbase')
var geolocation = require('../lib/validations/geolocation')
var saveVersions = require('../scripts/saveVersions')
var mockExchangeVolume = require('./mock/exchange-volume.json')
var mockExchangeVolumeHour = require('./mock/exchange-volume-live-hour.json')
var mockPaymentVolume = require('./mock/payment-volume.json')
var mockPaymentVolumeHour = require('./mock/payment-volume-live-hour.json')
var mockIssuedValue = require('./mock/issued-value.json')
var mockXrpDistribution = require('./mock/xrp-distribution.json')
var mockTopCurrencies = require('./mock/top-currencies.json')
var mockTopMarkets = require('./mock/top-markets.json')
var mockTopologyNodes = require('./mock/topology-nodes.json')
var mockTopologyLinks = require('./mock/topology-links.json')
var mockTopologyInfo = require('./mock/topology-info.json')
var mockFeeStats = require('./mock/fee-stats.json')
var mockExternalHour = require('./mock/external-markets-hour.json')
var mockExternalDay = require('./mock/external-markets-day.json')
var mockExternal3Day = require('./mock/external-markets-3day.json')
var mockExternal7Day = require('./mock/external-markets-7day.json')
var mockExternal30Day = require('./mock/external-markets-30day.json')
var port = config.get('port') || 7111


var geo = geolocation({
  table: config.get('hbase:prefix') + 'node_state',
  columnFamily: 'd'
})

var now = Date.now()
var today = smoment(moment(now))

/**
 * setup
 */

describe('setup mock data', function() {
  it('load data into hbase', function(done) {
    var table = 'agg_metrics'
    var rows = [
      hbase.putRow({
        table: table,
        rowkey: 'trade_volume|external|live|1hour',
        columns: mockExternalHour
      }),
      hbase.putRow({
        table: table,
        rowkey: 'trade_volume|external|live|1day',
        columns: mockExternalDay
      }),
      hbase.putRow({
        table: table,
        rowkey: 'trade_volume|external|live|3day',
        columns: mockExternal3Day
      }),
      hbase.putRow({
        table: table,
        rowkey: 'trade_volume|external|live|7day',
        columns: mockExternal7Day
      }),
      hbase.putRow({
        table: table,
        rowkey: 'trade_volume|external|live|30day',
        columns: mockExternal30Day
      }),
      hbase.putRow({
        table: table,
        rowkey: 'trade_volume|live',
        columns: mockExchangeVolume
      }),
      hbase.putRow({
        table: table,
        rowkey: 'trade_volume|live|hour',
        columns: mockExchangeVolumeHour
      }),
      hbase.putRow({
        table: table,
        rowkey: 'payment_volume|live',
        columns: mockPaymentVolume
      }),
      hbase.putRow({
        table: table,
        rowkey: 'payment_volume|live|hour',
        columns: mockPaymentVolumeHour
      }),
      hbase.putRow({
        table: table,
        rowkey: 'issued_value|live',
        columns: mockIssuedValue
      }),
      hbase.putRow({
        table: table,
        rowkey: 'trade_volume|day|20150114000000',
        columns: mockExchangeVolume
      }),
      hbase.putRow({
        table: table,
        rowkey: 'payment_volume|day|20150114000000',
        columns: mockPaymentVolume
      }),
      hbase.putRow({
        table: table,
        rowkey: 'issued_value|20150114000000',
        columns: mockIssuedValue
      }),
      hbase.putRow({
        table: table,
        rowkey: 'issued_value|20150113000000',
        columns: mockIssuedValue
      })
    ]

    mockXrpDistribution.forEach(function(r) {
      rows.push(hbase.putRow({
        table: 'xrp_distribution',
        rowkey: moment.utc(r.date).format('YYYYMMDDHHmmss'),
        columns: r
      }))
    })

    mockTopCurrencies.forEach(function(r, i) {
      var key = '20150114|00000' + (i + 1)
      rows.push(hbase.putRow({
        table: 'top_currencies',
        rowkey: key,
        columns: r
      }))
    })

    mockTopMarkets.forEach(function(r, i) {
      var key = '20150114|00000' + (i + 1)
      rows.push(hbase.putRow({
        table: 'top_markets',
        rowkey: key,
        columns: r
      }))
    })

    var parts = mockTopologyNodes[0].rowkey.split('+')
    var range = now + '_' + now

    mockTopologyNodes[0].rowkey = range + '+' + parts[1]
    parts = mockTopologyLinks[0].rowkey.split('+')
    mockTopologyLinks[0].rowkey = range + '+' + parts[1]
    mockTopologyInfo[0].rowkey = range

    mockTopologyNodes.forEach(function(r) {
      rows.push(hbase.putRow({
        table: 'crawl_node_stats',
        rowkey: r.rowkey,
        columns: r
      }))

      rows.push(hbase.putRow({
        table: 'node_state',
        rowkey: r.pubkey,
        columns: {
          ipp: r.ipp || 'not_present',
          version: r.version,
          city: 'San Francisco'
        }
      }))
    })

    mockTopologyLinks.forEach(function(r) {
      rows.push(hbase.putRow({
        table: 'connections',
        rowkey: r.rowkey,
        columns: r
      }))
    })

    mockTopologyInfo.forEach(function(r) {
      rows.push(hbase.putRow({
        table: 'crawls',
        rowkey: r.rowkey,
        columns: r
      }))
    })

    mockFeeStats.forEach(function(r) {
      rows.push(hbase.putRow({
        table: 'fee_stats',
        rowkey: r.rowkey,
        columns: r
      }))
    })

    Promise.all(rows).nodeify(function(err) {
      assert.ifError(err)
      done()
    })
  })

  it('import rippled versions', function() {
    this.timeout(60000)
    return saveVersions(hbase)
  })
})

/**
 * external markets
 */

describe('external markets', function() {
  it('live hour', function(done) {
    var url = 'http://localhost:' + port +
        '/v2/network/external_markets'

    request({
      url: url,
      json: true,
      qs: {
        period: 'hour'
      }
    },
    function(err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 200)
      assert.strictEqual(typeof body, 'object')
      assert.strictEqual(body.result, 'success')
      assert(Array.isArray(body.data.components))
      assert.strictEqual(body.data.date, '2016-10-31T17:45:20Z')
      assert.strictEqual(body.data.period, '1hour')
      assert.strictEqual(body.data.total, '1298637.6294938')
      done()
    })
  })

  it('live day', function(done) {
    var url = 'http://localhost:' + port +
        '/v2/network/external_markets'

    request({
      url: url,
      json: true
    },
    function(err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 200)
      assert.strictEqual(typeof body, 'object')
      assert.strictEqual(body.result, 'success')
      assert(Array.isArray(body.data.components))
      assert.strictEqual(body.data.date, '2016-10-31T17:45:20Z')
      assert.strictEqual(body.data.period, '1day')
      assert.strictEqual(body.data.total, '78377786.86422238')
      done()
    })
  })

  it('live 3 day', function(done) {
    var url = 'http://localhost:' + port +
        '/v2/network/external_markets'

    request({
      url: url,
      json: true,
      qs: {
        period: '3day'
      }
    },
    function(err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 200)
      assert.strictEqual(typeof body, 'object')
      assert.strictEqual(body.result, 'success')
      assert(Array.isArray(body.data.components))
      assert.strictEqual(body.data.date, '2016-10-31T17:45:20Z')
      assert.strictEqual(body.data.period, '3day')
      assert.strictEqual(body.data.total, '448499944.4663716')
      done()
    })
  })

  it('live 7 day', function(done) {
    var url = 'http://localhost:' + port +
        '/v2/network/external_markets'

    request({
      url: url,
      json: true,
      qs: {
        period: '7day'
      }
    },
    function(err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 200)
      assert.strictEqual(typeof body, 'object')
      assert.strictEqual(body.result, 'success')
      assert(Array.isArray(body.data.components))
      assert.strictEqual(body.data.date, '2016-10-31T17:45:20Z')
      assert.strictEqual(body.data.period, '7day')
      assert.strictEqual(body.data.total, '871952196.3685108')
      done()
    })
  })

  it('live 30 day', function(done) {
    var url = 'http://localhost:' + port +
        '/v2/network/external_markets'

    request({
      url: url,
      json: true,
      qs: {
        period: '30day'
      }
    },
    function(err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 200)
      assert.strictEqual(typeof body, 'object')
      assert.strictEqual(body.result, 'success')
      assert(Array.isArray(body.data.components))
      assert.strictEqual(body.data.date, '2016-10-31T17:45:20Z')
      assert.strictEqual(body.data.period, '30day')
      assert.strictEqual(body.data.total, '1511932644.1994743')
      done()
    })
  })

  it('fail invalid period', function(done) {
    var url = 'http://localhost:' + port +
        '/v2/network/external_markets'

    request({
      url: url,
      json: true,
      qs: {
        period: '5week'
      }
    },
    function(err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 400)
      assert.strictEqual(typeof body, 'object')
      assert.strictEqual(body.result, 'error')
      assert.strictEqual(body.message,
                         'invalid period - use: hour, day, 3day, 7day, 30day')
      done()
    })
  })
})

/**
 * rippled versions
 */

describe('rippled versions', function() {
  it('should get current rippled versions', function(done) {
    var url = 'http://localhost:' + port +
        '/v2/network/rippled_versions'
    var date = smoment()
    date.moment.startOf('day')

    request({
      url: url,
      json: true
    },
    function(err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 200)
      assert.strictEqual(typeof body, 'object')
      assert.strictEqual(body.result, 'success')
      assert(body.rows.length > 0)
      body.rows.forEach(function(d) {
        assert.strictEqual(date.format(), d.date)
        assert.strictEqual(typeof d.repo, 'string')
        assert.strictEqual(typeof d.version, 'string')
      })
      done()
    })
  })
})

/**
 * network fees
 */

describe('network fees', function() {
  it('should get ledger fee summaries', function(done) {
    var url = 'http://localhost:' + port +
        '/v2/network/fees'

    request({
      url: url,
      json: true
    },
    function(err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 200)
      assert.strictEqual(typeof body, 'object')
      assert.strictEqual(body.result, 'success')
      assert.strictEqual(body.count, 54)
      body.rows.forEach(function(r) {
        assert.strictEqual(typeof r.avg, 'number')
        assert.strictEqual(typeof r.min, 'number')
        assert.strictEqual(typeof r.max, 'number')
        assert.strictEqual(typeof r.total, 'number')
        assert.strictEqual(typeof r.tx_count, 'number')
        assert.strictEqual(typeof r.ledger_index, 'number')
        assert(moment(r.date).isValid())
      })
      done()
    })
  })

  it('should restrict by start and end dates', function(done) {
    var start = '2015-01-14T18:00:00'
    var end = '2015-02-01'
    var url = 'http://localhost:' + port +
        '/v2/network/fees?' +
        'start=' + start +
        '&end=' + end

    request({
      url: url,
      json: true
    },
    function(err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 200)
      assert.strictEqual(typeof body, 'object')
      assert.strictEqual(body.result, 'success')
      assert.strictEqual(body.count, 38)
      body.rows.forEach(function(r) {
        var date = moment(r.date)
        assert.strictEqual(typeof r.avg, 'number')
        assert.strictEqual(typeof r.min, 'number')
        assert.strictEqual(typeof r.max, 'number')
        assert.strictEqual(typeof r.total, 'number')
        assert.strictEqual(typeof r.tx_count, 'number')
        assert(date.isValid())
        assert(date.diff(start) >= 0)
        assert(date.diff(end) <= 0)
      })
      done()
    })
  })

  it('should get fee summaries in decending order', function(done) {
    var url = 'http://localhost:' + port +
        '/v2/network/fees?descending=true'

    request({
      url: url,
      json: true
    },
    function(err, res, body) {
      var date
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 200)
      assert.strictEqual(typeof body, 'object')
      assert.strictEqual(body.result, 'success')
      assert.strictEqual(body.count, 54)
      body.rows.forEach(function(r) {
        if (date) {
          assert(date.diff(r.date) >= 0)
        }

        date = moment(r.date)
      })
      done()
    })
  })

  it('should get hourly fee summaries', function(done) {
    var url = 'http://localhost:' + port +
        '/v2/network/fees?interval=hour'

    request({
      url: url,
      json: true
    },
    function(err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 200)
      assert.strictEqual(typeof body, 'object')
      assert.strictEqual(body.result, 'success')
      assert.strictEqual(body.count, 10)
      body.rows.forEach(function(r) {
        assert.strictEqual(typeof r.avg, 'number')
        assert.strictEqual(typeof r.min, 'number')
        assert.strictEqual(typeof r.max, 'number')
        assert.strictEqual(typeof r.total, 'number')
        assert.strictEqual(typeof r.tx_count, 'number')
        assert(moment(r.date).isValid())
      })
      done()
    })
  })

  it('should get daily fee summaries', function(done) {
    var url = 'http://localhost:' + port +
        '/v2/network/fees?interval=day'

    request({
      url: url,
      json: true
    },
    function(err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 200)
      assert.strictEqual(typeof body, 'object')
      assert.strictEqual(body.result, 'success')
      assert.strictEqual(body.count, 7)
      body.rows.forEach(function(r) {
        assert.strictEqual(typeof r.avg, 'number')
        assert.strictEqual(typeof r.min, 'number')
        assert.strictEqual(typeof r.max, 'number')
        assert.strictEqual(typeof r.total, 'number')
        assert.strictEqual(typeof r.tx_count, 'number')
        assert(moment(r.date).isValid())
      })
      done()
    })
  })

  it('should handle pagination correctly', function(done) {
    var url = 'http://localhost:' + port +
      '/v2/network/fees?interval=hour'

    utils.checkPagination(url, undefined, function(ref, i, body) {
      assert.strictEqual(body.rows.length, 1)
      assert.deepEqual(body.rows[0], ref.rows[i])
    }, done)
  })

  it('should include a link header when marker is present', function(done) {
    var url = 'http://localhost:' + port + '/v2/network/fees?limit=1'
    var linkHeader = '<' + url +
      '&marker=ledger|20131025102710|000002964124>; rel="next"'

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
    var start = 'x2015-01-14T00:00'
    var end = '2015-01-14T00:00'
    var url = 'http://localhost:' + port +
        '/v2/network/fees' +
        '?start=' + start +
        '&end=' + end

    request({
      url: url,
      json: true
    },
    function(err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 400)
      assert.strictEqual(typeof body, 'object')
      assert.strictEqual(body.result, 'error')
      assert.strictEqual(body.message, 'invalid start date format')
      done()
    })
  })

  it('should error on invalid end date', function(done) {
    var start = '2015-01-14T00:00'
    var end = 'x2015-01-14T00:00'
    var url = 'http://localhost:' + port +
        '/v2/network/fees' +
        '?start=' + start +
        '&end=' + end

    request({
      url: url,
      json: true
    },
    function(err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 400)
      assert.strictEqual(typeof body, 'object')
      assert.strictEqual(body.result, 'error')
      assert.strictEqual(body.message, 'invalid end date format')
      done()
    })
  })

  it('should error on invalid interval', function(done) {
    var url = 'http://localhost:' + port +
        '/v2/network/fees?interval=zzz'

    request({
      url: url,
      json: true
    },
    function(err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 400)
      assert.strictEqual(typeof body, 'object')
      assert.strictEqual(body.result, 'error')
      assert.strictEqual(body.message, 'invalid interval')
      done()
    })
  })
})

/**
 * network fee stats
 */

describe('network fee stats', function() {
  it('should get fee stats', function(done) {
    var url = 'http://localhost:' + port +
        '/v2/network/fee_stats'

    request({
      url: url,
      json: true
    },
    function(err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 200)
      assert.strictEqual(typeof body, 'object')
      assert.strictEqual(body.result, 'success')
      assert.strictEqual(body.count, 20)
      body.rows.forEach(function(r) {
        assert.strictEqual(typeof r.current_ledger_size, 'number')
        assert.strictEqual(typeof r.current_queue_size, 'number')
        assert.strictEqual(typeof r.expected_ledger_size, 'number')
        assert.strictEqual(typeof r.median_fee, 'number')
        assert.strictEqual(typeof r.minimum_fee, 'number')
        assert.strictEqual(typeof r.open_ledger_fee, 'number')
        assert.strictEqual(typeof r.pct_max_queue_size, 'number')
        assert(moment(r.date).isValid())
      })
      done()
    })
  })

  it('should restrict by start and end dates', function(done) {
    var start = '2016-08-24T00:15:15Z'
    var end = '2016-08-24T00:15:55Z'
    var url = 'http://localhost:' + port +
        '/v2/network/fee_stats?' +
        'start=' + start +
        '&end=' + end

    request({
      url: url,
      json: true
    },
    function(err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 200)
      assert.strictEqual(typeof body, 'object')
      assert.strictEqual(body.result, 'success')
      assert.strictEqual(body.count, 9)
      body.rows.forEach(function(r) {
        var date = moment(r.date)
        assert(date.isValid())
        assert(date.diff(start) >= 0)
        assert(date.diff(end) <= 0)
      })
      done()
    })
  })

  it('should get fee stats in decending order', function(done) {
    var url = 'http://localhost:' + port +
        '/v2/network/fee_stats?descending=true'

    request({
      url: url,
      json: true
    },
    function(err, res, body) {
      var date
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 200)
      assert.strictEqual(typeof body, 'object')
      assert.strictEqual(body.result, 'success')
      assert.strictEqual(body.count, 20)
      body.rows.forEach(function(r) {
        if (date) {
          assert(date.diff(r.date) >= 0)
        }

        date = moment(r.date)
      })
      done()
    })
  })

  it('should get fee stats at interval', function(done) {
    var url = 'http://localhost:' + port +
        '/v2/network/fee_stats?interval=minute&start=2016-08-24T00:15:00Z'

    request({
      url: url,
      json: true
    },
    function(err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 200)
      assert.strictEqual(typeof body, 'object')
      assert.strictEqual(body.result, 'success')
      assert.strictEqual(body.count, 2)
      body.rows.forEach(function(r) {
        assert.strictEqual(typeof r.current_ledger_size, 'number')
        assert.strictEqual(typeof r.current_queue_size, 'number')
        assert.strictEqual(typeof r.expected_ledger_size, 'number')
        assert.strictEqual(typeof r.median_fee, 'number')
        assert.strictEqual(typeof r.minimum_fee, 'number')
        assert.strictEqual(typeof r.open_ledger_fee, 'number')
        assert.strictEqual(typeof r.pct_max_queue_size, 'number')
        assert(moment(r.date).isValid())
      })
      done()
    })
  })

  it('should handle pagination correctly', function(done) {
    var url = 'http://localhost:' + port +
      '/v2/network/fee_stats?'

    utils.checkPagination(url, undefined, function(ref, i, body) {
      assert.strictEqual(body.rows.length, 1)
      assert.deepEqual(body.rows[0], ref.rows[i])
    }, done)
  })

  it('should include a link header when marker is present', function(done) {
    var url = 'http://localhost:' + port + '/v2/network/fee_stats?limit=1'
    var linkHeader = '<' + url +
      '&marker=raw|20160824001505>; rel="next"'

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
    var start = 'x2015-01-14T00:00'
    var end = '2015-01-14T00:00'
    var url = 'http://localhost:' + port +
        '/v2/network/fees' +
        '?start=' + start +
        '&end=' + end

    request({
      url: url,
      json: true
    },
    function(err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 400)
      assert.strictEqual(typeof body, 'object')
      assert.strictEqual(body.result, 'error')
      assert.strictEqual(body.message, 'invalid start date format')
      done()
    })
  })

  it('should error on invalid end date', function(done) {
    var start = '2015-01-14T00:00'
    var end = 'x2015-01-14T00:00'
    var url = 'http://localhost:' + port +
        '/v2/network/fees' +
        '?start=' + start +
        '&end=' + end

    request({
      url: url,
      json: true
    },
    function(err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 400)
      assert.strictEqual(typeof body, 'object')
      assert.strictEqual(body.result, 'error')
      assert.strictEqual(body.message, 'invalid end date format')
      done()
    })
  })

  it('should error on invalid interval', function(done) {
    var url = 'http://localhost:' + port +
        '/v2/network/fees?interval=zzz'

    request({
      url: url,
      json: true
    },
    function(err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 400)
      assert.strictEqual(typeof body, 'object')
      assert.strictEqual(body.result, 'error')
      assert.strictEqual(body.message, 'invalid interval')
      done()
    })
  })
})

/**
 * exchange volume
 */

describe('network - exchange volume', function() {
  it('get live exchange volume', function(done) {
    var url = 'http://localhost:' + port +
        '/v2/network/exchange_volume'

    request({
      url: url,
      json: true
    },
    function(err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 200)
      assert.strictEqual(typeof body, 'object')
      assert.strictEqual(body.result, 'success')
      assert.strictEqual(body.count, 1)
      assert.strictEqual(body.rows[0].count, 46933)
      done()
    })
  })

  it('get live exchange volume (hour)', function(done) {
    var url = 'http://localhost:' + port +
        '/v2/network/exchange_volume?live=hour'

    request({
      url: url,
      json: true
    },
    function(err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 200)
      assert.strictEqual(typeof body, 'object')
      assert.strictEqual(body.result, 'success')
      assert.strictEqual(body.count, 1)
      assert.strictEqual(body.rows[0].count, 12345)
      done()
    })
  })

  it('get exchange volume with exchange currency', function(done) {
    var currency = 'BTC'
    var issuer = 'rMwjYedjc7qqtKYVLiAccJSmCwih4LnE2q'
    var start = '2015-01-14T00:00'
    var end = '2015-01-14T00:00'
    var url = 'http://localhost:' + port +
        '/v2/network/exchange_volume' +
        '?exchange_currency=' + currency +
        '&exchange_issuer=' + issuer +
        '&start=' + start +
        '&end=' + end

    request({
      url: url,
      json: true
    },
    function(err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 200)
      assert.strictEqual(typeof body, 'object')
      assert.strictEqual(body.result, 'success')
      assert.strictEqual(body.count, 1)
      assert.strictEqual(body.rows[0].exchange.currency, currency)
      assert.strictEqual(body.rows[0].exchange.issuer, issuer)
      done()
    })
  })

  it('should error on exchange currency without issuer', function(done) {
    var url = 'http://localhost:' + port +
      '/v2/network/exchange_volume' +
      '?exchange_currency=USD'

    request({
      url: url,
      json: true
    },
    function(err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 400)
      assert.strictEqual(typeof body, 'object')
      assert.strictEqual(body.result, 'error')
      assert.strictEqual(body.message, 'exchange currency must have an issuer')
      done()
    })
  })

  it('should error on exchange XRP with issuer', function(done) {
    var url = 'http://localhost:' + port +
      '/v2/network/exchange_volume' +
      '?exchange_currency=XRP&exchange_issuer=zzz'

    request({
      url: url,
      json: true
    },
    function(err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 400)
      assert.strictEqual(typeof body, 'object')
      assert.strictEqual(body.result, 'error')
      assert.strictEqual(body.message, 'XRP cannot have an issuer')
      done()
    })
  })

  it('should error on invalid live period', function(done) {
    var url = 'http://localhost:' + port +
        '/v2/network/exchange_volume?live=week'

    request({
      url: url,
      json: true
    },
    function(err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 400)
      assert.strictEqual(typeof body, 'object')
      assert.strictEqual(body.result, 'error')
      assert.strictEqual(body.message,
                         'invalid period - use: ' +
                         'hour, day, 3day, 7day, 30day')
      done()
    })
  })
})

/**
 * payment volume
 */

describe('network - payment volume', function() {
  it('get live payments volume', function(done) {
    var url = 'http://localhost:' + port +
        '/v2/network/payment_volume'

    request({
      url: url,
      json: true
    },
    function(err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 200)
      assert.strictEqual(typeof body, 'object')
      assert.strictEqual(body.result, 'success')
      assert.strictEqual(body.count, 1)
      assert.strictEqual(body.rows[0].count, 9716)
      done()
    })
  })

  it('get live payments volume (hour)', function(done) {
    var url = 'http://localhost:' + port +
        '/v2/network/payment_volume?live=hour'

    request({
      url: url,
      json: true
    },
    function(err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 200)
      assert.strictEqual(typeof body, 'object')
      assert.strictEqual(body.result, 'success')
      assert.strictEqual(body.count, 1)
      assert.strictEqual(body.rows[0].count, 1234)
      done()
    })
  })

  it('get historical payment volume', function(done) {
    var start = '2015-01-14T00:00'
    var end = '2015-01-14T00:00'
    var url = 'http://localhost:' + port +
        '/v2/network/payment_volume' +
        '?start=' + start +
        '&end=' + end

    request({
      url: url,
      json: true
    },
    function(err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 200)
      assert.strictEqual(typeof body, 'object')
      assert.strictEqual(body.result, 'success')
      assert.strictEqual(body.count, 1)
      assert.strictEqual(body.rows[0].count, 9716)
      done()
    })
  })
})

/**
 * XRP distribution
 */

describe('network - XRP distribution', function() {
  it('get XRP distribution', function(done) {
    var url = 'http://localhost:' + port +
        '/v2/network/xrp_distribution'

    request({
      url: url,
      json: true
    },
    function(err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 200)
      assert.strictEqual(typeof body, 'object')
      assert.strictEqual(body.result, 'success')
      assert.strictEqual(body.count, 5)
      assert.strictEqual(body.rows[0].distributed, '34403590264.90344')
      done()
    })
  })

  it('get limit results by start and end date', function(done) {
    var start = '2016-03-20T00:00:00Z'
    var end = '2016-04-03T00:00:00Z'
    var url = 'http://localhost:' + port +
        '/v2/network/xrp_distribution?' +
        'start=' + start +
        '&end=' + end

    request({
      url: url,
      json: true
    },
    function(err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 200)
      assert.strictEqual(typeof body, 'object')
      assert.strictEqual(body.result, 'success')
      assert.strictEqual(body.count, 3)
      assert.strictEqual(body.rows[0].distributed, '34404396143.21523')
      assert.strictEqual(body.rows[2].distributed, '34868657431.30438')
      done()
    })
  })

  it('should should get distribution in descending order', function(done) {
    var url = 'http://localhost:' + port +
        '/v2/network/xrp_distribution?descending=true'

    request({
      url: url,
      json: true,
      qs: {
        descending: true
      }
    },
    function(err, res, body) {
      var d
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 200)
      assert.strictEqual(typeof body, 'object')
      assert.strictEqual(body.result, 'success')
      assert.strictEqual(body.count, 5)
      body.rows.forEach(function(r) {
        if (d) {
          assert(d.diff(r.date) >= 0)
        }

        d = moment.utc(r.date)
      })
      done()
    })
  })

  it('should include a link header when marker is present', function(done) {
    var url = 'http://localhost:' + port +
      '/v2/network/xrp_distribution?limit=1'
    var linkHeader = '<' + url +
      '&marker=20160320000000>; rel="next"'

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

  it('should handle pagination correctly', function(done) {
    var url = 'http://localhost:' + port +
      '/v2/network/xrp_distribution?'

    utils.checkPagination(url, undefined, function(ref, i, body) {
      assert.strictEqual(body.rows.length, 1)
      assert.deepEqual(body.rows[0], ref.rows[i])
    }, done)
  })

  it('should get XRP distribution in CSV format', function(done) {
    var url = 'http://localhost:' + port +
        '/v2/network/xrp_distribution?format=csv'

    request({
      url: url
    },
    function(err, res) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 200)
      assert.strictEqual(res.headers['content-disposition'],
        'attachment; filename=XRP-distribution.csv')
      done()
    })
  })

  it('should error on invalid start date', function(done) {
    var date = 'zzz2015-01-14'
    var url = 'http://localhost:' + port +
      '/v2/network/xrp_distribution?start=' + date

    request({
      url: url,
      json: true
    },
    function(err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 400)
      assert.strictEqual(typeof body, 'object')
      assert.strictEqual(body.result, 'error')
      assert.strictEqual(body.message, 'invalid start date format')
      done()
    })
  })

  it('should error on invalid end date', function(done) {
    var date = 'zzz2015-01-14'
    var url = 'http://localhost:' + port +
      '/v2/network/xrp_distribution?end=' + date

    request({
      url: url,
      json: true
    },
    function(err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 400)
      assert.strictEqual(typeof body, 'object')
      assert.strictEqual(body.result, 'error')
      assert.strictEqual(body.message, 'invalid end date format')
      done()
    })
  })
})

/**
 * top markets
 */

describe('network - top markets', function() {
  it('should get top markets', function(done) {
    var date = '2015-01-14'
    var url = 'http://localhost:' + port +
        '/v2/network/top_markets/' + date

    request({
      url: url,
      json: true
    },
    function(err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 200)
      assert.strictEqual(body.markets.length, 56)
      done()
    })
  })

  it('should limit top markets results', function(done) {
    var date = '2015-01-14'
    var limit = 3
    var url = 'http://localhost:' + port +
      '/v2/network/top_markets/' + date + '?limit=' + limit

    request({
      url: url,
      json: true
    },
    function(err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 200)
      assert.strictEqual(body.markets.length, 3)
      done()
    })
  })

  it('should error on invalid date', function(done) {
    var date = 'zzz2015-01-14'
    var url = 'http://localhost:' + port +
        '/v2/network/top_markets/' + date

    request({
      url: url,
      json: true
    },
    function(err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 400)
      assert.strictEqual(typeof body, 'object')
      assert.strictEqual(body.result, 'error')
      assert.strictEqual(body.message, 'invalid date format')
      done()
    })
  })
})

/**
 * topCurrencies
 */

describe('network - top currencies', function() {
  it('should get top currencies', function(done) {
    var date = '2015-01-14'
    var url = 'http://localhost:' + port +
        '/v2/network/top_currencies/' + date

    request({
      url: url,
      json: true
    },
    function(err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 200)
      assert.strictEqual(body.currencies.length, 41)
      done()
    })
  })

  it('should limit top currencies results', function(done) {
    var date = '2015-01-14'
    var limit = 3
    var url = 'http://localhost:' + port +
      '/v2/network/top_currencies/' + date + '?limit=' + limit

    request({
      url: url,
      json: true
    },
    function(err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 200)
      assert.strictEqual(body.currencies.length, 3)
      done()
    })
  })

  it('should error on invalid date', function(done) {
    var date = 'zzz2015-01-14'
    var url = 'http://localhost:' + port +
        '/v2/network/top_currencies/' + date

    request({
      url: url,
      json: true
    },
    function(err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 400)
      assert.strictEqual(typeof body, 'object')
      assert.strictEqual(body.result, 'error')
      assert.strictEqual(body.message, 'invalid date format')
      done()
    })
  })
})

/**
 * Topology - nodes and links
 */

describe('network - topology', function() {
  it('should update node geolocation', function(done) {
    this.timeout(15000)

    geo.geolocateNodes()
    .then(done)
    .catch(e => {
      assert.ifError(e)
    })
  })

  it('should get topology nodes and links', function(done) {
    var url = 'http://localhost:' + port +
        '/v2/network/topology/'

    request({
      url: url,
      json: true
    },
    function(err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 200)
      assert.strictEqual(body.node_count, 1)
      assert.strictEqual(body.link_count, 1)
      assert.strictEqual(body.nodes[0].city, undefined)
      done()
    })
  })

  it('should get topology nodes and links in verbose', function(done) {
    var url = 'http://localhost:' + port +
        '/v2/network/topology?verbose=true'

    request({
      url: url,
      json: true
    },
    function(err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 200)
      assert.strictEqual(body.node_count, 1)
      assert.strictEqual(body.link_count, 1)
      assert.strictEqual(body.nodes[0].city, 'Montréal')
      done()
    })
  })

  it('should get topology nodes', function(done) {
    var url = 'http://localhost:' + port +
        '/v2/network/topology/nodes'

    request({
      url: url,
      json: true
    },
    function(err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 200)
      assert.strictEqual(body.count, 1)
      assert.strictEqual(body.nodes[0].city, undefined)
      done()
    })
  })

  it('should get topology nodes in verbose', function(done) {
    var url = 'http://localhost:' + port +
        '/v2/network/topology/nodes?verbose=true'

    request({
      url: url,
      json: true
    },
    function(err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 200)
      assert.strictEqual(body.count, 1)
      assert.strictEqual(body.nodes[0].city, 'Montréal')
      done()
    })
  })

  it('should get topology links', function(done) {
    var url = 'http://localhost:' + port +
        '/v2/network/topology/links'

    request({
      url: url,
      json: true
    },
    function(err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 200)
      assert.strictEqual(body.count, 1)
      done()
    })
  })

  it('should get a single topology node', function(done) {
    var pubkey = 'n94Extku8HiQVY8fcgxeot4bY7JqK2pNYfmdnhgf6UbcmgucHFY8'
    var url = 'http://localhost:' + port +
        '/v2/network/topology/nodes/' + pubkey

    request({
      url: url,
      json: true
    },
    function(err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 200)
      assert.strictEqual(body.node_public_key, pubkey)
      assert.strictEqual(body.city, 'San Francisco')
      done()
    })
  })

  it('should get topology by date', function(done) {
    var url = 'http://localhost:' + port +
        '/v2/network/topology?date=' +
        moment().subtract(1, 'day').format('YYYY-MM-DD')

    request({
      url: url,
      json: true
    },
    function(err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 200)
      assert.strictEqual(body.date, '2016-03-18T22:31:33Z')
      assert.strictEqual(body.node_count, 9)
      assert.strictEqual(body.link_count, 5)
      done()
    })
  })

  it('should get topology nodes by date', function(done) {
    var url = 'http://localhost:' + port +
        '/v2/network/topology/nodes?date=' +
        moment().subtract(1, 'day').format('YYYY-MM-DD')

    request({
      url: url,
      json: true
    },
    function(err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 200)
      assert.strictEqual(body.date, '2016-03-18T22:31:33Z')
      assert.strictEqual(body.count, 9)
      done()
    })
  })

  it('should get topology links by date', function(done) {
    var url = 'http://localhost:' + port +
        '/v2/network/topology/links?date=' +
        moment().subtract(1, 'day').format('YYYY-MM-DD')

    request({
      url: url,
      json: true
    },
    function(err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 200)
      assert.strictEqual(body.date, '2016-03-18T22:31:33Z')
      assert.strictEqual(body.count, 5)
      done()
    })
  })

  it('should error on invalid date', function(done) {
    var date = 'zzz2015-01-14'
    var url = 'http://localhost:' + port +
        '/v2/network/topology?date=' + date

    request({
      url: url,
      json: true
    },
    function(err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 400)
      assert.strictEqual(typeof body, 'object')
      assert.strictEqual(body.result, 'error')
      assert.strictEqual(body.message, 'invalid date format')
      done()
    })
  })

  it('should error on date over 30 days', function(done) {
    var date = moment.utc().subtract(31, 'days').format('YYYY-MM-DD')
    var url = 'http://localhost:' + port +
        '/v2/network/topology?date=' + date

    request({
      url: url,
      json: true
    },
    function(err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 400)
      assert.strictEqual(typeof body, 'object')
      assert.strictEqual(body.result, 'error')
      assert.strictEqual(body.message, 'date must be less than 30 days ago')
      done()
    })
  })

  it('should error on invalid date', function(done) {
    var date = 'zzz2015-01-14'
    var url = 'http://localhost:' + port +
        '/v2/network/topology/nodes?date=' + date

    request({
      url: url,
      json: true
    },
    function(err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 400)
      assert.strictEqual(typeof body, 'object')
      assert.strictEqual(body.result, 'error')
      assert.strictEqual(body.message, 'invalid date format')
      done()
    })
  })

  it('should error on date over 30 days', function(done) {
    var date = moment.utc().subtract(31, 'days').format('YYYY-MM-DD')
    var url = 'http://localhost:' + port +
        '/v2/network/topology/nodes?date=' + date

    request({
      url: url,
      json: true
    },
    function(err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 400)
      assert.strictEqual(typeof body, 'object')
      assert.strictEqual(body.result, 'error')
      assert.strictEqual(body.message, 'date must be less than 30 days ago')
      done()
    })
  })

  it('should error on invalid date', function(done) {
    var date = 'zzz2015-01-14'
    var url = 'http://localhost:' + port +
        '/v2/network/topology/links?date=' + date

    request({
      url: url,
      json: true
    },
    function(err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 400)
      assert.strictEqual(typeof body, 'object')
      assert.strictEqual(body.result, 'error')
      assert.strictEqual(body.message, 'invalid date format')
      done()
    })
  })

  it('should error on date over 30 days', function(done) {
    var date = moment.utc().subtract(31, 'days').format('YYYY-MM-DD')
    var url = 'http://localhost:' + port +
        '/v2/network/topology/links?date=' + date

    request({
      url: url,
      json: true
    },
    function(err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 400)
      assert.strictEqual(typeof body, 'object')
      assert.strictEqual(body.result, 'error')
      assert.strictEqual(body.message, 'date must be less than 30 days ago')
      done()
    })
  })

  it('should get get topology nodes in CSV format', function(done) {
    var url = 'http://localhost:' + port +
        '/v2/network/topology/nodes?format=csv'

    request({
      url: url
    },
    function(err, res) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 200)
      assert.strictEqual(res.headers['content-disposition'],
        'attachment; filename=topology nodes - ' + today.format() + '.csv')
      done()
    })
  })

  it('should get get topology links in CSV format', function(done) {
    var url = 'http://localhost:' + port +
        '/v2/network/topology/links?format=csv'

    request({
      url: url
    },
    function(err, res) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 200)
      assert.strictEqual(res.headers['content-disposition'],
        'attachment; filename=topology links - ' + today.format() + '.csv')
      done()
    })
  })
})
