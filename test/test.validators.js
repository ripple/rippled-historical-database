/* eslint max-len:0 */
'use strict'

var config = require('../config')
var assert = require('assert')
var request = require('request')
var Promise = require('bluebird')
var hbase = require('../lib/hbase')
var smoment = require('../lib/smoment')
var mockValidations = require('./mock/validations.json')
var mockReports = require('./mock/validator-reports.json')
var port = config.get('port') || 7111

describe('validator reports', function() {

  before(function() {

    return hbase.deleteAllRows({
      table: 'validator_reports'
    }).then(() => {

      var date = smoment()
      date.moment.startOf('day')

      return Promise.map(mockReports, function(report) {
        const rowkey = report['rowkey'] ? report['rowkey'] :
          [
            date.hbaseFormatStartRow(),
            report.validation_public_key
          ].join('|');

        return hbase.putRow({
          table: 'validator_reports',
          rowkey: rowkey,
          columns: report
        });
      })
    })
  })

  after(function() {
    return hbase.deleteAllRows({
      table: 'validator_reports'
    })
  })


  it('should get validator reports', function(done) {
    var date = smoment()
    var url = 'http://localhost:' + port +
      '/v2/network/validator_reports'

    date.moment.startOf('day')

    request({
      url: url,
      json: true
    },
    function(err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 200)
      assert.strictEqual(body.reports.length, 5)
      body.reports.forEach(function(r) {
        assert.strictEqual(r.date, date.format())
      })
      done()
    })
  })


  it('should get validator reports by date', function(done) {
    var date = smoment('2016-01-01')
    var url = 'http://localhost:' + port +
      '/v2/network/validator_reports?date=' + date.format()

    request({
      url: url,
      json: true
    },
    function(err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 200)
      assert.strictEqual(body.reports.length, 0)
      done()
    })
  })

  it('should error on invalid date', function(done) {
    var date = 'zzz2015-01-14'
    var url = 'http://localhost:' + port +
        '/v2/network/validator_reports?date=' + date

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

  it('should get reports by validator', function(done) {
    var pubkey = 'n9MnXUt5Qcx3BuBYKJfS4fqSohgkT79NGjXnZeD9joKvP3A5RNGm'
    var url = 'http://localhost:' + port +
        '/v2/network/validators/' + pubkey + '/reports'

    request({
      url: url,
      json: true
    },
    function(err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 200)
      assert.strictEqual(body.reports.length, 1)
      done()
    })
  })

  it('should error on invalid start date', function(done) {
    var pubkey = 'n9MnXUt5Qcx3BuBYKJfS4fqSohgkT79NGjXnZeD9joKvP3A5RNGm'
    var start = 'zzz'
    var url = 'http://localhost:' + port +
        '/v2/network/validators/' + pubkey + '/reports?start=' + start

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
    var pubkey = 'n949f75evCHwgyP4fPVgaHqNHxUVN15PsJEZ3B3HnXPcPjcZAoy7'
    var end = 'zzz'
    var url = 'http://localhost:' + port +
        '/v2/network/validators/' + pubkey + '/reports?end=' + end

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

  it('should get validator reports in CSV format', function(done) {
    var url = 'http://localhost:' + port +
        '/v2/network/validator_reports?format=csv'

    request({
      url: url
    },
    function(err, res) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 200)
      assert.strictEqual(res.headers['content-disposition'],
        'attachment; filename=validator reports.csv')
      done()
    })
  })
})

describe('validators', function() {

  before(function() {
    return hbase.deleteAllRows({
      table: 'validators'
    }).then(() => {
      return Promise.map(mockValidations, function(val) {
        return hbase.putRow({
          table: 'validators',
          rowkey: val.validation_public_key,
          columns: {
            validation_public_key: val.validation_public_key,
            last_datetime: smoment().format()
          }
        });
      })
    })
  })

  after(function() {
    return hbase.deleteAllRows({
      table: 'validators'
    })
  })

  it('should get all validators', function(done) {
    var url = 'http://localhost:' + port +
        '/v2/network/validators'

    request({
      url: url,
      json: true
    },
    function(err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 200)
      assert.strictEqual(body.validators.length, 5)
      done()
    })
  })

  it('should get a single validator', function(done) {
    var pubkey = 'n9Kk6U5nSF8EggfmTpMdna96UuXWAVwSsDSXRkXeZ5vLcAFk77tr'
    var url = 'http://localhost:' + port +
        '/v2/network/validators/' + pubkey

    request({
      url: url,
      json: true
    },
    function(err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 200)
      assert.strictEqual(body.validation_public_key, pubkey)
      done()
    })
  })

  it('should get error on validator not found', function(done) {
    var pubkey = 'zzz'
    var url = 'http://localhost:' + port +
        '/v2/network/validators/' + pubkey

    request({
      url: url,
      json: true
    },
    function(err, res) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 404)
      done()
    })
  })

  it('should get validators in CSV format', function(done) {
    var url = 'http://localhost:' + port +
        '/v2/network/validators?format=csv'

    request({
      url: url
    },
    function(err, res) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 200)
      assert.strictEqual(res.headers['content-disposition'],
        'attachment; filename=validators.csv')
      done()
    })
  })
})
