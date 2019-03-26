/* eslint max-len:0 */
'use strict'

var config = require('../config')
var assert = require('assert')
var request = require('request')
var Promise = require('bluebird')
var hbase = require('../lib/hbase')
var smoment = require('../lib/smoment')
var moment = require('moment')
const nconf = require('nconf');
var utils = require('./utils')
var reports = require('./mock/validator-reports.json');
var validations = require('./mock/ledger-validations.json');
var state = require('./mock/validator-state.json');
var port = config.get('port') || 7111

const validatorStates = {};
const ledgerValidations = {};
const historical = {};
const yesterday = smoment();

yesterday.moment.startOf('day').subtract(1, 'day');

reports.forEach((d, i) => {
  const key = `${yesterday.hbaseFormat()}|${d.pubkey}`;
  historical[key] = d;
  if (i < 5) {
    historical[`20160101000000|${d.pubkey}`] = d;
  }
});

validations.forEach(d => {
  ledgerValidations[d.rowkey] = d;
});

state.forEach(d => {
  d.last_ledger_time = smoment().format();
  validatorStates[d.rowkey] = d;
});

describe('setup mock data', function() {
  it('load data into hbase', function(done) {
    Promise.all([
      hbase.putRows({
        table: 'validator_state',
        rows: validatorStates
      }),
      hbase.putRows({
        table: 'validator_reports',
        rows: historical
      }),
      hbase.putRows({
        table: 'validations_by_ledger',
        rows: ledgerValidations
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

describe('validator reports', function() {
  it('should get validator reports (yesterday)', function(done) {
    var url = 'http://localhost:' + port +
      '/v2/network/validator_reports'

    request({
      url: url,
      json: true
    },
    function(err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 200)
      assert.strictEqual(body.reports.length, 91)
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
      assert.strictEqual(body.reports.length, 5)
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
        '/v2/network/validators/' + pubkey + '/reports?start=2015-12-31&end=2016-05-01'

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

describe('ledger validations', function() {
  it('should get ledger validations', function(done) {
    var h = '9373383605D0994AF33ACECA206693B331BA61C3CDA511AF3E7DD569593E126C'
    var url = 'http://localhost:' + port +
        '/v2/ledgers/' + h + '/validations'

    request({
      url: url,
      json: true
    },
    function(err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 200)
      assert.strictEqual(body.ledger_hash, h)
      assert.strictEqual(body.validations.length, 7)
      body.validations.forEach(function(d) {
        assert.strictEqual(d.ledger_hash, h)
      })
      done()
    })
  })

  it('should handle /ledgers/:hash/validations pagination', function(done) {
    var h = '9373383605D0994AF33ACECA206693B331BA61C3CDA511AF3E7DD569593E126C'
    var url = 'http://localhost:' + port +
        '/v2/ledgers/' + h + '/validations?'

    utils.checkPagination(url, undefined, function(ref, i, body) {
      assert.strictEqual(body.validations.length, 1)
      assert.equal(body.validations[0].signature, ref.validations[i].signature)
    }, done)
  })

  it('should include a link header when marker is present', function(done) {
    var h = '9373383605D0994AF33ACECA206693B331BA61C3CDA511AF3E7DD569593E126C'
    var url = 'http://localhost:' + port +
        '/v2/ledgers/' + h + '/validations?limit=1'
    var linkHeader = '<' + url + '&marker=' +
      '9373383605D0994AF33ACECA206693B331BA61C3CDA511AF3E7DD569593E126C' +
      '|nHUkp7WhouVMobBUKGrV5FNqjsdD9zKP5jpGnnLLnYxUQSGAwrZ6>'

    request({
      url: url,
      json: true
    },
    function(err, res) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 200)
      assert.strictEqual(res.headers.link.substr(0, 244), linkHeader)
      done()
    })
  })


  it('should get ledger validations in CSV format', function(done) {
    var h = '9373383605D0994AF33ACECA206693B331BA61C3CDA511AF3E7DD569593E126C'
    var url = 'http://localhost:' + port +
        '/v2/ledgers/' + h + '/validations?format=csv'

    request({
      url: url
    },
    function(err, res) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 200)
      assert.strictEqual(res.headers['content-disposition'],
        'attachment; filename=' + h + ' - validations.csv')
      done()
    })
  })

  it('should get a specific ledger validation', function(done) {
    var h = '9373383605D0994AF33ACECA206693B331BA61C3CDA511AF3E7DD569593E126C'
    var pubkey = 'nHUkhmyFPr3vEN3C8yfhKp4pu4t3wkTCi2KEDBWhyMNpsMj2HbnD'
    var url = 'http://localhost:' + port +
        '/v2/ledgers/' + h + '/validations/' + pubkey

    request({
      url: url,
      json: true
    },
    function(err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 200)
      assert.strictEqual(body.ledger_hash, h)
      assert.strictEqual(body.validation_public_key, pubkey)
      done()
    })
  })

  it('should error on an invalid ledger hash', function(done) {
    var hash = 'zzz'
    var url = 'http://localhost:' + port +
        '/v2/ledgers/' + hash + '/validations?format=csv'

    request({
      url: url,
      json: true
    },
    function(err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 400)
      assert.strictEqual(typeof body, 'object')
      assert.strictEqual(body.result, 'error')
      assert.strictEqual(body.message, 'invalid ledger hash')
      done()
    })
  })

  it('should error on validation not found', function(done) {
    var h = '9373383605D0994AF33ACECA206693B331BA61C3CDA511AF3E7DD569593E126C'
    var pubkey = 'abcd'
    var url = 'http://localhost:' + port +
        '/v2/ledgers/' + h + '/validations/' + pubkey

    request({
      url: url,
      json: true
    },
    function(err, res, body) {
      assert.ifError(err)
      assert.strictEqual(res.statusCode, 404)
      assert.strictEqual(typeof body, 'object')
      assert.strictEqual(body.result, 'error')
      assert.strictEqual(body.message, 'validation not found')
      done()
    })
  })
})


describe('validators', function() {
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
      assert.strictEqual(body.validators.length, 9)
      done()
    })
  })

  it('should get a single validator', function(done) {
    var pubkey = 'nHUsvzSgVYb7hy4A7VFkERmvLXqzW8oQRDRVULRv4UzJYPeFr4Zq'
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
});
