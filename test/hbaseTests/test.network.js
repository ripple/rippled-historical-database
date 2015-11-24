var request = require('request');
var Promise = require('bluebird');
var assert = require('assert');
var moment = require('moment');
var utils = require('../utils');
var config = require('../../config/import.config');
var port = config.get('port') || 7111;
var prefix = config.get('prefix') || 'TEST_';
var HBase = require('../../lib/hbase/hbase-client');
var mockExchangeVolume = require('../mock/exchange-volume.json');
var mockPaymentVolume = require('../mock/payment-volume.json');
var mockIssuedValue = require('../mock/issued-value.json');

var hbaseConfig = config.get('hbase');
hbaseConfig.prefix = prefix;
hbaseConfig.logLevel = 2;
hbaseConfig.max_sockets = 200;
hbaseConfig.timeout = 60000;

hbase = new HBase(hbaseConfig);

describe('network - exchange volume', function() {
  before(function(done) {
    var table = 'agg_metrics';

    Promise.all([
      hbase.putRow(table, 'trade_volume|live', mockExchangeVolume),
      hbase.putRow(table, 'payment_volume|live', mockPaymentVolume),
      hbase.putRow(table, 'issued_value|live', mockIssuedValue),
      hbase.putRow(table, 'trade_volume|day|20150114000000', mockExchangeVolume),
      hbase.putRow(table, 'payment_volume|day|20150114000000', mockPaymentVolume),
      hbase.putRow(table, 'issued_value|20150114000000', mockIssuedValue)
    ]).nodeify(function(err, resp){
      assert.ifError(err);
      done();
    });
  });


  it('get live exchange volume', function(done) {
    var url = 'http://localhost:' + port +
        '/v2/network/exchange_volume';

    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'success');
      assert.strictEqual(body.count, 1);
      assert.strictEqual(body.rows[0].count, 46933);
      done();
    });
  });

  it('get historical exchange volume', function(done) {
    var start = '2015-01-14T00:00';
    var end = '2015-01-14T00:00';
    var url = 'http://localhost:' + port +
        '/v2/network/exchange_volume' +
        '?start=' + start +
        '&end=' + end;

    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'success');
      assert.strictEqual(body.count, 1);
      assert.strictEqual(body.rows[0].count, 46933);
      done();
    });
  });

  it('get exchange volume with exchange currency', function(done) {
    var currency = 'BTC';
    var issuer = 'rMwjYedjc7qqtKYVLiAccJSmCwih4LnE2q';
    var start = '2015-01-14T00:00';
    var end = '2015-01-14T00:00';
    var url = 'http://localhost:' + port +
        '/v2/network/exchange_volume' +
        '?exchange_currency=' + currency +
        '&exchange_issuer=' + issuer +
        '&start=' + start +
        '&end=' + end;

    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'success');
      assert.strictEqual(body.count, 1);
      assert.strictEqual(body.rows[0].exchange.currency, currency);
      assert.strictEqual(body.rows[0].exchange.issuer, issuer);
      done();
    });
  });

  it('should error on exchange currency without issuer', function(done) {
    var url = 'http://localhost:' + port +
      '/v2/network/exchange_volume' +
      '?exchange_currency=USD';

    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 400);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'error');
      assert.strictEqual(body.message, 'exchange currency must have an issuer');
      done();
    });
  });

  it('should error on exchange XRP with issuer', function(done) {
    var url = 'http://localhost:' + port +
      '/v2/network/exchange_volume' +
      '?exchange_currency=XRP&exchange_issuer=zzz';

    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 400);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'error');
      assert.strictEqual(body.message, 'XRP cannot have an issuer');
      done();
    });
  });

  it('should error on invalid start date', function(done) {
    var start = 'x2015-01-14T00:00';
    var end = '2015-01-14T00:00';
    var url = 'http://localhost:' + port +
        '/v2/network/exchange_volume' +
        '?start=' + start +
        '&end=' + end;

    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 400);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'error');
      assert.strictEqual(body.message, 'invalid start date format');
      done();
    });
  });

  it('should error on invalid end date', function(done) {
    var start = '2015-01-14T00:00';
    var end = 'x2015-01-14T00:00';
    var url = 'http://localhost:' + port +
        '/v2/network/exchange_volume' +
        '?start=' + start +
        '&end=' + end;

    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 400);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'error');
      assert.strictEqual(body.message, 'invalid end date format');
      done();
    });
  });

  it('should error on invalid interval', function(done) {
    var start = '2015-01-14T00:00';
    var end = '2015-01-14T00:00';
    var url = 'http://localhost:' + port +
        '/v2/network/exchange_volume' +
        '?start=' + start +
        '&end=' + end + '&interval=years';

    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 400);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'error');
      assert.strictEqual(body.message, 'invalid interval - use: day, week, month');
      done();
    });
  });
});

describe('network - payment volume', function() {
  it('get live payments volume', function(done) {
    var url = 'http://localhost:' + port +
        '/v2/network/payment_volume';

    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'success');
      assert.strictEqual(body.count, 1);
      assert.strictEqual(body.rows[0].count, 9716);
      done();
    });
  });

  it('get historical payment volume', function(done) {
    var start = '2015-01-14T00:00';
    var end = '2015-01-14T00:00';
    var url = 'http://localhost:' + port +
        '/v2/network/payment_volume' +
        '?start=' + start +
        '&end=' + end;

    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'success');
      assert.strictEqual(body.count, 1);
      assert.strictEqual(body.rows[0].count, 9716);
      done();
    });
  });
});


describe('network - issued value', function() {
  it('get live issued_value', function(done) {
    var url = 'http://localhost:' + port +
        '/v2/network/issued_value';

    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'success');
      assert.strictEqual(body.count, 1);
      assert.strictEqual(body.rows[0].total, 1673555846.5357773);
      done();
    });
  });


  it('get historical issued value', function(done) {
    var start = '2015-01-14T00:00';
    var end = '2015-01-14T00:00';
    var url = 'http://localhost:' + port +
        '/v2/network/issued_value' +
        '?start=' + start +
        '&end=' + end;

    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'success');
      assert.strictEqual(body.count, 1);
      assert.strictEqual(body.rows[0].total, 1673555846.5357773);
      done();
    });
  });

  it('should error on interval provided', function(done) {
    var start = '2015-01-14T00:00';
    var end = '2015-01-14T00:00';
    var url = 'http://localhost:' + port +
        '/v2/network/issued_value' +
        '?start=' + start +
        '&end=' + end + '&interval=day';

    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 400);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'error');
      assert.strictEqual(body.message, 'interval cannot be used');
      done();
    });
  });
});
