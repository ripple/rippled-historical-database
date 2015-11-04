var request = require('request');
var assert = require('assert');
var moment = require('moment');
var utils = require('../utils');
var config = require('../../config/import.config');
var port = config.get('port') || 7111;

describe('active accounts API endpoint', function() {
  it('get active accounts data', function(done) {
    var currency = 'USD';
    var issuer = 'rMwjYedjc7qqtKYVLiAccJSmCwih4LnE2q';
    var date = '2015-01-14';
    var period = '7day';
    var exchanges = 0;
    var url = 'http://localhost:' + port +
        '/v2/active_accounts/xrp/' + currency + '+' + issuer +
        '?date=' + date +
        '&period=' + period;
        ;

    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'success');
      assert.strictEqual(body.count, body.accounts.length);
      assert(Array.isArray(body.accounts));
      body.accounts.forEach(function(d) {
        assert.strictEqual(typeof d.buy, 'object');
        assert.strictEqual(typeof d.sell, 'object');
        assert.strictEqual(typeof d.account, 'string');
        assert.strictEqual(typeof d.base_volume, 'number');
        assert.strictEqual(typeof d.counter_volume, 'number');
        assert.strictEqual(typeof d.count, 'number');
        exchanges += d.count;
      });

      assert.strictEqual(body.exchanges_count, exchanges/2);
      done();
    });
  });

  it('get active accounts data with exchanges', function(done) {
    var currency = 'USD';
    var issuer = 'rMwjYedjc7qqtKYVLiAccJSmCwih4LnE2q';
    var date = '2015-01-14';
    var period = '7day';
    var url = 'http://localhost:' + port +
        '/v2/active_accounts/xrp/' + currency + '+' + issuer +
        '?date=' + date +
        '&period=' + period +
        '&include_exchanges=true';

    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'success');
      assert(Array.isArray(body.accounts));
      body.accounts.forEach(function(d) {
        assert(Array.isArray(d.exchanges));
      });
      done();
    });
  });

  it('should error when base currency has no issuer', function(done) {
    var currency = 'USD';
    var url = 'http://localhost:' + port +
        '/v2/active_accounts/' + currency +
        '/XRP?include_exchanges=true'
    var last = 0;

    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 400);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'error');
      assert.strictEqual(body.message, 'base issuer is required');
      done();
    });
  });

  it('should error when counter currency has no issuer', function(done) {
    var currency = 'USD';
    var url = 'http://localhost:' + port +
        '/v2/active_accounts/XRP/' + currency +
        '?include_exchanges=true'
    var last = 0;

    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 400);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'error');
      assert.strictEqual(body.message, 'counter issuer is required');
      done();
    });
  });


  it('should error when XRP has issuer (base)', function(done) {
    var currency = 'USD';
    var url = 'http://localhost:' + port +
        '/v2/active_accounts/XRP+zzzz/' + currency +
        '+zzzz?include_exchanges=true'
    var last = 0;

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

  it('should error when XRP has issuer (base)', function(done) {
    var currency = 'USD';
    var url = 'http://localhost:' + port +
        '/v2/active_accounts/' + currency +
        '+zzzz/XRP+zzzz?include_exchanges=true'
    var last = 0;

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


  it('should error when period is invalid', function(done) {
    var currency = 'USD';
    var issuer = 'rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B';
    var url = 'http://localhost:' + port +
        '/v2/active_accounts/xrp/' + currency + '+' + issuer +
        '?period=1234';

    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 400);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'error');
      assert.strictEqual(body.message, 'invalid period - use: 1day, 3day, 7day');
      done();
    });
  });

  it('should error when date is invalid', function(done) {
    var currency = 'USD';
    var url = 'http://localhost:' + port +
        '/v2/active_accounts/USD+zzz/XRP' +
        '?include_exchanges=true&date=zzzz';
    var last = 0;

    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 400);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'error');
      assert.strictEqual(body.message, 'invalid date format');
      done();
    });
  });
});
