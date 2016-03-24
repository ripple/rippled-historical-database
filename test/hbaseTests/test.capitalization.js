var request = require('request');
var assert = require('assert');
var moment = require('moment');
var utils = require('../utils');
var config = require('../../config/test.config');
var port = config.get('port') || 7111;

describe('capitalization API endpoint', function() {
  it('get capitalization data', function(done) {
    var currency = 'USD';
    var issuer = 'rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B';
    var url = 'http://localhost:' + port +
        '/v2/capitalization/' + currency + '+' + issuer;

    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'success');
      assert.strictEqual(body.currency, currency);
      assert.strictEqual(body.issuer, issuer);
      done();
    });
  });

  it('should fail with invalid issuer', function(done) {
    var currency = 'USD';
    var issuer = 'rvYAfWj5gh';
    var url = 'http://localhost:' + port +
        '/v2/capitalization/' + currency + '+' + issuer +
        '?interval=day';

    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 400);
      assert.strictEqual(body.result, 'error');
      assert.strictEqual(body.message, 'invalid issuer address');
      done();
    });
  });

  it('should fail with invalid interval', function(done) {
    var currency = 'USD';
    var issuer = 'rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B';
    var url = 'http://localhost:' + port +
        '/v2/capitalization/' + currency + '+' + issuer +
        '?interval=years';

    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 400);
      assert.strictEqual(body.result, 'error');
      assert.strictEqual(body.message, 'invalid interval - use: day, week, month');
      done();
    });
  });

  it('should fail with invalid start date', function(done) {
    var currency = 'USD';
    var issuer = 'rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B';
    var start = 'x2015-01-14T00:00';
    var end = '2015-01-14T00:00';
    var url = 'http://localhost:' + port +
        '/v2/capitalization/' + currency + '+' + issuer +
        '?start=' + start +
        '&end=' + end

    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 400);
      assert.strictEqual(body.result, 'error');
      assert.strictEqual(body.message, 'invalid start date format');
      done();
    });
  });

  it('should fail with invalid end date', function(done) {
    var currency = 'USD';
    var issuer = 'rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B';
    var start = '2015-01-14T00:00';
    var end = 'x2015-01-14T00:00';
    var url = 'http://localhost:' + port +
        '/v2/capitalization/' + currency + '+' + issuer +
        '?start=' + start +
        '&end=' + end

    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 400);
      assert.strictEqual(body.result, 'error');
      assert.strictEqual(body.message, 'invalid end date format');
      done();
    });
  });
});
