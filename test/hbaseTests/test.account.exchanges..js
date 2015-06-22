var request = require('request');
var assert = require('assert');
var moment = require('moment');
var utils = require('../utils');
var config = require('../../config/import.config');
var port = config.get('port') || 7111;

describe('account exchanges API endpoint', function() {

  it('should make sure /accounts/:account/exhanges handles limit correctly', function(done) {
    var url = 'http://localhost:' + port + '/v2/accounts/rHsZHqa5oMQNL5hFm4kfLd47aEMYjPstpg/exchanges?limit=5';
    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(body.count, 5);
      assert.strictEqual(body.exchanges.length, 5);
      done();
    });
  });

  it('should make sure /accounts/:account/exhanges handles dates correctly', function(done) {
    var start= '2015-01-14T18:52:00';
    var end= '2015-01-14T19:00:00';
    var url = 'http://localhost:' + port + '/v2/accounts/rHsZHqa5oMQNL5hFm4kfLd47aEMYjPstpg/exchanges?'
                                         + 'start=' + start + '&end='+ end;
    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.notStrictEqual(body.exchanges.length, 0);  // Make sure we test something
      body.exchanges.forEach( function(exch) {
        var d= moment.utc(exch.executed_time);
        assert.strictEqual( d.isBetween(moment.utc(start), moment.utc(end)) , true);
      });
      done();
    });
  });

  it('should make sure /accounts/:account/exhanges/:curr handles currency correctly', function(done) {
    var url = 'http://localhost:' + port + '/v2/accounts/rHsZHqa5oMQNL5hFm4kfLd47aEMYjPstpg/exchanges/jpy';
    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.notStrictEqual(body.exchanges.length, 0);  // Make sure we test something
      body.exchanges.forEach( function(exch) {
        assert.strictEqual(exch.base_currency, 'JPY');
      });
      done();
    });
  });

  it('should make sure /accounts/:account/exhanges/:curr handles currency correctly', function(done) {
    var url = 'http://localhost:' + port + '/v2/accounts/rHsZHqa5oMQNL5hFm4kfLd47aEMYjPstpg/exchanges/BTC';
    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.notStrictEqual(body.exchanges.length, 0);  // Make sure we test something
      body.exchanges.forEach( function(exch) {
        assert.strictEqual(exch.base_currency, 'BTC');
      });
      done();
    });
  });

  it('should make sure /accounts/:account/exhanges/:curr-iss/:counter handles parameters correctly', function(done) {
    var url = 'http://localhost:' + port + '/v2/accounts/rHsZHqa5oMQNL5hFm4kfLd47aEMYjPstpg/exchanges/USD+rMwjYedjc7qqtKYVLiAccJSmCwih4LnE2q/xrp';
    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.notStrictEqual(body.exchanges.length, 0);  // Make sure we test something
      body.exchanges.forEach( function(exch) {
        assert.strictEqual(exch.base_currency, 'USD');
        assert.strictEqual(exch.base_issuer, 'rMwjYedjc7qqtKYVLiAccJSmCwih4LnE2q');
        assert.strictEqual(exch.counter_currency, 'XRP');
      });
      done();
    });
  });

  it('should make sure /accounts/:account/exhanges handles pagination correctly', function(done) {
    this.timeout(5000);
    var url = 'http://localhost:' + port + '/v2/accounts/rHsZHqa5oMQNL5hFm4kfLd47aEMYjPstpg/exchanges?';
    utils.checkPagination(url, undefined, function(ref, i, body) {
      assert.strictEqual(body.exchanges.length, 1);
      assert.equal(body.exchanges[0].base_amount, ref.exchanges[i].base_amount);
      assert.equal(body.exchanges[0].base_currency, ref.exchanges[i].base_currency);
      assert.equal(body.exchanges[0].tx_hash, ref.exchanges[i].tx_hash);
    }, done);
  });

  it('should make sure /accounts/:account/exhanges handles pagination correctly (descending)', function(done) {
    this.timeout(5000);
    var url = 'http://localhost:' + port + '/v2/accounts/rHsZHqa5oMQNL5hFm4kfLd47aEMYjPstpg/exchanges?';
    utils.checkPagination(url, undefined, function(ref, i, body) {
      assert.strictEqual(body.exchanges.length, 1);
      assert.equal(body.exchanges[0].base_amount, ref.exchanges[i].base_amount);
      assert.equal(body.exchanges[0].base_currency, ref.exchanges[i].base_currency);
      assert.equal(body.exchanges[0].tx_hash, ref.exchanges[i].tx_hash);
    }, done);
  });

  it('should make sure /accounts/:account/exchanges handles empty response correctly', function(done) {
    var url = 'http://localhost:' + port + '/v2/accounts/rrrrUBy92h6worVCYERZcVCzgzgmHb17Dx/exchanges';
    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(body.exchanges.length, 0);
       assert.strictEqual(body.count, 0);
      done();
    });
  });
});
