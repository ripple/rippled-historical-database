var config = require('../config');
var request = require('request');
var assert = require('assert');
var moment = require('moment');
var utils = require('./utils');
var port = config.get('port') || 7111;

describe('account balance changes API endpoint', function() {

  it('should handle limit correctly', function(done) {
    var url = 'http://localhost:' + port +
      '/v2/accounts/rpjZUBy92h6worVCYERZcVCzgzgmHb17Dx/balance_changes?limit=2';

    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(body.count, 2);
      assert.strictEqual(body.balance_changes.length, 2);
      done();
    });
  });

  it('should handle currency correctly', function(done) {
    var url = 'http://localhost:' + port +
      '/v2/accounts/rpjZUBy92h6worVCYERZcVCzgzgmHb17Dx/balance_changes?currency=xrp';

    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.notStrictEqual(body.balance_changes.length, 0);  // Make sure we test something
      body.balance_changes.forEach( function(bch) {
        assert.strictEqual(bch.currency, 'XRP');
      });
      done();
    });
  });

  it('should handle counterparty correctly', function(done) {
    var counterparty = 'rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B';
    var url = 'http://localhost:' + port +
      '/v2/accounts/rHsZHqa5oMQNL5hFm4kfLd47aEMYjPstpg/balance_changes?' +
      'currency=btc&counterparty=' + counterparty;

    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.notStrictEqual(body.balance_changes.length, 0);  // Make sure we test something
      body.balance_changes.forEach( function(bch) {
        assert.strictEqual(bch.currency, 'BTC');
        assert.strictEqual(bch.counterparty, counterparty);
      });
      done();
    });
  });

  it('should limit results by change type', function(done) {
    var url = 'http://localhost:' + port +
      '/v2/accounts/rHsZHqa5oMQNL5hFm4kfLd47aEMYjPstpg/balance_changes?' +
      'change_type=transaction_cost';

    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.notStrictEqual(body.balance_changes.length, 0);  // Make sure we test something
      body.balance_changes.forEach( function(bch) {
        assert.strictEqual(bch.currency, 'XRP');
        assert.strictEqual(bch.change_type, 'transaction_cost');
      });
      done();
    });
  });

  it('should handle pagination correctly', function(done) {
    var url = 'http://localhost:' + port +
      '/v2/accounts/rpjZUBy92h6worVCYERZcVCzgzgmHb17Dx/balance_changes?';

    utils.checkPagination(url, undefined, function(ref, i, body) {
      assert.strictEqual(body.balance_changes.length, 1);
      assert.equal(body.balance_changes[0].change, ref.balance_changes[i].change);
      assert.equal(body.balance_changes[0].currency, ref.balance_changes[i].currency);
      assert.equal(body.balance_changes[0].tx_hash, ref.balance_changes[i].tx_hash);
    }, done);
  });

  it('should handle pagination correctly (descending)', function(done) {
    var url = 'http://localhost:' + port +
      '/v2/accounts/rpjZUBy92h6worVCYERZcVCzgzgmHb17Dx/balance_changes' +
      '?descending=true';

    utils.checkPagination(url, undefined, function(ref, i, body) {
      assert.strictEqual(body.balance_changes.length, 1);
      assert.equal(body.balance_changes[0].change, ref.balance_changes[i].change);
      assert.equal(body.balance_changes[0].currency, ref.balance_changes[i].currency);
      assert.equal(body.balance_changes[0].tx_hash, ref.balance_changes[i].tx_hash);
    }, done);
  });

  it('should handle dates correctly', function(done) {
    var start = '2015-01-14T18:00:00';
    var end = '2015-01-14T18:30:00';
    var url = 'http://localhost:' + port + '/v2/accounts/rpjZUBy92h6worVCYERZcVCzgzgmHb17Dx/balance_changes?'
                                         + 'start=' + start + '&end='+ end;
    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.notStrictEqual(body.balance_changes.length, 0);  // Make sure we test something
      body.balance_changes.forEach( function(bch) {
        var d= moment.utc(bch.executed_time);
        assert.strictEqual( d.isBetween(moment.utc(start), moment.utc(end)) , true);
      });
      done();
    });
  });

  it('should handle descending correctly', function(done) {
    var url = 'http://localhost:' + port +
      '/v2/accounts/rMwjYedjc7qqtKYVLiAccJSmCwih4LnE2q/balance_changes?' +
      'descending=true';

    request({
      url: url,
      json: true
    },
    function (err, res, body) {
      var d;
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.notStrictEqual(body.balance_changes.length, 0);  // Make sure we test something
      body.balance_changes.forEach(function(bch) {
        if (d) {
          assert(d.diff(bch.executed_time) >= 0);
        }

        d = moment.utc(bch.executed_time);
      });
      done();
    });
  });

  it('should handle empty response correctly', function(done) {
    var start = '1015-01-14T18:00:00';
    var end = '1970-01-14T18:30:00';
    var url = 'http://localhost:' + port + '/v2/accounts/rpjZUBy92h6worVCYERZcVCzgzgmHb17Dx/balance_changes?'
                                         + 'start=' + start + '&end='+ end;
    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(body.balance_changes.length, 0);
      assert.strictEqual(body.count, 0);
      done();
    });
  });

  it('should handle invalid params correctly', function(done) {
    var url = 'http://localhost:' + port +
      '/v2/accounts/rrrrUBy92h6worVCYERZcVCzgzgmHb17Dx/balance_changes' +
      '?counterparty=rpjZUBy92h6worVCYERZcVCzgzgmHb17Dx&currency=Xrp';

    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 400);
      done();
    });
  });

  it('should handle invalid change_type', function(done) {
    var url = 'http://localhost:' + port +
      '/v2/accounts/rHsZHqa5oMQNL5hFm4kfLd47aEMYjPstpg/balance_changes?' +
      'change_type=zzz';

    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 400);
      done();
    });
  });

  it('should include a link header when marker is present', function(done) {
    var url = 'http://localhost:' + port +
        '/v2/accounts/rpjZUBy92h6worVCYERZcVCzgzgmHb17Dx/balance_changes?limit=1';
    var linkHeader = '<' + url +
      '&marker=rpjZUBy92h6worVCYERZcVCzgzgmHb17Dx|20150114182720|000011119603|00004|00001>; rel="next"';

    request({
      url: url,
      json: true
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(res.headers.link, linkHeader);
      done();
    });
  });
});
