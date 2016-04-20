var config = require('./config');
var request = require('request');
var assert = require('assert');
var moment = require('moment');
var utils = require('./utils');
var port = config.get('port') || 7111;

describe('transactions API endpoint', function() {
  it('should return a transaction given a transaction hash', function(done) {
    var hash = '22F26CE4E2270CE3CF4EB61C609E7ADEDCD41D4C1BA2D96D680A9B016C4F47DA';
    var url = 'http://localhost:' + port + '/v2/transactions/' + hash;

    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'success');
      assert.strictEqual(typeof body.transaction, 'object');
      assert.strictEqual(body.transaction.hash, hash);
      assert.strictEqual(typeof body.transaction.tx, 'object');
      assert.strictEqual(typeof body.transaction.meta, 'object');
      done();
    });
  });

  it('should return a transaction in binary', function(done) {
    var hash = '22F26CE4E2270CE3CF4EB61C609E7ADEDCD41D4C1BA2D96D680A9B016C4F47DA';
    var url = 'http://localhost:' + port + '/v2/transactions/' + hash;

    request({
      url: url,
      json: true,
      qs: {
        binary : true
      }
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'success');
      assert.strictEqual(typeof body.transaction, 'object');
      assert.strictEqual(body.transaction.hash, hash);
      assert.strictEqual(typeof body.transaction.tx, 'string');
      assert.strictEqual(typeof body.transaction.meta, 'string');
      done();
    });
  });

  it('should return an error if the transaction is not found', function(done) {
    var hash = '22F26CE4E2270CE3CF4EB61C609E7ADEDCD41D4C1BA2D96D680A9B016C4F47DC';
    var url = 'http://localhost:' + port + '/v2/transactions/' + hash;

    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 404);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'error');
      assert.strictEqual(body.message, 'transaction not found');
      done();
    });
  });

  it('should return an error if the hash is invalid', function(done) {
    var hash = '22F26CE4E2270CE3CF4EB61C609E7ADEDCD41D4C1BA2D96D680A9B016C4F47D';
    var url = 'http://localhost:' + port + '/v2/transactions/' + hash;

    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 400);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'error');
      assert.strictEqual(body.message, 'invalid hash');
      done();
    });
  });

  /**** transactions endpoint ****/

  it('should return transactions by time', function(done) {
    var url = 'http://localhost:' + port + '/v2/transactions/';

    request({
      url: url,
      json: true,
      qs: {
        limit : 100
      }
    },
    function (err, res, body) {
      var prev;

      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'success');
      assert.strictEqual(body.count, 100);
      assert.notStrictEqual(body.marker, undefined);
      assert.notStrictEqual(body.transactions.length, 0);
      body.transactions.forEach(function(t) {
        assert(t.hash);
        assert(t.date);
        assert(t.ledger_index);
        assert(t.tx);
        assert(t.meta);

        if (prev) {
          assert(moment.utc(t.date).diff(prev) >= 0);
        }

        prev = t.date;
      });

      done();
    });
  });

  it('should handle descending = false', function(done) {
    var url = 'http://localhost:' + port + '/v2/transactions/';

    request({
      url: url,
      json: true,
      qs: {
        limit : 100,
        descending : false,
      }
    },
    function (err, res, body) {
      var prev;

      assert.ifError(err);
      assert.notStrictEqual(body.transactions.length, 0);
      body.transactions.forEach(function(t) {
        if (prev) {
          assert(moment.utc(t.date).diff(prev) >= 0);
        }

        prev = t.date;
      });

      done();
    });
  });

  it('should return transactions in binary', function(done) {
    var url = 'http://localhost:' + port + '/v2/transactions/';

    request({
      url: url,
      json: true,
      qs: {
        binary : true,
      }
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'success');
      assert.notStrictEqual(body.transactions.length, 0);
      body.transactions.forEach(function(t) {
        assert(t.hash);
        assert(t.date);
        assert(t.ledger_index);
        assert.strictEqual(typeof t.tx, 'string');
        assert.strictEqual(typeof t.meta, 'string');
      });
      done();
    });
  });

  it('should restrict results based on time', function(done) {
    var url = 'http://localhost:' + port + '/v2/transactions/';
    var start = '2015-02-09T18:14:20+00:00';
    var end = '2015-02-09T18:14:50+00:00';
    request({
      url: url,
      json: true,
      qs: {
        start: start,
        end: end
      }
    },
    function(err, res, body) {
      assert.ifError(err);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'success');
      assert.strictEqual(body.transactions.length, 11);
      body.transactions.forEach(function(t) {
        var d= moment.utc(t.date);
        assert.strictEqual(d.isBetween(moment.utc(start), moment.utc(end)), true);
      });
      done();
    });
  });

  it('should handle pagination correctly', function(done) {
    var start = '2015-02-09T18:14:40';
    var end = '2015-02-09T18:14:50';
    var url = 'http://localhost:' + port +
        '/v2/transactions?start=' + start +
        '&end=' + end;

    utils.checkPagination(url, undefined, function(ref, i, body) {
      assert.strictEqual(body.transactions.length, 1);
      assert.equal(body.transactions[0].hash, ref.transactions[i].hash);
      assert.equal(body.transactions[0].ledger_index, ref.transactions[i].ledger_index);
    }, done);
  });

  it('should filter by transaction type', function(done) {
    var url = 'http://localhost:' + port + '/v2/transactions/';
    var type = 'OfferCreate';

    request({
      url: url,
      json: true,
      qs: {
        type: type
      }
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'success');
      assert.notStrictEqual(body.transactions.length, 0);
      body.transactions.forEach(function(t) {
        assert.strictEqual(t.tx.TransactionType, type);
      });
      done();
    });
  });

  it('should filter by transaction result', function(done) {
    var url = 'http://localhost:' + port + '/v2/transactions/';
    var result = 'tecUNFUNDED_OFFER';

    request({
      url: url,
      json: true,
      qs: {
        result: result
      }
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'success');
      assert.strictEqual(body.transactions.length, 14);
      body.transactions.forEach(function(t) {
        assert.strictEqual(t.meta.TransactionResult, result);
      });
      done();
    });
  });

  it('should give an error for an invalid transaction result', function(done) {
    var url = 'http://localhost:' + port + '/v2/transactions/';

    request({
      url: url,
      json: true,
      qs: {
        result: 'tecZ'
      }
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'error');
      assert.strictEqual(res.statusCode, 400);
      assert.strictEqual(body.message, 'invalid transaction result');
      done();
    });
  });

  it('should give an error for an invalid transaction type', function(done) {
    var url = 'http://localhost:' + port + '/v2/transactions/';

    request({
      url: url,
      json: true,
      qs: {
        type: 'Transaction'
      }
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'error');
      assert.strictEqual(res.statusCode, 400);
      assert.strictEqual(body.message, 'invalid transaction type');
      done();
    });
  });

  it('should give an error for an invalid start date', function(done) {
    var url = 'http://localhost:' + port + '/v2/transactions/';

    request({
      url: url,
      json: true,
      qs: {
        start: '11111111a'
      }
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'error');
      assert.strictEqual(res.statusCode, 400);
      assert.strictEqual(body.message, 'invalid start date format');
      done();
    });
  });

  it('should give an error for an invalid end date', function(done) {
    var url = 'http://localhost:' + port + '/v2/transactions/';

    request({
      url: url,
      json: true,
      qs: {
        end: '12-12-2014'
      }
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'error');
      assert.strictEqual(res.statusCode, 400);
      assert.strictEqual(body.message, 'invalid end date format');
      done();
    });
  });

  it('should include a link header when marker is present', function(done) {
    var url = 'http://localhost:' + port + '/v2/transactions?limit=1';
    var linkHeader = '<' + url +
      '&marker=20130611200120|000001021029|00001>; rel="next"';

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
