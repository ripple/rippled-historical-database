var request = require('request');
var assert = require('assert');
var moment = require('moment');
var config = require('../../config/import.config');
var port = config.get('port') || 7111;

describe('account transactions API endpoint', function() {

  it('should return the last 20 transactions for an account', function(done) {
    var account = 'rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B';
    var url = 'http://localhost:' + port + '/v2/accounts/' + account + '/transactions';

    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'success');
      assert.strictEqual(body.count, 20);
      assert.strictEqual(body.transactions.length, 20);
      body.transactions.forEach(function(tx) {
        assert.strictEqual(typeof tx.meta, 'object');
        assert.strictEqual(typeof tx.tx, 'object');
      });
      done();
    });
  });

  it('should return limit the returned transactions to 5', function(done) {
    var account = 'rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B';
    var url = 'http://localhost:' + port + '/v2/accounts/' + account + '/transactions';

    request({
      url: url,
      json: true,
      qs: {
        limit : 5,
        offset : 5
      }
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'success');
      assert.strictEqual(body.count, 5);
      assert.strictEqual(body.transactions.length, 5);
      done();
    });
  });

  it('should return return only specified transaction types', function(done) {
    var account = 'rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B';
    var url = 'http://localhost:' + port + '/v2/accounts/' + account + '/transactions';

    request({
      url: url,
      json: true,
      qs: {
        type : 'OfferCreate',
      }
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'success');
      assert.strictEqual(body.count, 20);
      body.transactions.forEach(function(tx) {
        assert(['OfferCreate'].indexOf(tx.tx.TransactionType) !== -1);
      });
      done();
    });
  });

  it('should return return only specified transaction results', function(done) {
    var account = 'rfZ4YjC4CyaKFx9cgzYNKk4E2zTXRJif26';
    var result  = 'tecUNFUNDED_OFFER';
    var url = 'http://localhost:' + port + '/v2/accounts/' + account + '/transactions';

    request({
      url: url,
      json: true,
      qs: {
        limit : 5,
        result : result,
      }
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'success');
      body.transactions.forEach(function(tx) {
        assert.strictEqual(tx.meta.TransactionResult, result);
      });
      done();
    });
  });

  it('should return transactions for a given date range', function(done) {
    var account = 'rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B';
    var url = 'http://localhost:' + port + '/v2/accounts/' + account + '/transactions';

    var start= '2015-01-14T18:27:10';
    var end= '2015-01-14T18:27:29';

    request({
      url: url,
      json: true,
      qs: {
        start : start,
        end   : end,
      }
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'success');
      assert.strictEqual(body.count, 8);
      assert.strictEqual(body.transactions.length, 8);
      body.transactions.forEach( function(trans) {
        var d= moment.utc(trans.date);
        assert.strictEqual( d.isBetween(moment.utc(start), moment.utc(end))
                          || d.isSame(moment.utc(start)) || d.isSame(moment.utc(end))
                          , true);
      });
      done();
    });
  });

  it('should return transactions for a given date range (bis)', function(done) {
    var account = 'rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B';
    var url = 'http://localhost:' + port + '/v2/accounts/' + account + '/transactions';

    var start= '2015-01-14T18:27:10';
    var end= '2015-01-14T18:27:30';

    request({
      url: url,
      json: true,
      qs: {
        start : start,
        end   : end,
      }
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'success');
      assert.strictEqual(body.count, 13);
      assert.strictEqual(body.transactions.length, 13);
      body.transactions.forEach( function(trans) {
        var d= moment.utc(trans.date);
        assert.strictEqual( d.isBetween(moment.utc(start), moment.utc(end))
                          || d.isSame(moment.utc(start)) || d.isSame(moment.utc(end))
                          , true);
      });
      done();
    });
  });

  it('should return results in binary form', function(done) {
    var account = 'rfZ4YjC4CyaKFx9cgzYNKk4E2zTXRJif26';
    var url = 'http://localhost:' + port + '/v2/accounts/' + account + '/transactions';

    request({
      url: url,
      json: true,
      qs: {
        limit : 5,
        binary : true
      }
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'success');
      body.transactions.forEach(function(tx) {
        assert.strictEqual(typeof tx.meta, 'string');
        assert.strictEqual(typeof tx.tx, 'string');
      });
      done();
    });
  });

  it('should return results in ascending order', function(done) {
    var account = 'rMwjYedjc7qqtKYVLiAccJSmCwih4LnE2q';
    var url  = 'http://localhost:' + port + '/v2/accounts/' + account + '/transactions';
    var last = 0;

    request({
      url: url,
      json: true,
      qs: {
        descending:false,
        limit:50
      }
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'success');
      assert.strictEqual(body.transactions.length, 50);
      body.transactions.forEach(function(tx) {
        assert(last <= tx.ledger_index);
        last = tx.ledger_index;
      });
      done();
    });
  });

  it('should return a specific account transaction for a given sequence #', function(done) {
    var account  = 'rHsZHqa5oMQNL5hFm4kfLd47aEMYjPstpg';
    var sequence = 11370364;
    var url = 'http://localhost:' + port + '/v2/accounts/' + account + '/transactions/' + sequence;

    request({
      url: url,
      json: true
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'success');
      assert.strictEqual(typeof body.transaction, 'object');
      assert.strictEqual(typeof body.transaction.date, 'string');
      assert.strictEqual(typeof body.transaction.ledger_index, 'number');
      assert.strictEqual(typeof body.transaction.hash, 'string');
      assert.strictEqual(body.transaction.tx.Sequence, sequence);
      done();
    });
  });

  it('should return account transactions by sequence', function(done) {
    var account  = 'rHsZHqa5oMQNL5hFm4kfLd47aEMYjPstpg';
    var url = 'http://localhost:' + port + '/v2/accounts/' + account + '/transactions';
    var max  = 11370364;
    var min  = 11370357;
    var last = min - 1;
    request({
      url: url,
      json: true,
      qs: {
        min_sequence : min,
        max_sequence : max,
      }
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'success');
      body.transactions.forEach(function(tx) {
        assert.strictEqual(tx.tx.Sequence, last+1);
        assert(tx.tx.Sequence <= max);
        assert(tx.tx.Sequence >= min);
        last = tx.tx.Sequence;
      });
      done();
    });
  });

  it('should return an error if the transaction is not found', function(done) {
    var account  = 'rHsZHqa5oMQNL5hFm4kfLd47aEMYjPstpg';
    var sequence = 10000;
    var url = 'http://localhost:' + port + '/v2/accounts/' + account + '/transactions/' + sequence;

    request({
      url: url,
      json: true
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

  it('should return an error for an invalid time', function(done) {
    var account = 'rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B';
    var url = 'http://localhost:' + port + '/v2/accounts/' + account + '/transactions';

    request({
      url: url,
      json: true,
      qs: {
        start : '2015x',
        end   : '2015x',
      }
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

  it('should return an error for an invalid max or min sequence', function(done) {
    var account = 'rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B';
    var url = 'http://localhost:' + port + '/v2/accounts/' + account + '/transactions';
    var max = 'zzz1';
    var min = 'zzz2';

    request({
      url: url,
      json: true,
      qs: {
        min_sequence : min,
        max_sequence : max,
      }
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 400);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'error');
      assert.strictEqual(body.message, 'invalid min_sequence');
      done();
    });
  });
});
