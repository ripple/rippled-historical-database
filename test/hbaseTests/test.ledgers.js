var request = require('request');
var assert = require('assert');
var moment = require('moment');
var config = require('../../config/import.config');
var port = config.get('port') || 7111;

describe('ledgers API endpoint', function() {
  it('should return the latest ledger: /ledgers', function(done) {
    var url = 'http://localhost:' + port + '/v2/ledgers';
    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'success');
      assert.strictEqual(typeof body.ledger, 'object');
      assert.strictEqual(body.ledger.ledger_index, 11616413);
      assert.strictEqual(body.ledger.transactions, undefined);
      done();
    });
  });

  it('should return ledgers by ledger index: /ledgers/:ledger_index', function(done) {
    var url = 'http://localhost:' + port + '/v2/ledgers/11119599';
    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'success');
      assert.strictEqual(typeof body.ledger, 'object');
      assert.strictEqual(body.ledger.ledger_index, 11119599);
      assert.strictEqual(body.ledger.transactions, undefined);
      done();
    });
  });

  it('should return an error if the ledger is not found: /ledgers/:ledger_index', function(done) {
    var url = 'http://localhost:' + port + '/v2/ledgers/20000';
    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 404);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'error');
      assert.strictEqual(body.message, 'ledger not found');
      done();
    });
  });

  it('should return ledgers by ledger hash: /ledgers/:ledger_hash', function(done) {
    var url = 'http://localhost:' + port + '/v2/ledgers/b5931ad267e59306769309aff13fccd55c2ef944e99228c8f2eeec5d3b49234d';
    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'success');
      assert.strictEqual(typeof body.ledger, 'object');
      assert.strictEqual(body.ledger.ledger_hash, 'B5931AD267E59306769309AFF13FCCD55C2EF944E99228C8F2EEEC5D3B49234D');
      assert.strictEqual(body.ledger.transactions, undefined);
      done();
    });
  });

  it('should return an error if the hash is invald: /ledgers/:ledger_hash', function(done) {
    var url = 'http://localhost:' + port + '/v2/ledgers/b59';
    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 400);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'error');
      assert.strictEqual(body.message, 'invalid ledger identifier');
      done();
    });
  });

  it('should return ledgers by date: /ledgers/:date', function(done) {
    var url = 'http://localhost:' + port + '/v2/ledgers/2015-01-14T17:43:10';
    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(res.statusCode, 200);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'success');
      assert.strictEqual(typeof body.ledger, 'object');
      assert.strictEqual(body.ledger.transactions, undefined);
      done();
    });
  });

  it('should include transaction hashes with transactions=true', function(done) {
    var url = 'http://localhost:' + port + '/v2/ledgers';
    request({
      url: url,
      json: true,
      qs: {
        transactions : true,
      }
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(typeof body.ledger.transactions, 'object');
      body.ledger.transactions.forEach(function(hash) {
        assert.strictEqual(typeof hash, 'string');
        assert.strictEqual(hash.length, 64);
      });

      done();
    });
  });

  it('should include transaction json with expand=true', function(done) {
    var url = 'http://localhost:' + port + '/v2/ledgers';
    request({
      url: url,
      json: true,
      qs: {
        expand : true
      }
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(typeof body.ledger.transactions, 'object');
      body.ledger.transactions.forEach(function(tx) {
        assert.strictEqual(typeof tx, 'object');
        assert.strictEqual(typeof tx.tx, 'object');
        assert.strictEqual(typeof tx.meta, 'object');
        assert.strictEqual(typeof tx.hash, 'string');
        assert.strictEqual(typeof tx.date, 'string');
        assert.strictEqual(typeof tx.ledger_index, 'number');
      });

      done();
    });
  });

  it('should include transaction binary with binary=true', function(done) {
    var url = 'http://localhost:' + port + '/v2/ledgers';
    request({
      url: url,
      json: true,
      qs: {
        binary : true
      }
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(typeof body.ledger.transactions, 'object');
      body.ledger.transactions.forEach(function(tx) {
        assert.strictEqual(typeof tx, 'object');
        assert.strictEqual(typeof tx.tx, 'string');
        assert.strictEqual(typeof tx.meta, 'string');
        assert.strictEqual(typeof tx.hash, 'string');
        assert.strictEqual(typeof tx.date, 'string');
        assert.strictEqual(typeof tx.ledger_index, 'number');
      });

      done();
    });
  });
});
