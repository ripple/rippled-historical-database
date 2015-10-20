var config   = require('../config/import.config');
var assert   = require('assert');
var Promise  = require('bluebird');
var request  = require('request');
var Postgres = require('../import/postgres/client');
var Server   = require('../api/server');
var fs       = require('fs');
var path     = __dirname + '/ledgers/';
var files    = fs.readdirSync(path);
var dbConfig = config.get('postgres');
var hbConfig = config.get('hbase');
var port     = 7111;
var server;
var db;

dbConfig.database = config.get('dbname') || 'test_db';
db     = new Postgres(dbConfig);
server = new Server({
  postgres : dbConfig,
  hbase    : hbConfig,
  port     : port,
});

describe('ETL and API:', function() {

  before(function(done) {
    console.log('migrating database...');
    db.migrate().then(function(){
      console.log('done');
      done();
    });
  });

  /*** import ledgers into Postgres ***/

  it('should save ledgers and transactions into the database', function(done) {
    this.timeout(10000);
    Promise.map(files, function(filename) {
      return new Promise(function(resolve, reject) {
        var ledger = JSON.parse(fs.readFileSync(path + filename, "utf8"));
        db.saveLedger(ledger, function(err, resp){
          if (err) reject(err);
          else     resolve();
        });
      });
    }).nodeify(function(err, resp) {
      assert.ifError(err);
      done();
    });
  });

  /*** run mulitple API's ***/

  it('should run up to 40 API servers simultaneously', function(done) {
    var count = 100;
    var server;
    var port;

    while (count-- > 60) {
      port   = '322' + count;
      server = new Server({
        postgres : dbConfig,
        hbase    : hbConfig,
        port     : port,
      });
    }

    //test the last one
    var url = 'http://localhost:' + port + '/v1/ledgers';
    request({
      url: url,
      json: true,
    },
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'success');
      done();
    });
  });

  /*** ledgers API endpoint ***/

  describe('/v1/ledgers', function() {

    it('should return the latest ledger: /v1/ledgers', function(done) {
      var url = 'http://localhost:' + port + '/v1/ledgers';
      request({
        url: url,
        json: true,
      },
      function (err, res, body) {
        console.log(err, body);
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

    it('should return ledgers by ledger index: /v1/ledgers/:ledger_index', function(done) {
      var url = 'http://localhost:' + port + '/v1/ledgers/11119599';
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

    it('should return an error if the ledger is not found: /v1/ledgers/:ledger_index', function(done) {
      var url = 'http://localhost:' + port + '/v1/ledgers/20000';
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

    it('should return ledgers by ledger hash: /v1/ledgers/:ledger_hash', function(done) {
      var url = 'http://localhost:' + port + '/v1/ledgers/b5931ad267e59306769309aff13fccd55c2ef944e99228c8f2eeec5d3b49234d';
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
        assert.strictEqual(body.ledger.ledger_hash, 'b5931ad267e59306769309aff13fccd55c2ef944e99228c8f2eeec5d3b49234d');
        assert.strictEqual(body.ledger.transactions, undefined);
        done();
      });
    });

    it('should return an error if the hash is invald: /v1/ledgers/:ledger_hash', function(done) {
      var url = 'http://localhost:' + port + '/v1/ledgers/b59';
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

    it('should return ledgers by date: /v1/ledgers/:date', function(done) {
      var url = 'http://localhost:' + port + '/v1/ledgers/2015-01-14 17:43:10';
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
      var url = 'http://localhost:' + port + '/v1/ledgers';
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
      var url = 'http://localhost:' + port + '/v1/ledgers';
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
      var url = 'http://localhost:' + port + '/v1/ledgers';
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

  /*** accounts:/account/transactions API endpoint ***/

  describe('/v1/accounts/:account/transactions', function() {


    it('should return the last 20 transactions', function(done) {
      var account = 'rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B';
      var url = 'http://localhost:' + port + '/v1/accounts/' + account + '/transactions';

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

    it('should return a count of transactions', function(done) {
      var account = 'rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B';
      var url = 'http://localhost:' + port + '/v1/accounts/' + account + '/transactions';

      request({
        url: url,
        json: true,
        qs: {
          count : true
        }
      },
      function (err, res, body) {
        assert.ifError(err);
        assert.strictEqual(typeof body, 'object');
        assert.strictEqual(body.result, 'success');
        assert.strictEqual(body.count, 107);
        assert.strictEqual(typeof body.transactions, 'undefined');
        done();
      });
    });

    it('should return limit the returned transactions to 5', function(done) {
      var account = 'rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B';
      var url = 'http://localhost:' + port + '/v1/accounts/' + account + '/transactions';

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
      var url = 'http://localhost:' + port + '/v1/accounts/' + account + '/transactions';

      request({
        url: url,
        json: true,
        qs: {
          type : 'OfferCreate,OfferCancel',
        }
      },
      function (err, res, body) {
        assert.ifError(err);
        assert.strictEqual(typeof body, 'object');
        assert.strictEqual(body.result, 'success');
        assert.strictEqual(body.count, 20);
        body.transactions.forEach(function(tx) {
          assert(['OfferCreate','OfferCancel'].indexOf(tx.tx.TransactionType) !== -1);
        });
        done();
      });
    });

    it('should return return only specified transaction results', function(done) {
      var account = 'rfZ4YjC4CyaKFx9cgzYNKk4E2zTXRJif26';
      var result  = 'tecUNFUNDED_OFFER';
      var url = 'http://localhost:' + port + '/v1/accounts/' + account + '/transactions';

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
      var url = 'http://localhost:' + port + '/v1/accounts/' + account + '/transactions';

      request({
        url: url,
        json: true,
        qs: {
          start : '2015-01-14 18:27:10',
          end   : '2015-01-14 18:27:20',
        }
      },
      function (err, res, body) {
        assert.ifError(err);
        assert.strictEqual(typeof body, 'object');
        assert.strictEqual(body.result, 'success');
        assert.strictEqual(body.count, 8);
        assert.strictEqual(body.transactions.length, 8);
        done();
      });
    });

    it('should return transactions for a given range of ledgers', function(done) {
      var account = 'rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B';
      var url = 'http://localhost:' + port + '/v1/accounts/' + account + '/transactions';
      var max = 11119607;
      var min = 11119603;

      request({
        url: url,
        json: true,
        qs: {
          ledger_min : min,
          ledger_max : max,
        }
      },
      function (err, res, body) {
        assert.ifError(err);
        assert.strictEqual(typeof body, 'object');
        assert.strictEqual(body.result, 'success');
        assert.strictEqual(body.transactions.length, 11);
        body.transactions.forEach(function(tx) {
          assert(tx.ledger_index >= min);
          assert(tx.ledger_index <= max);
        });
        done();
      });
    });

    it('should return return results in binary form', function(done) {
      var account = 'rfZ4YjC4CyaKFx9cgzYNKk4E2zTXRJif26';
      var url = 'http://localhost:' + port + '/v1/accounts/' + account + '/transactions';

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
      var url  = 'http://localhost:' + port + '/v1/accounts/' + account + '/transactions';
      var last = 0;

      request({
        url: url,
        json: true,
        qs: {
          descending:false,
          limit:200
        }
      },
      function (err, res, body) {
        assert.ifError(err);
        assert.strictEqual(typeof body, 'object');
        assert.strictEqual(body.result, 'success');
        assert.strictEqual(body.transactions.length, 179);
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
      var url = 'http://localhost:' + port + '/v1/accounts/' + account + '/transactions/' + sequence;

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
      var url = 'http://localhost:' + port + '/v1/accounts/' + account + '/transactions';
      var max  = 11370364;
      var min  = 11370357;
      var last = max + 1;
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
          assert.strictEqual(tx.tx.Sequence, last-1);
          assert(tx.tx.Sequence <= max);
          assert(tx.tx.Sequence >= min);
          last = tx.tx.Sequence;
        });
        done();
      });
    });

    it('should return a count of account transactions by sequence', function(done) {
      var account  = 'rHsZHqa5oMQNL5hFm4kfLd47aEMYjPstpg';
      var url = 'http://localhost:' + port + '/v1/accounts/' + account + '/transactions';
      var max  = 11370364;
      var min  = 11370357;
      var last = max + 1;
      request({
        url: url,
        json: true,
        qs: {
          min_sequence : min,
          max_sequence : max,
          count : true
        }
      },
      function (err, res, body) {
        assert.ifError(err);
        assert.strictEqual(typeof body, 'object');
        assert.strictEqual(body.result, 'success');
        assert.strictEqual(body.count, 8);
        assert.strictEqual(typeof body.transactions, 'undefined');
        done();
      });
    });

    it('should return an error if the transaction is not found', function(done) {
      var account  = 'rHsZHqa5oMQNL5hFm4kfLd47aEMYjPstpg';
      var sequence = 10000;
      var url = 'http://localhost:' + port + '/v1/accounts/' + account + '/transactions/' + sequence;

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
      var url = 'http://localhost:' + port + '/v1/accounts/' + account + '/transactions';

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
        assert.strictEqual(body.message, 'invalid start time, format must be ISO 8601');
        done();
      });
    });

    it('should return an error for an invalid max or min ledger index', function(done) {
      var account = 'rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B';
      var url = 'http://localhost:' + port + '/v1/accounts/' + account + '/transactions';
      var max = 'zzz1';
      var min = 'zzz2';

      request({
        url: url,
        json: true,
        qs: {
          ledger_min : min,
          ledger_max : max,
        }
      },
      function (err, res, body) {
        assert.ifError(err);
        assert.strictEqual(res.statusCode, 400);
        assert.strictEqual(typeof body, 'object');
        assert.strictEqual(body.result, 'error');
        assert.strictEqual(body.message, 'invalid ledger_min');
        done();
      });
    });
  });

  /*** transactions/:tx_hash API endpoint ***/

  describe('/v1/transactions/:tx_hash', function() {

    it('should return a transaction given a transaction hash', function(done) {
      var hash = '22F26CE4E2270CE3CF4EB61C609E7ADEDCD41D4C1BA2D96D680A9B016C4F47DA';
      var url = 'http://localhost:' + port + '/v1/transactions/' + hash;

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
      var url = 'http://localhost:' + port + '/v1/transactions/' + hash;

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
      var url = 'http://localhost:' + port + '/v1/transactions/' + hash;

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
      var url = 'http://localhost:' + port + '/v1/transactions/' + hash;

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
  });
});
