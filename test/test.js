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
var port     = 7111;
var server;
var db;

dbConfig.database = config.get('dbname') || 'test_db';
db     = new Postgres(dbConfig);
server = new Server({postgres:dbConfig, port:port});

describe('ETL and API:', function() {

  before(function(done) {
    console.log('migrating database...');
    db.migrate().then(function(){
      console.log('done');
      done();
    });
  });
  
  it('should save ledgers and transactions into the database', function(done) {
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
  
  it('should run up to 40 API servers simultaneously', function(done) {
    var count = 100;
    var server;
    var port;
    
    while (count-- > 60) {
      port   = '322' + count;
      server = new Server({
        postgres : dbConfig, 
        port     : port
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
  
  describe('/v1/ledgers', function() {
    it('should return the latest ledger: /v1/ledgers', function(done) {
      var url = 'http://localhost:' + port + '/v1/ledgers';
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
        assert.strictEqual(body.ledger.ledger_index, 11119607);
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
});
