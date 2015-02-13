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
  
  it('should return the latest ledger with /v1/ledgers', function(done) {
    var url = 'http://localhost:' + port + '/v1/ledgers';
    request({
      url: url,
      json: true,
    }, 
    function (err, res, body) {
      assert.ifError(err);
      assert.strictEqual(typeof body, 'object');
      assert.strictEqual(body.result, 'success');
      assert.strictEqual(typeof body.ledger, 'object');
      assert.strictEqual(body.ledger.ledger_index, 11119607);
      assert.strictEqual(body.ledger.transactions, undefined);
      done();
    });
  }); 
});
