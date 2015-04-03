var assert  = require('assert');
var Parser  = require('../lib/ledgerParser');
var Rest    = require('../lib/hbase/hbase-rest');
var HBase   = require('../lib/hbase/hbase-client');
var Promise = require('bluebird');
var fs      = require('fs');
var PREFIX  = 'TEST_' + Math.random().toString(36).substr(2, 5) + '_';

var rest = new Rest({
  prefix : PREFIX,
  host   : "54.164.78.183",
  port   : 20550
});

var options = {
  "logLevel" : 2,
  "prefix"   : PREFIX,
  "host"     : "54.172.205.78",
  "port"     : 9090
};

var hbase = new HBase(options);
var path  = __dirname + '/ledgers/';
var files = fs.readdirSync(path);

describe('HBASE client and API endpoints', function () {
  before(function(done){
    this.timeout(60000);
    console.log('creating tables in HBASE');
    rest.initTables(function(err, resp) {
      assert.ifError(err);
      done();
    });
  });

  it('should save ledgers into hbase', function(done) {
    this.timeout(60000);
    Promise.map(files, function(filename) {
      return new Promise(function(resolve, reject) {
        var ledger = JSON.parse(fs.readFileSync(path + filename, "utf8"));
        var parsed = Parser.parseLedger(ledger);

        hbase.saveParsedData({data:parsed}, function(err, resp) {
          assert.ifError(err);
          hbase.saveTransactions(parsed.transactions, function(err, resp) {
            assert.ifError(err);
            hbase.saveLedger(parsed.ledger, function(err, resp) {
              assert.ifError(err);
              console.log(ledger.ledger_index, 'saved');
              resolve();
            });
          });
        });
      });
    }).nodeify(function(err, resp) {
      assert.ifError(err);
      console.log(resp.length, 'ledgers saved');
      done();
    });
  });

  after(function(done) {
    this.timeout(60000);
    console.log('removing tables');
    rest.removeTables(function(err, resp) {
      done();
    });
  });
});
