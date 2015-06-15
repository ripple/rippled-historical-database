var config = require('../../config/import.config');
var assert = require('assert');
var Parser = require('../../lib/ledgerParser');
var Rest = require('../../lib/hbase/hbase-rest');
var HBase = require('../../lib/hbase/hbase-client');
var Promise = require('bluebird');
var fs = require('fs');
var path = __dirname + '/../ledgers/';
var files = fs.readdirSync(path);
var hbaseConfig = config.get('hbase');
var hbase;

hbaseConfig.prefix = config.get('prefix') || 'TEST_';
hbaseConfig.logLevel = 2;
hbaseConfig.max_sockets = 200;
hbaseConfig.timeout = 30000;

hbase = new HBase(hbaseConfig);

describe('import ledgers', function(done) {
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
      done(err);
    });
  });
});
