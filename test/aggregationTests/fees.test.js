var config = require('../config');
var assert = require('assert');
var Promise = require('bluebird');
var Rest = require('../../lib/hbase/hbase-rest');
var Parser = require('../../lib/ledgerParser');
var prefix = config.get('hbase:prefix');
var hbaseConfig = config.get('hbase');
var restConfig = config.get('hbase-rest');

var Aggregation = require('../../lib/aggregation/fees');
var fs = require('fs');
var path = __dirname + '/../mock/ledgers/';
var files = fs.readdirSync(path);
var EPOCH_OFFSET = 946684800;
var rest;
var fees;

hbaseConfig.prefix = prefix;
restConfig.prefix = prefix;
rest = new Rest(restConfig);
fees = new Aggregation(hbaseConfig);

console.log('# ledgers:', files.length);

describe('create Hbase tables', function(done) {
  it('should create network_fees table', function(done) {
    this.timeout(10000);
    rest.addTable('network_fees')
    .then(function() {
      done();
    })
    .catch(function(e) {
      assert.ifError(e);
    });
  });

  it('should aggregate fees', function(done) {
    this.timeout(5000);

    Promise.map(files, function(filename) {
      var ledger = JSON.parse(fs.readFileSync(path + filename, "utf8"));
      var data;

      ledger.close_time += EPOCH_OFFSET;

      data = Parser.summarizeFees(ledger);
      return fees.handleFeeSummary(data);
    }).then(function() {
      done();
    }).catch(function(e) {
      assert.ifError(e);
    });

  });
});
