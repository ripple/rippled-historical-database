var config = require('./config');
var assert = require('assert');
var Rest = require('../lib/hbase/hbase-rest');
var restConfig = config.get('hbase-rest');
var HBase = require('../lib/hbase/hbase-client');

var hbaseConfig = config.get('hbase');
var prefix = config.get('prefix') || 'TEST_';
var port = config.get('port') || 7111;
var hbase;
var server;
var rest;

restConfig.prefix = prefix;
rest = new Rest(restConfig);

hbaseConfig.prefix = prefix;
hbaseConfig.logLevel = 2;
hbaseConfig.max_sockets = 500;
hbaseConfig.timeout = 30000;

describe('create Hbase tables', function(done) {
  it('should create tables via rest API', function(done) {
    this.timeout(60000);
    rest.initTables('ledgers', function(err, resp) {
      assert.ifError(err);
      done();
    });
  });
});
