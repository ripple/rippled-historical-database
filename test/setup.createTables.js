var config = require('./config');
var assert = require('assert');
var Rest = require('../lib/hbase/hbase-rest');
var restConfig = config.get('hbase-rest');
var prefix = config.get('prefix');
var rest;

restConfig.prefix = prefix;
rest = new Rest(restConfig);

describe('create Hbase tables', function(done) {
  it('should create tables via rest API', function(done) {
    this.timeout(60000);
    rest.initTables('ledgers', function(err, resp) {
      assert.ifError(err);
      rest.initTables('validations', function(err, resp) {
        assert.ifError(err);
        done();
      });
    });
  });
});
