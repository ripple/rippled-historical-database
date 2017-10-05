var config = require('../config')
config.file('defaults', __dirname + '/test_config.json')

var assert = require('assert');
var Rest = require('../lib/hbase/hbase-rest');
var restConfig = config.get('hbase-rest');
restConfig.prefix = config.get('hbase:prefix');

var rest = new Rest(restConfig);

describe('create Hbase tables', function(done) {
  it('should create tables via rest API', function(done) {
    this.timeout(60000);
    rest.initTables('ledgers', function(err, resp) {
      assert.ifError(err);
      rest.initTables('validators', function(err, resp) {
        assert.ifError(err);
        done();
      });
    });
  });
});
