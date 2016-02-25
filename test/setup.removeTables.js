var config = require('./config');
var assert = require('assert');
var Rest = require('../lib/hbase/hbase-rest');
var restConfig = config.get('hbase-rest');

restConfig.prefix = config.get('prefix') || 'TEST_';

var rest = new Rest(restConfig);

describe('remove hbase tables', function(done) {
  it('should remove tables via rest API', function(done) {
    this.timeout(90000);
    rest.removeTables('ledgers', function(err, resp) {
      assert.ifError(err);
      done();
    });
  });
});
