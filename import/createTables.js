var Rest = require('../lib/hbase/hbase-rest');
var config = require('../config');
var rest = new Rest(config.get('hbase-rest'));

rest.initTables('ledgers', function(err, resp) {
  console.log(err, resp);
  rest.initTables('validators', function(err, resp) {
    console.log(err, resp);
  });
});
