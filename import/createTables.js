var Rest    = require('../storm/multilang/resources/src/lib/modules/hbase-rest');
var config  = require('../config/import.config');
var rest    = new Rest(config.get('hbase-rest'));


rest.initTables(function(err, resp) {
  console.log(err, resp);
});