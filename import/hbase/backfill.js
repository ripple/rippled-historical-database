var config = require('../../config/import.config');
var log    = require('../../lib/log')('hbase backfill');
var HistoricalImport = require('./history');
var h = new HistoricalImport();
var start = config.get('startIndex');
var stop  = config.get('stopIndex') || 'validated';

h.db.connect().nodeify(function(err, resp){
  if (err) {
    log.error('Unable to connect to HBASE');
    process.exit();
  } else {
    //start should always be a lower index than stop
    h.start(start, stop, function(){
      process.exit();
    });
  }
});