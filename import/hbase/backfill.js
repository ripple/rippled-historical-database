var config = require('../../config/import.config');
var log    = require('../../lib/log')('hbase backfill');
var HistoricalImport = require('./history');
var h = new HistoricalImport();
var start = config.get('startIndex') || 'validated';
var stop  = config.get('stopIndex');

setTimeout(function() {
  h.start(stop, start, function(){
    process.exit();
  });
}, 500);
