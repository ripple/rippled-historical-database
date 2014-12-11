var config = require('../../config/import.config');
var HistoricalImport = require('./history');
var h = new HistoricalImport();
var start = config.get('startIndex') || 'validated';
var stop  = config.get('stopIndex');

//start should always a higher index than stop
h.start(start, stop, function(){
  process.exit();
});