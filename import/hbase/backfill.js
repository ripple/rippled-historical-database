var config = require('../../config/import.config');
var HistoricalImport = require('./history');
var h = new HistoricalImport();
var start = config.get('startIndex');
var stop  = config.get('stopIndex') || 'validated';

//start should always be a lower index than stop
h.start(start, stop, function(){
  process.exit();
});