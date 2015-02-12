var config = require('../../config/import.config');
var HistoricalImport = require('./history');
var h = new HistoricalImport();
var start = config.get('startIndex') || 'validated';
var stop  = config.get('stopIndex');

setTimeout(function() {
  h.start(stop, start, function(){
    process.exit();
  });
}, 500);
