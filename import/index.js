var config   = require('../config/import.config');
var winston  = require('winston');
var Import   = require('./importer');
var importer = new Import(config);
var db       = require('../lib/db')(config);  

//importer.backFill();
importer.liveStream();
importer.on('ledger', function(ledger) {
  console.log(ledger.ledger_index); 
});