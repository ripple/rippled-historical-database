var db = require('./db');
var winston = require('winston');
var Stream  = require('./ledgerStream');
var stream  = new Stream();
var last;
var lastHash;

//stream.backFill();
stream.liveStream();
stream.on('ledger', function(ledger) {
  console.log(ledger.ledger_index);
/*
  db.saveLedger(ledger, function(err, resp){
    
  });
*/  
});

