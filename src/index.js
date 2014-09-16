var db = require('./db');
var Stream = require('./ledgerStream');
var stream = new Stream();


stream.start();
stream.on('ledger', function(ledger){
  db.saveLedger(ledger, function(err, resp){
    
  });
});


