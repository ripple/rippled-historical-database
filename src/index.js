var Stream = require('./ledgerStream');
var stream = new Stream();

stream.start();
stream.on('ledger', function(ledger){
  console.log(ledger.ledger_index);
});
