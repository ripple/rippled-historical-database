var Parser   = require('../lib/ledgerParser');
var Importer = require('../lib/ripple-importer');
var fs       = require('fs');
var live     = new Importer({
  ripple : {
    "trace"                 : false,
    "allow_partial_history" : false,
    "servers" : [
      { "host" : "s-west.ripple.com", "port" : 443, "secure" : true },
      { "host" : "s-east.ripple.com", "port" : 443, "secure" : true }
    ]
  }});

var path         = __dirname + '/transactions/';
var EPOCH_OFFSET = 946684800;

/*
var tx = JSON.parse(fs.readFileSync(path + 'demmurage-IOU.json', "utf8"));
var parsed;

tx.metaData = tx.meta;
tx.executed_time = tx.date + EPOCH_OFFSET;
parsed = Parser.parseTransaction(tx);

console.log(parsed.exchanges);

tx = JSON.parse(fs.readFileSync(path + 'demmurage-XRP.json', "utf8"));
tx.metaData = tx.meta;
tx.executed_time = tx.date + EPOCH_OFFSET;
parsed = Parser.parseTransaction(tx);

console.log(parsed.exchanges);
*/

tx = JSON.parse(fs.readFileSync(path + 'demmurage-XRP2.json', "utf8"));

tx.metaData = tx.meta;
tx.executed_time = tx.date + EPOCH_OFFSET;
parsed = Parser.parseTransaction(tx);

//console.log(parsed.offers);

//start import stream
live.backFill(10000000, 10000100);
live.on('ledger', function(ledger) {
  console.log(ledger.ledger_index);

  var parsed = Parser.parseLedger(ledger);
  console.log(parsed.exchanges);
  return;

  ledger.transactions.forEach(function(tx) {
    parsed = Parser.parseTransaction(tx);
    //console.log(parsed.offers);
  });
});
