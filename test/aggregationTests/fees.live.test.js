var Aggregation  = require('../../lib/aggregation/fees');
var Parser       = require('../../lib/ledgerParser');
var Hbase        = require('../../lib/hbase/hbase-client');
var utils        = require('../../lib/utils');
var Importer     = require('../../lib/ripple-importer');
var config       = require('../config');
var moment       = require('moment');
var options = {
  logLevel : 3,
  hbase: config.get('hbase'),
  ripple: config.get('ripple')
};

options.hbase.logLevel = 3;

var EPOCH_OFFSET = 946684800;
var live  = new Importer(options);
var fees = new Aggregation(options.hbase);

live.liveStream();
live.on('ledger', function (ledger) {
  processLedger(ledger);
});


function processLedger (ledger) {

  var parsed = Parser.parseLedger(ledger);
  fees.handleFeeSummary(parsed.feeSummary)
  .then(function() {
    console.log('saved', parsed.feeSummary);
  })
  .catch(function(e){
    console.log(e);
  });
}
