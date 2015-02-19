var express    = require('express');
var bodyParser = require('body-parser');
var Hbase      = require('../storm/multilang/resources/src/lib/hbase-client');
var config     = require('../config/api.config');
var cors       = require('cors');
var Postgres   = require('./lib/db');
var Routes     = require('./routes');

var Server = function (options) {
  var app    = express();
  var db     = new Postgres(options.postgres);
  var hb     = new Hbase(options.hbase);
  var routes = Routes({postgres : db, hbase : hb});
  var server;

  app.use(bodyParser.json());
  app.use(cors());

  //define routes
  app.get('/v1/accounts/:address/transactions', routes.accountTx);
  app.get('/v1/accounts/:address/transactions/:sequence', routes.accountTxSeq);
  app.get('/v1/ledgers/:ledger_param?', routes.getLedger);
  app.get('/v1/accounts/:address/balances', routes.accountBalances);
  app.get('/v1/transactions/:tx_hash', routes.getTx);
  app.get('/v1/accounts/:address/payments', routes.getPayments);
  app.get('/v1/accounts/:address/balances/changes', routes.getChanges);
  app.get('/v1/transactions/:base/:counter/exchanges', routes.getExchanges);

  //start the server
  server = app.listen(options.port);
  console.log('Ripple Data API running on port: ' + options.port);

  this.close = function () {
    if (server) {
      server.close();
      console.log('closing API on port: ' + options.port);
    }
  };
};

module.exports = Server;

