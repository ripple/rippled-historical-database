var express    = require('express');
var bodyParser = require('body-parser');
var Hbase      = require('../lib/hbase/hbase-client');
var config     = require('../config/api.config');
var cors       = require('cors');
var Postgres   = require('./lib/db');
var Routes     = require('./routes');
var json2csv   = require('nice-json2csv');

var Server = function (options) {
  var app    = express();
  var db     = new Postgres(options.postgres);
  var hb     = new Hbase(options.hbase);
  var routes = Routes({postgres : db, hbase : hb});
  var server;

  app.use(bodyParser.json());
  app.use(bodyParser.urlencoded({extended:true}));
  app.use(json2csv.expressDecorator);
  app.use(cors());

  //define routes
  app.get('/v1/accounts/:address/transactions', routes.accountTx);
  app.get('/v1/accounts/:address/transactions/:sequence', routes.accountTxSeq);
  app.get('/v1/accounts/:address/payments/:date?', routes.accountPayments);
  app.get('/v1/accounts/:address/reports/:date?',  routes.accountReports);
  app.get('/v1/accounts/:address/balance_changes', routes.getChanges);
  app.get('/v1/accounts/:address/exchanges', routes.accountExchanges);
  app.get('/v1/accounts/:address/exchanges/:base', routes.accountExchanges);
  app.get('/v1/accounts/:address/exchanges/:base/:counter', routes.accountExchanges);
  //app.get('/v1/accounts/:address/offers', routes.accountOffers);
  app.get('/v1/accounts/:address/balances', routes.accountBalances);
  app.get('/v1/accounts/:address', routes.getAccount);
  app.get('/v1/accounts', routes.accounts);
  app.get('/v1/ledgers/:ledger_param?', routes.getLedger);
  app.get('/v1/transactions/:tx_hash', routes.getTx);
  app.get('/v1/exchanges/:base/:counter', routes.getExchanges);
  app.get('/v1/reports/:date?', routes.reports);
  app.get('/v1/stats', routes.stats);
  app.get('/v1/stats/:family', routes.stats);
  app.get('/v1/stats/:family/:metric', routes.stats);

  //app.get('/v1/payments/:date?', routes.payments);
  app.get('/v1/last_validated', routes.getLastValidated);

  //start the server
  server = app.listen(options.port);
  console.log('Ripple Data API running on port: ' + options.port);

  //hb.getStats({interval:'hour'}).nodeify(function(err, resp){console.log(err, resp)});

  this.close = function () {
    if (server) {
      server.close();
      console.log('closing API on port: ' + options.port);
    }
  };
};

module.exports = Server;

