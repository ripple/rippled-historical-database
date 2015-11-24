'use strict';

var express = require('express');
var bodyParser = require('body-parser');
var Hbase = require('../lib/hbase/hbase-client');
var cors = require('cors');
var Postgres = require('./lib/db');
var Routes = require('./routes');
var RoutesV2 = require('./routesV2');
var generateMap = require('./apiMap');
var json2csv = require('nice-json2csv');
var favicon = require('serve-favicon');

var Server = function (options) {
  var app = express();
  var hbase;
  var routesV2;
  var postgres;
  var routes;
  var server;

  app.use(bodyParser.json());
  app.use(bodyParser.urlencoded({extended:true}));
  app.use(json2csv.expressDecorator);
  app.use(cors());
  app.use(filterDuplicateQueryParams);
  app.use(favicon(__dirname + '/favicon.png'));

  // v1 routes (requires postgres)
  if (options.postgres) {
    postgres = new Postgres(options.postgres);
    routes = Routes(postgres);

    app.get('/v1/accounts/:address/transactions', routes.accountTx);
    app.get('/v1/accounts/:address/transactions/:sequence', routes.accountTxSeq);
    app.get('/v1/accounts/:address/balances', routes.accountBalances);
    app.get('/v1/ledgers/:ledger_param?', routes.getLedger);
    app.get('/v1/transactions/:tx_hash', routes.getTx);
    app.get('/v1/last_validated', routes.getLastValidated);
  }

  // v2 routes (requires hbase)
  if (options.hbase) {
    hbase = new Hbase(options.hbase);
    routesV2 = RoutesV2(hbase);

    app.get('/v2/health/:aspect?', routesV2.checkHealth);
    app.get('/v2/gateways/:gateway?', routesV2.gateways.Gateways);
    app.get('/v2/gateways/:gateway/assets/:filename?', routesV2.gateways.Assets);
    app.get('/v2/currencies/:currencyAsset?', routesV2.gateways.Currencies);
    app.get('/v2/capitalization/:currency/:issuer', routesV2.capitalization);
    app.get('/v2/network/exchange_volume', routesV2.network.exchangeVolume);
    app.get('/v2/network/payment_volume', routesV2.network.paymentVolume);
    app.get('/v2/network/issued_value', routesV2.network.issuedValue);
    //app.get('/v2/network/top_markets', routesV2.network.topMarkets);
    //app.get('/v2/network/top_currencies', routesV2.network.topCurrencies);
    app.get('/v2/last_validated', routesV2.getLastValidated);
    app.get('/v2/transactions/', routesV2.getTransactions);
    app.get('/v2/transactions/:tx_hash', routesV2.getTransactions);
    app.get('/v2/ledgers/:ledger_param?', routesV2.getLedger);
    app.get('/v2/accounts/:address/transactions/:sequence', routesV2.accountTxSeq);
    app.get('/v2/accounts/:address/transactions', routesV2.accountTransactions);
    app.get('/v2/accounts/:address/balances', routesV2.accountBalances);
    app.get('/v2/accounts/:address/payments/:date?', routesV2.accountPayments);
    app.get('/v2/accounts/:address/reports/:date?', routesV2.accountReports);
    app.get('/v2/accounts/:address/balance_changes', routesV2.getChanges);
    app.get('/v2/accounts/:address/exchanges', routesV2.accountExchanges);
    app.get('/v2/accounts/:address/exchanges/:base', routesV2.accountExchanges);
    app.get('/v2/accounts/:address/exchanges/:base/:counter', routesV2.accountExchanges);
    // app.get('/v2/accounts/:address/offers', routesV2.accountOffers);
    app.get('/v2/accounts/:address', routesV2.getAccount);
    app.get('/v2/accounts', routesV2.accounts);
    app.get('/v2/exchanges/:base/:counter', routesV2.getExchanges);
    app.get('/v2/exchange_rates/:base/:counter', routesV2.getExchangeRate);
    app.get('/v2/normalize', routesV2.normalize);
    app.get('/v2/reports/:date?', routesV2.reports);
    app.get('/v2/stats', routesV2.stats);
    app.get('/v2/stats/:family', routesV2.stats);
    app.get('/v2/stats/:family/:metric', routesV2.stats);
    // app.get('/v2/payments/:date?', routesV2.payments);
  }

  // index page
  app.get('/', generateMap);
  app.get('/v2', generateMap);

  // start the server
  server = app.listen(options.port);
  console.log('Ripple Data API running on port: ' + options.port);

  // log error
  server.on('error', function(err) {
    console.log(err);
  });

  // log close
  server.on('close', function () {
    console.log('server on port: ' + options.port + ' closed');
  });

  this.close = function () {
    if (server) {
      server.close();
      console.log('closing API on port: ' + options.port);
    }
  };
};

/**
 * filterDuplicateQueryParams
 * NOTE: this only works if we dont pass
 * an array as a query param intentionally
 */

function filterDuplicateQueryParams(req, res, next) {

  for (var key in req.query) {
    if (Array.isArray(req.query[key])) {
      req.query[key] = req.query[key][0];
    }
  }

  next();
}

module.exports = Server;

