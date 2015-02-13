var express    = require('express');
var bodyParser = require('body-parser');
var cors       = require('cors');
var Postgres   = require('./lib/db');
var Routes     = require('./routes');

var Server = function (options) {
  var app    = express();
  var db     = new Postgres(options.postgres);
  var routes = Routes({postgres : db});
  var server;
  
  app.use(bodyParser.json());
  app.use(cors());

  //define routes
  app.get('/v1/accounts/:address/transactions', routes.accountTx);
  app.get('/v1/accounts/:address/transactions/:sequence', routes.accountTxSeq);
  app.get('/v1/ledgers/:ledger_param?', routes.getLedger);
  app.get('/v1/accounts/:address/balances', routes.accountBalances);
  app.get('/v1/transactions/:tx_hash', routes.getTx);

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



