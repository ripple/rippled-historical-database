var config     = require('../config/api.config');
var express    = require('express');
var bodyParser = require('body-parser');
var cors       = require('cors');
var postgres   = require('./lib/db')(config.get('sql'));
var routes     = require('./routes')({
  postgres : postgres
});

var app = express();
app.use(bodyParser.json());
app.use(cors());

//define routes
app.get('/v1/accounts/:address/transactions', routes.accountTx);
app.get('/v1/accounts/:address/transactions/:sequence', routes.accountTxSeq);
app.get('/v1/ledgers/:ledger_param?', routes.getLedger);
app.get('/v1/accounts/:address/balances', routes.accountBalances);
app.get('/v1/transactions/:tx_hash', routes.getTx);

//start the server
app.listen(config.get('port'));
console.log('Ripple Data API running on port ' + config.get('port'));

