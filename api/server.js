var config     = require('../config/api.config.json');
var winston    = require('winston');
var express    = require('express');
var bodyParser = require('body-parser');

var app = express();
app.use(bodyParser.json());

//define routes
//app.get('/v1/account/:address/transactions', routes.account.transactions);

//start the server
app.listen(config.port);
winston.info('Ripple Data API running on port '+config.port);




