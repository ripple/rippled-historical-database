var config   = require('../../config/import.config');
var log      = require('../../lib/log')('server');
var postgres = new require('../../lib/db.js')(config.get('sql'));


var accountTx = function (req, res, next) {

  postgres.getAccountTransactions({address:req.params.address}, function(err, resp) {
    console.log(err, resp);
  });
}

module.exports = accountTx;
