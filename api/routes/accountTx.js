var config   = require('../../config/import.config');
var log      = require('../../lib/log')('api');
var postgres = new require('../../lib/db.js')(config.get('sql'));
var response = require('response');

var accountTx = function (req, res, next) {

  var options = {
    account    : req.params.address,
    limit      : req.query.limit,
    offset     : req.query.offset,
    descending : req.query.descending === 'false' ? false : true
  };  
  
  postgres.getAccountTransactions(options, function(err, transactions) {
    if (err) {
      response.json({result:'error', message:'unable to retrieve transactions'}).status(500).pipe(res); 
    
    } else {
      var result = {
        result       : 'success',
        transactions : transactions
      };
      
      response.json(result).pipe(res); 
    }
  });
}

module.exports = accountTx;
