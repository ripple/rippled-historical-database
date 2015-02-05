var config   = require('../../config/api.config');
var log      = require('../../lib/log')('api');
var postgres = new require('../lib/db.js')(config.get('sql'));
var request  = require('request');
var response = require('response');

var accountBalances = function (req, res, next) {

  var options = prepareOptions();

  log.info('ACCOUNT BALANCES:', options.account); 

  postgres.getLedger(options, function(err, ledger){
    if (err) {
      errorResponse(err);
    } else {
      getBalances(ledger.ledger_index, options.account);
    }
  });

  /**
  * prepareOptions
  * parse request parameters to determine query options 
  */
  function prepareOptions () {
    var options = {
      ledger_index : req.query.ledger_index,
      ledger_hash  : req.query.ledger_hash,
      datetime     : req.query.datetime,
      currency     : req.query.currency,
      counterparty : req.query.counterparty,
      limit        : req.query.limit,
      marker       : req.query.marker,
      tx_return    : 'none',
      account      : req.params.address
    };

    return options;
  };

  /**
  * getBalances
  * use ledger_index from getLedger api call
  * to get balances using ripple REST 
  */
 function getBalances(ledger_index, account) {
    if (!account) errorResponse({error: 'Must provide account.', code:400});
    var url = 'https://api.ripple.com/v1/accounts/'+account+'/balances';
    request({
        url: url,
        qs: {
          currency: options.currency,
          counterparty: options.counterparty,
          limit: options.limit,
          marker: options.marker,
          ledger: ledger_index
        }
      }, 
      function (err, res, body) {
      if (err) errorResponse(err);
      else {
        try {
          var balances = JSON.parse(body);
        }
        catch(err) {
          errorResponse({error: 'Could not parse json', code:400})
        }
        successResponse(balances);
      }
    });
 }

 /**
  * errorResponse 
  * return an error response
  * @param {Object} err
  */
  function errorResponse (err) {
    if (err.code === 400) {
      log.error(err.error || err);
      response.json({result:'error', message:err.error}).status(400).pipe(res);  
       
    } else {
      response.json({result:'error', message:'unable to retrieve balances'}).status(500).pipe(res);  
    }     
  };
  
 /**
  * successResponse
  * return a successful response
  * @param {Object} balances
  */  
  function successResponse (balances) {
    log.info('ACCOUNT BALANCES: Balances Found:', balances.balances.length);
    response.json(balances).pipe(res);      
  };

}

module.exports = accountBalances;
