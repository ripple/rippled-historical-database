var config   = require('../../config/api.config');
var log      = require('../../lib/log')('api');
var postgres = new require('../lib/db.js')(config.get('sql'));
var response = require('response');

var getLedger = function (req, res, next) {

  var options = prepareOptions();

  if (options.ledger_index) log.info('LEDGER:', options.ledger_index); 
  else if (options.ledger_hash) log.info('LEDGER:', options.ledger_hash); 
  else if (options.datetime) log.info('LEDGER:', options.datetime);
  else log.info('LEDGER: latest');  

  postgres.getLedger(options, function(err, ledger){
    if (err) {
      errorResponse(err);
    } else{
      successResponse(ledger);
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
      datetime : req.query.datetime
    };

    var ledger_param = req.params.ledger_param,
        reg = /^\d+$/;
    if (reg.test(ledger_param)) options.ledger_index = ledger_param;
    else options.ledger_hash = ledger_param;
      
    if (!req.query.transactions) {
      options.tx_return = 'none';
    }
    else if (req.query.transactions && !req.query.expand) {
      options.tx_return = 'hex';
    } 
    else if (req.query.transactions && req.query.expand && req.query.binary) {
      options.tx_return = 'binary';
    }
    else if (req.query.transactions && req.query.expand && !req.query.binary) {
      options.tx_return = 'json';
    }
    return options;
  };

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
      response.json({result:'error', message:'unable to retrieve transactions'}).status(500).pipe(res);  
    }     
  };
  
 /**
  * successResponse
  * return a successful response
  * @param {Object} ledger
  */  
  function successResponse (ledger) {
    var result = {
      result : 'success',
      ledger : ledger
    };
    if (ledger.transactions)
      log.info('LEDGER: Ledger Found with', ledger.transactions.length, 'transactions.');
    else
      log.info('LEDGER: Ledger Found.');
    response.json(result).pipe(res);      
  };

}

module.exports = getLedger;