var config   = require('../../config/api.config');
var log      = require('../../lib/log')('api');
var postgres = new require('../lib/db.js')(config.get('sql'));
var response = require('response');

var accountTx = function (req, res, next) {

  var options = prepareOptions();
  
  log.info('ACCOUNT TX:', options.account); 

  postgres.getAccountTransactions(options, function(err, resp) {
    if (err) {
      errorResponse(err);   
    } else {
      successResponse(resp); 
    }
  });
  
 /**
  * prepareOptions
  * parse request parameters to determine query options 
  */
  function prepareOptions () {
    var options = {
      account      : req.params.address,
      limit        : req.query.limit || 20,
      offset       : req.query.offset,
      descending   : req.query.descending === 'false' ? false : true,
      start        : req.query.start,
      end          : req.query.end,
      minLedger    : req.query.ledger_min,
      maxLedger    : req.query.ledger_max,
      type         : req.query.type,
      result       : req.query.result,
      binary       : !req.query.binary || req.query.binary === 'false' ? false : true,
      min_sequence : req.query.min_sequence,
      max_sequence : req.query.max_sequence
    };

    if (isNaN(options.limit)) {
      options.limit = 20;
        
    } else if (options.limit > 1000) {
      options.limit = 1000;  
    } 

    return options;
  };
  
 /**
  * errorResponse 
  * return an error response
  * @param {Object} err
  */
  function errorResponse (err) {
    if (err.code.toString()[0] === '4') {
      log.error(err.error || err);
      response.json({result:'error', message:err.error}).status(err.code).pipe(res);
    } else {
      response.json({result:'error', message:'unable to retrieve transactions'}).status(500).pipe(res);  
    }     
  }
  
 /**
  * successResponse
  * return a successful response
  * @param {Object} transactions
  */  
  function successResponse (data) {
    var result = {
      result       : 'success',
      count        : data.transactions.length,
      total        : data.total,
      transactions : data.transactions
    };
    
    log.info('ACCOUNT TX: Transactions Found:', data.transactions.length);  
    response.json(result).pipe(res);      
  };
}

module.exports = accountTx;
