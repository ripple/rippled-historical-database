var Logger   = require('../../storm/multilang/resources/src/lib/modules/logger');
var log      = new Logger({scope : 'account tx'});
var response = require('response');
var postgres;

var log = new Logger({
  scope : 'validator'
});

var accountTx = function (req, res, next) {

  var intMatch = /^\d+$/;
  var options = prepareOptions();
  
  if (options.minLedger && !intMatch.test(options.minLedger)) {
    errorResponse({error: 'invalid ledger_min', code:400});
    return;
  }
  
  if (options.maxLedger && !intMatch.test(options.maxLedger)) {
    errorResponse({error: 'invalid ledger_max', code:400});
    return;
  }  
  
  log.info('ACCOUNT TX:', options.account); 

  if (options.min_sequence || options.max_sequence)
    postgres.getAccountTxSeq(options, function(err, resp) {
      if (err) {
        errorResponse(err);   
      } else {
        successResponse(resp); 
      }
    });
  else
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

module.exports = function(db) {
  postgres = db;
  return accountTx;
};
