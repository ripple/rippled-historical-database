var log      = require('../../lib/log')('api');
var response = require('response');
var postgres;

var accountTxSeq = function (req, res, next) {

  var options = prepareOptions();
  
  log.info('ACCOUNT TX:', options.account); 

  postgres.getAccountTxSeq(options, function(err, resp) {
    if (err) {
      errorResponse(err);   
    } else if (resp.transactions.length === 0) {
      errorResponse({error: "transaction not found", code:404})
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
      sequence     : req.params.sequence,
      binary       : !req.query.binary || req.query.binary === 'false' ? false : true
    };

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
      response.json({result:'error', message:'unable to retrieve transaction'}).status(500).pipe(res);  
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
      transaction : data.transactions[0]
    };
    
    log.info('ACCOUNT TX: Transaction Found');  
    response.json(result).pipe(res);      
  };
}

module.exports = function(db) {
  postgres = db;
  return accountTxSeq;
};
