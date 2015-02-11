var config   = require('../../config/api.config');
var log      = require('../../lib/log')('api');
var postgres = new require('../lib/db.js')(config.get('sql'));
var response = require('response');

var accountTxSeq = function (req, res, next) {

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

module.exports = accountTxSeq;
