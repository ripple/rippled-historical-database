var config   = require('../../config/import.config');
var log      = require('../../lib/log')('api');
var postgres = new require('../lib/db.js')(config.get('sql'));
var response = require('response');

var accountTx = function (req, res, next) {

  var options = prepareOptions();

  postgres.getAccountTransactions(options, function(err, transactions) {
    if (err) {
      errorResponse(err);   
    } else {
      successResponse(transactions); 
    }
  });
  
 /**
  * 
  * prepareOptions
  * parse request parameters to determine query options 
  */
  function prepareOptions () {
    var options = {
      account    : req.params.address,
      limit      : req.query.limit || 10,
      offset     : req.query.offset,
      descending : req.query.descending === 'false' ? false : true,
      start      : req.query.start,
      end        : req.query.end,
      type       : req.query.type,
      result     : req.query.result
    };
    
    if (isNaN(options.limit)) {
      options.limit = 10;
        
    } else if (options.limit > 1000) {
      options.limit = 1000;  
    } 
    
    return options;
  };
  
 /**
  * 
  * errorResponse 
  * return an error response
  * @param {Object} err
  */
  function errorResponse (err) {
    if (err.code === 400) {
      log.error(err);
      response.json({result:'error', message:err.error}).status(400).pipe(res);  
       
    } else {
      response.json({result:'error', message:'unable to retrieve transactions'}).status(500).pipe(res);  
    }     
  };
  
 /**
  * 
  * successResponse
  * return a successful response
  * @param {Object} transactions
  */  
  function successResponse (transactions) {
    var result = {
      result       : 'success',
      count        : transactions.length,
      transactions : transactions
    };
      
    response.json(result).pipe(res);      
  };
}

module.exports = accountTx;
