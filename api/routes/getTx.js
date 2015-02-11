var config   = require('../../config/api.config');
var log      = require('../../lib/log')('api');
var postgres = new require('../lib/db.js')(config.get('sql'));
var response = require('response');

var getTx = function (req, res, next) {

  var options = prepareOptions();
  
  log.info('TX:', options.tx_hash); 

  postgres.getTx(options, function(err, ledger){
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
      tx_hash : req.params.tx_hash,
      binary  : !req.query.binary || req.query.binary === 'false' ? false : true 
    };

    return options;
  }

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
  function successResponse (transaction) {
    var result = {
      result       : 'success',
      transaction : transaction
    };
    
    log.info('TX: Transaction Found.');
    response.json(result).pipe(res);      
  }

};

module.exports = getTx;
