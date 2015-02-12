var Logger   = require('../../storm/multilang/resources/src/lib/modules/logger');
var log      = new Logger({scope : 'get tx'});
var response = require('response');
var postgres;

var log = new Logger({
  scope : 'validator'
});

var getTx = function (req, res, next) {
  var hexMatch = new RegExp('^(0x)?[0-9A-Fa-f]+$');
  var options  = {
    tx_hash : req.params.tx_hash,
    binary  : !req.query.binary || req.query.binary === 'false' ? false : true 
  };

  if (!hexMatch.test(options.tx_hash) || options.tx_hash.length % 2 !== 0) {
    errorResponse({error: 'invalid hash', code:400});
    return;
  }
    
  log.info('TX:', options.tx_hash); 
  
  postgres.getTx(options, function(err, ledger){
    if (err) {
      errorResponse(err);
    } else{
      successResponse(ledger);
    }
  });

  /**
  * errorResponse 
  * return an error response
  * @param {Object} err
  */
  function errorResponse (err) {
    log.error(err.error || err);
    
    if (err.code && err.code.toString()[0] === '4') {
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

module.exports = function(db) {
  postgres = db;
  return getTx;
};
