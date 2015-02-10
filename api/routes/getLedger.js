var config   = require('../../config/api.config');
var log      = require('../../lib/log')('api');
var postgres = new require('../lib/db.js')(config.get('sql'));
var moment  = require('moment');
var response = require('response');

var getLedger = function (req, res, next) {

  var options = prepareOptions();

  if (options.ledger_index) log.info('LEDGER:', options.ledger_index); 
  else if (options.ledger_hash) log.info('LEDGER:', options.ledger_hash); 
  else if (options.date) log.info('LEDGER:', options.date.format());
  else log.info('LEDGER: latest');  

  if (options.err) errorResponse(options.err);
  else 
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
      date         : req.query.date,
      binary       : !req.query.binary || req.query.binary === 'false' ? false : true,
      expand       : !req.query.expand || req.query.expand === 'false' ? false : true,
      transactions : !req.query.transactions || req.query.transactions === 'false' ? false : true
    };

    var ledger_param = req.params.ledger_param;
    if (ledger_param) {
      var intMatch = /^\d+$/;
      var hexMatch = new RegExp('^(0x)?[0-9A-Fa-f]+$');
      var iso = moment.utc(req.params.ledger_param, moment.ISO_8601);
      if (intMatch.test(ledger_param)) options.ledger_index = ledger_param;
      else if (iso.isValid()) options.date = iso;
      else if (hexMatch.test(ledger_param) && ledger_param.length % 2 === 0) 
        options.ledger_hash = ledger_param;
      else options.err = {error:"Invalid ledger identifier.", code:400};
    }
    
    if (options.binary) options.tx_return = 'binary';
    else if (options.expand) options.tx_return = 'json';
    else if (options.transactions) options.tx_return = 'hex';
    else options.tx_return = 'none';

    return options;
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
    } else if (err.code === 404) {
      log.error(err.error || err);
      response.json({result:'error', message:err.error}).status(404).pipe(res);
    } else {
      response.json({result:'error', message:'unable to retrieve ledger'}).status(500).pipe(res);  
    }     
  }
  
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
  }

};

module.exports = getLedger;