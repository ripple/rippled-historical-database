var config   = require('../../config/api.config');
var log      = require('../../lib/log')('api');
var postgres = new require('../lib/db.js')(config.get('sql'));
var response = require('response');

var getLedger = function (req, res, next) {

	var options = prepareOptions();

	postgres.getLedger(options, function(err, ledger){
		if (err) {
			errorResponse(err);
		} else{
			successResponse(ledger);
		}
	});

  function prepareOptions () {
    var options = {
      ledger_index : req.query.ledger_index,
      ledger_hash  : req.query.ledger_hash,
      closing_time : req.query.datetime
    };
      
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
     
    response.json(result).pipe(res);      
  };

}

module.exports = getLedger;