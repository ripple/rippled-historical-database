var config   = require('../../storm/multilang/resources/config');
var Logger   = require('../../storm/multilang/resources/src/lib/modules/logger');
var moment   = require('moment');
var response = require('response');

var log = new Logger({
  scope : 'validator'
});

var accountBalances = function(hbase) {
  self = this;

self.getChanges = function (req, res, next) {
  var options = prepareOptions();
  
  hbase.getAccountBalanceChanges(options, function(err, payments) {
    if (err) errorResponse(err);
    else if 
      (payments.length === 0) errorResponse({error: "no balance changes found", code: 404});
    else successResponse(payments);
  });

  function prepareOptions() {
    var options = {
      account  : req.params.address,
      currency : req.query.currency,
      issuer   : req.query.issuer,
      limit    : req.query.limit,
      start    : req.query.start,
      end      : req.query.end
    }

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
      response.json({result:'error', message:'unable to retrieve balance changes'}).status(500).pipe(res);  
    }     
  }

  /**
  * successResponse
  * return a successful response
  * @param {Object} payments
  */  
  function successResponse (changes) {
    var result = {
      result          : "sucess",
      count           : changes.length,
      balance_changes : changes
    };

    response.json(result).pipe(res);      
  }

};

  return this;
}

module.exports = function(db) {
  abc = accountBalances(db);
  return abc.getChanges;
};
