var Logger   = require('../../lib/logger');
var log      = new Logger({scope : 'get account balance changes'});
var moment   = require('moment');
var response = require('response');

var accountBalances = function(hbase) {
  self = this;

self.getChanges = function (req, res, next) {
  var options = prepareOptions();

  log.info("ACCOUNT BALANCE CHANGE:", options.account);

  hbase.getAccountBalanceChanges(options, function(err, changes) {
    if (err) {
      errorResponse(err);

    } else {
      changes.rows.forEach(function(ex) {
        delete ex.rowkey;
        delete ex.client;
        delete ex.account;
      });

      successResponse(changes);
    }
  });

  function prepareOptions() {
    
    var options = {
      account  : req.params.address,
      currency : req.query.currency,
      issuer   : req.query.issuer,
      limit    : req.query.limit,
      start    : req.query.start,
      end      : req.query.end,
      marker   : req.query.marker
    }

    if (!options.end)   options.end   = moment.utc('9999-12-31');
    if (!options.start) options.start = moment.utc(0);

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
  * @param {Object} balance changes
  */
  function successResponse (changes) {
    var result = {
      result          : "success",
      count           : changes.rows.length,
      marker          : changes.marker,      
      balance_changes : changes.rows
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
