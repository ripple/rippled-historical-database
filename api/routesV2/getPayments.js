var config   = require('../../storm/multilang/resources/config');
var Logger   = require('../../storm/multilang/resources/src/lib/modules/logger');
var log      = new Logger({scope : 'get payments'});
var moment   = require('moment');
var response = require('response');

var accountPayments = function(hbase) {
  self = this;

self.getPayments = function (req, res, next) {
  var options = prepareOptions();

  log.info("PAYMENTS: " + options.account);

  hbase.getPayments(options, function(err, payments) {
    if (err) errorResponse(err);
    else if
      (payments.length === 0) errorResponse({error: "no payments found", code: 404});
    else successResponse(payments);
  });

  function prepareOptions() {
    var options = {
      account : req.params.address,
      start   : req.query.start,
      end     : req.query.end,
      limit   : req.query.limit
    }
    return options;
  }

  /**
  * errorResponse
  * return an error response
  * @param {Object} err
  */

  function errorResponse(err) {
    log.error(err.error || err);
    if (err.code && err.code.toString()[0] === '4') {
      response.json({result: 'error', message: err.error})
        .status(err.code).pipe(res);
    } else {
      response.json({result: 'error', message: 'unable to retrieve payments'})
        .status(500).pipe(res);
    }
  }

  /**
  * successResponse
  * return a successful response
  * @param {Object} payments
  */

  function successResponse(payments) {
    var result = {
      result: 'success',
      count: payments.length,
      payments: payments
    };

    response.json(result).pipe(res);
  }
};

  return this;
};

module.exports = function(db) {
  ap = accountPayments(db);
  return ap.getPayments;
};
