var Logger   = require('../../lib/logger');
var log      = new Logger({scope : 'account payments'});
var moment   = require('moment');
var response = require('response');
var hbase;

/**
 * AccountPayments
 */

var AccountPayments = function (req, res, next) {
  var options = prepareOptions();

  if (options.error) {
    errorResponse(options);
    return;

  } else {
    log.info("get: " + options.account);

    hbase.getAccountPayments(options, function(err, payments) {
      if (err) {
        errorResponse(err);
      } else {
        payments.rows.forEach(function(p) {
          delete p.rowkey;
          delete p.tx_index;
          delete p.client;
          p.executed_time = moment.unix(p.executed_time).utc().format();
        });

        successResponse(payments);
      }
    });
  }

  /**
   * prepareOptions
   */

  function prepareOptions() {
    var options = {
      account    : req.params.address,
      start      : req.query.start,
      end        : req.query.end,
      type       : req.query.type,
      currency   : req.query.currency,
      marker     : req.query.marker,
      descending : (/false/i).test(req.query.descending) ? false : true,
      limit      : Number(req.query.limit) || 200,
    };

    if (!options.account) {
      return {error: 'Account is required', code:400};
    }

    if (req.params.date) {
      options.start = moment.utc(req.params.date).startOf('day');
      options.end   = moment.utc(req.params.date).startOf('day').add(1, 'day');

    } else {
      if (!options.end)   options.end   = moment.utc('9999-12-31');
      if (!options.start) options.start = moment.utc(0);
    }

    if (options.limit > 1000) {
      return {error:'limit cannot exceed 1000', code:400};
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
      response.json({result:'error', message:'unable to retrieve payments'}).status(500).pipe(res);
    }
  }

  /**
  * successResponse
  * return a successful response
  * @param {Object} payments
  */

  function successResponse (payments) {
    var result = {
      result   : "success",
      count    : payments.rows.length,
      marker   : payments.marker,
      payments : payments.rows
    };

    response.json(result).pipe(res);
  }
}

module.exports = function(db) {
  hbase = db;
  return AccountPayments;
};
