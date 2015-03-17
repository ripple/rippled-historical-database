var config   = require('../../storm/multilang/resources/config');
var Logger   = require('../../storm/multilang/resources/src/lib/modules/logger');
var log      = new Logger({scope : 'Account Reports'});
var moment   = require('moment');
var response = require('response');
var hbase;

/**
 * Account Reports
 */

var AccountReports = function (req, res, next) {
  var options = prepareOptions();

  if (options.error) {
    errorResponse(options);
    return;

  } else {
    log.info(options.account, options.start.toString(), '-', options.end.toString());

    hbase.getAggregateAccountPayments(options)
    .nodeify(function(err, resp) {
      if (err) {
        errorResponse(err);
      } else {
        if (options.descending) resp.reverse();
        if (!options.accounts) {
          resp.forEach(function(row) {
            row.receiving_counterparties = row.receiving_counterparties.length;
            row.sending_counterparties   = row.sending_counterparties.length;
          });
        }

        successResponse(resp);
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
      descending : (/true/i).test(req.query.descending) ? true : false,
      accounts   : (/true/i).test(req.query.accounts) ? true : false,
    };

    if (!options.account) {
      return {error: 'Account is required', code:400};
    }

    if (req.params.date) {
      options.start = moment.utc(req.params.date).startOf('day');
      options.end   = moment.utc(req.params.date).startOf('day');

    } else {
      if (!options.end)   options.end   = moment.utc().startOf('day');
      if (!options.start) options.start = moment.utc().startOf('day');
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

  function successResponse (resp) {
    var result = {
      result   : "sucess",
      count    : resp.length,
      rows     : resp
    };

    response.json(result).pipe(res);
  }
}

module.exports = function(db) {
  hbase = db;
  return AccountReports;
};
