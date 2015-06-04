var Logger   = require('../../lib/logger');
var log      = new Logger({scope : 'Account Reports'});
var response = require('response');
var utils    = require('../../lib/utils');
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
      format     : (req.query.format || 'json').toLowerCase()
    };

    if (!options.accounts) {
      options.accounts = (/true/i).test(req.query.counterparties) ? true : false;
    }

    if (!options.account) {
      return {error: 'Account is required', code:400};
    }

    if(req.query.start) options.start = smoment(req.query.start)
    else options.start = smoment(0);

    if(req.query.end) options.end = smoment(req.query.end)
    else options.end = smoment();

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

  function successResponse(resp) {
    if (options.format === 'csv') {
      if (options.accounts) {
        resp.forEach(function(r) {
          r.sending_counterparties = r.sending_counterparties.join(', ');
          r.receiving_counterparties = r.receiving_counterparties.join(', ');
        });
      }

      res.csv(resp, options.account + ' - reports.csv');

    } else {
      response.json({
        result: 'success',
        count: resp.length,
        reports: resp
      }).pipe(res);
    }
  }
};

module.exports = function(db) {
  hbase = db;
  return AccountReports;
};
