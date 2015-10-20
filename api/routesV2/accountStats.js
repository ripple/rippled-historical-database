var Logger = require('../../lib/logger');
var log = new Logger({scope : 'Account Reports'});
var utils = require('../../lib/utils');
var smoment = require('../../lib/smoment');
var hbase;

/**
 * Account Stats
 */

var AccountStats = function (req, res, next) {
  var days;
  var options = {
    account: req.params.address,
    start: smoment(req.query.start),
    end: smoment(req.query.end),
    descending: (/true/i).test(req.query.descending) ? true : false,
    format: (req.query.format || 'json').toLowerCase()
  };

  if (req.params.date) {
    options.start = smoment(req.params.date);
    options.end = smoment(req.params.date);
  }

  if (!options.start) {
    errorResponse({error: 'invalid date format', code: 400});
    return;

  } else if (!options.end) {
    errorResponse({error: 'invalid end date format', code: 400});
    return;
  }

  days = options.end.moment.diff(options.start.moment, 'days');

  if (!days) {
    options.start.moment.startOf('day');

  } else if (Math.abs(days) > 200) {
    errorResponse({error: 'choose a date range less than 200 days', code: 400});
    return;
  }

  log.info(options.account, options.start.format(), '-', options.end.format());

  hbase.getAccountStats(options, function(err, resp) {
    if (err) {
      errorResponse(err);
    } else {
      if (options.descending) resp.reverse();

      successResponse(resp);
    }
  });

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
  return AccountStats;
};
