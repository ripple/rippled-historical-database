'use strict';

var Logger = require('../../lib/logger');
var log = new Logger({scope: 'reports'});
var smoment = require('../../lib/smoment');
var response = require('response');
var hbase;

/**
 * Reports
 */

var Reports = function (req, res, next) {
  var options = prepareOptions();

  if (options.error) {
    errorResponse(options);
    return;

  } else {
    log.info(options.start.toString(), '-', options.end.toString());

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
      start      : req.query.start,
      end        : req.query.end,
      descending : (/true/i).test(req.query.descending) ? true : false,
      accounts   : (/true/i).test(req.query.accounts) ? true : false,
      format     : (req.query.format || 'json').toLowerCase()
    };

    if (!options.accounts) {
      options.accounts = (/true/i).test(req.query.counterparties) ? true : false;
    }

    // if (req.params.date) {
    //   options.start  = moment.utc(req.params.date).startOf('day');
    //   options.end    = moment.utc(options.start).add(1, 'day');

    // } else {
    //   if (!options.end)   options.end   = moment.utc();
    //   if (!options.start) options.start = moment.utc().startOf('day');
    // }

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
    var filename = 'account reports';
    var start;
    var end;

    if (options.format === 'csv') {
      if (options.accounts) {
        resp.forEach(function(r) {
          r.sending_counterparties = r.sending_counterparties.join(', ');
          r.receiving_counterparties = r.receiving_counterparties.join(', ');
        });
      }

      start = smoment(options.start);
      end = smoment(options.end);

      filename += ' ' + start.format('YYYY-MM-DD');
      // if (options.end && end.diff(start.add(1, 'day')) > 0) {
      //   filename += ' - ' + end.format('YYYY-MM-DD');
      // }
      res.csv(resp, filename + '.csv');

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
  return Reports;
};
