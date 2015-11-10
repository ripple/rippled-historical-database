var Logger = require('../../lib/logger');
var log = new Logger({scope : 'Account Reports'});
var utils = require('../../lib/utils');
var smoment = require('../../lib/smoment');
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
    log.info(options.account, options.start.format(), '-', options.end.format());

    hbase.getAggregateAccountPayments(options)
    .nodeify(function(err, resp) {
      if (err) {
        errorResponse(err);
      } else {
        if (options.descending) {
          resp.rows.reverse();
        }

        resp.rows.forEach(function(row) {

          // return the count only
          if (!options.accounts) {
            row.receiving_counterparties = row.receiving_counterparties.length;
            row.sending_counterparties   = row.sending_counterparties.length;
          }

          // convert amount to string
          if (options.payments) {
            row.payments.forEach(function(p) {
              p.amount = p.amount.toString();
            });

          // delete the payments array
          } else {
            delete row.payments;
          }

          row.high_value_received = row.high_value_received.toString();
          row.high_value_sent = row.high_value_sent.toString();
          row.total_value_received = row.total_value_received.toString();
          row.total_value_sent = row.total_value_sent.toString();
          row.total_value = row.total_value.toString();
        });

        successResponse(resp);
      }
    });
  }

  /**
   * prepareOptions
   */

  function prepareOptions() {
    var days;
    var options = {
      account: req.params.address,
      start: smoment(req.query.start),
      end: smoment(req.query.end),
      descending: (/true/i).test(req.query.descending) ? true : false,
      accounts: (/true/i).test(req.query.accounts) ? true : false,
      payments: (/true/i).test(req.query.payments) ? true : false,
      format: (req.query.format || 'json').toLowerCase()
    };

    if (!options.accounts) {
      options.accounts = (/true/i).test(req.query.counterparties) ? true : false;
    }

    if (!options.account) {
      return {error: 'Account is required', code:400};
    }

    if (req.params.date) {
      options.start = smoment(req.params.date);
      options.end = smoment(req.params.date);
    }

    if (!options.start) {
      return {error: 'invalid date format', code: 400};
    } else if (!options.end) {
      return {error: 'invalid end date format', code: 400};
    }

    days = options.end.moment.diff(options.start.moment, 'days');
    if (!days) {
      options.start.moment.startOf('day');
    } else if(Math.abs(days) > 200) {
      return {error: 'choose a date range less than 200 days', code: 400};
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
      res.status(err.code).json({
        result: 'error',
        message: err.error
      });
    } else {
      res.status(500).json({
        result: 'error',
        message: 'unable to retrieve payments'
      });
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
        resp.rows.forEach(function(r) {
          r.sending_counterparties = r.sending_counterparties.join(', ');
          r.receiving_counterparties = r.receiving_counterparties.join(', ');
        });
      }

      res.csv(resp, options.account + ' - reports.csv');

    } else {
      res.json({
        result: 'success',
        start: options.start.format(),
        end: options.end.format(),
        count: resp.rows.length,
        reports: resp.rows
      });
    }
  }
};

module.exports = function(db) {
  hbase = db;
  return AccountReports;
};
