var Logger   = require('../../lib/logger');
var log      = new Logger({scope : 'reports'});
var moment   = require('moment');
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
    };

    if (req.params.date) {
      options.start  = moment.utc(req.params.date).startOf('day');
      options.end    = moment.utc(options.start).add(1, 'day');

    } else {
      if (!options.end)   options.end   = moment.utc();
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

    //result = formatResponse(resp);
    response.json(result).pipe(res);
  }
}

module.exports = function(db) {
  hbase = db;
  return Reports;
};


function formatResponse (resp) {
  var result = [];
  var header;

  if (!resp) return result;
  header = Object.keys(resp[0] || {});
  if (header.length) {
    result.push(header);
  }

  resp.forEach(function(row) {
    var r = [];
    header.forEach(function(key) {
      r.push(row[key]);
    });

    result.push(r);
  });

  return result;
}
