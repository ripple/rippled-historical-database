var Logger = require('../../lib/logger');
var log = new Logger({scope : 'Account Stats'});
var utils = require('../../lib/utils');
var smoment = require('../../lib/smoment');
var hbase = require('../../lib/hbase')
var families = ['transactions', 'value'];



/**
 * Account Stats
 */

var AccountStats = function (req, res, next) {
  var days;
  var options = {
    account: req.params.address,
    family: req.params.family,
    start: smoment(req.query.start || '2013-01-01'),
    end: smoment(req.query.end),
    limit: req.query.limit || 200,
    marker: req.query.marker,
    descending: (/true/i).test(req.query.descending) ? true : false,
    format: (req.query.format || 'json').toLowerCase()
  };

  if (req.params.date) {
    options.start = smoment(req.params.date);
    options.end = smoment(req.params.date);
  }

  if (!options.start) {
    errorResponse({error: 'invalid start date format', code: 400});
    return;

  } else if (!options.end) {
    errorResponse({error: 'invalid end date format', code: 400});
    return;
  }

  if (isNaN(options.limit)) {
    options.limit = 200;

  } else if (options.limit > 1000) {
    options.limit = 1000;
  }

  days = options.end.moment.diff(options.start.moment, 'days');

  if (!days) {
    options.start.moment.startOf('day');
  }

  if (families.indexOf(options.family) === -1) {
    errorResponse({error: 'invalid family', code: 400});
    return;
  }

  log.info(options.family, options.account, options.start.format(), '-', options.end.format());

  hbase.getAccountStats(options, function(err, resp) {
    if (err) {
      errorResponse(err);
    } else {
      successResponse(resp);
    }
  });

  /**
  * errorResponse
  * return an error response
  * @param {Object} err
  */

  function errorResponse (err) {
    log.error(err.error || err);
    if (err.code && err.code.toString()[0] === '4') {
      res.status(err.code).json({
        result:'error',
        message:err.error
      });
    } else {
      res.status(500).json({
        result:'error',
        message:'unable to retrieve exchanges'
      });
    }
  }

  /**
  * successResponse
  * return a successful response
  * @param {Object} payments
  */

  function successResponse(resp) {

    if (resp.marker) {
      utils.addLinkHeader(req, res, resp.marker);
    }

    if (options.format === 'csv') {
      var filename = options.account + ' - stats.' + options.family + '.csv';
      var results = [];
      resp.rows.forEach(function(r) {
        results.push(utils.flattenJSON(r));
      });
      res.csv(results, filename);

    } else {
      res.json({
        result: 'success',
        count: resp.rows.length,
        marker: resp.marker,
        rows: resp.rows
      });
    }
  }
};

module.exports = AccountStats
