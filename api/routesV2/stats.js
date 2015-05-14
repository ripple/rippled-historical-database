'use strict';

var Logger = require('../../lib/logger');
var log = new Logger({scope: 'stats'});
var moment = require('moment');
var response = require('response');
var utils = require('../../lib/utils');
var hbase;

/**
 * Stats
 */

var Stats = function(req, res) {
  var options;

  /**
   * prepareOptions
   */

  function prepareOptions() {
    var options = {
      family: req.params.family || req.query.family,
      start: req.query.start,
      end: req.query.end,
      descending: (/false/i).test(req.query.descending) ? false : true,
      interval: req.query.interval || 'day',
      limit: req.query.limit || 200,
      marker: req.query.marker,
      format: (req.query.format || 'json').toLowerCase(),
    };

    if (!options.end) {
      options.end = moment.utc();
    }
    if (!options.start) {
      options.start = moment.utc('2013-01-01');
    }

    if (req.params.metric) {
      options.metrics = [options.family + ':' + req.params.metric];
    } else if (req.query.metrics) {
      options.metrics = req.query.metrics.split(',');
      if (options.family) {
        options.metrics.forEach(function(metric, i) {
          options.metrics[i] = options.family + ':' + metric;
        });
      } else {
        return {error: 'family is required', code: 400};
      }
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
      response.json({result: 'error', message: 'unable to retrieve stats'})
        .status(500).pipe(res);
    }
  }

  /**
  * successResponse
  * return a successful response
  * @param {Object} payments
  */

  function successResponse(resp) {
    var filename = 'stats';

    if (options.format === 'csv') {
      if (options.family) {
        filename += ' ' + options.family;
      }

      if (options.metrics) {
        filename += '-' + options.metrics.join('-');
      }

      if (!options.family && !options.metric) {
        resp.rows.forEach(function(r, i) {
          resp.rows[i] = utils.flattenJSON(r);
        });
      }

      res.csv(resp.rows, filename + '.csv');

    } else {
      response.json({
        result: 'success',
        count: resp.rows.length,
        marker: resp.marker,
        stats: resp.rows
      }).pipe(res);
    }
  }

  options = prepareOptions();

  if (options.error) {
    errorResponse(options);
    return;

  } else {
    log.info(options.start.toString(), '-', options.end.toString());

    hbase.getStats(options, function(err, resp) {
      if (err) {
        errorResponse(err);
      } else {
        successResponse(resp);
      }
    });
  }
};

module.exports = function(db) {
  hbase = db;
  return Stats;
};
