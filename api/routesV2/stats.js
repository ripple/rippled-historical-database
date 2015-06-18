'use strict';

var Logger = require('../../lib/logger');
var log = new Logger({scope: 'stats'});
var smoment = require('../../lib/smoment');
var response = require('response');
var utils = require('../../lib/utils');
var families = ['type','result','metric'];
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
      family: (req.params.family || req.query.family || '').toLowerCase(),
      start: smoment(req.query.start || '2013-01-01'),
      end: smoment(req.query.end),
      descending: (/true/i).test(req.query.descending) ? true : false,
      interval: req.query.interval || 'day',
      limit: req.query.limit || 200,
      marker: req.query.marker,
      format: (req.query.format || 'json').toLowerCase(),
    };

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

    if (!options.start) {
      return {error: 'invalid start date format', code: 400};
    } else if (!options.end) {
      return {error: 'invalid end date format', code: 400};
    }

    if (options.family && families.indexOf(options.family) === -1) {
      return {error: 'invalid family, use: ' + families.join(', '), code: 400};
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
    log.info(options.start.format(), '-', options.end.format());

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
