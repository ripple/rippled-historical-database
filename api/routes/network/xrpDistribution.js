'use strict';

var Logger = require('../../../lib/logger');
var log = new Logger({scope: 'metrics'});
var smoment = require('../../../lib/smoment');
var utils = require('../../../lib/utils');
var hbase = require('../../../lib/hbase')

function getXrpDistribution(req, res) {

  var options = {
    start: smoment(req.query.start || '2013-01-01'),
    end: smoment(req.query.end),
    descending: (/true/i).test(req.query.descending) ? true : false,
    marker: req.query.marker,
    limit: Number(req.query.limit || 200),
    format: (req.query.format || 'json').toLowerCase()
  };

  if (isNaN(options.limit)) {
    options.limit = 200;

  } else if (options.limit > 1000) {
    options.limit = 1000;
  }


  if (!options.start) {
    errorResponse({
      error: 'invalid start date format',
      code: 400
    });
    return;

  } else if (!options.end) {
    errorResponse({
      error: 'invalid end date format',
      code: 400
    });
    return;
  }


  hbase.getScanWithMarker(hbase, {
    table: 'xrp_distribution',
    startRow: options.start.hbaseFormatStartRow(),
    stopRow: options.end.hbaseFormatStopRow(),
    marker: options.marker,
    limit: options.limit,
    descending: options.descending
  },
  function(err, resp) {

    if (err) {
      errorResponse(err);
    } else {

      resp.rows.forEach(function(r) {
        r.date = smoment(r.date).format();
        delete r.rowkey;
        delete r.currency;
      });

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
      res.status(err.code).json({
        result: 'error',
        message: err.error
      });
    } else {
      res.status(500).json({
        result: 'error',
        message: 'error getting data'
      });
    }
  }

 /**
  * successResponse
  * return a successful response
  * @param {Object} resp
  */

  function successResponse(resp) {

    if (resp.marker) {
      utils.addLinkHeader(req, res, resp.marker);
    }

    // csv
    if (options.format === 'csv') {
      resp.rows.forEach(function(r, i) {
        resp.rows[i] = utils.flattenJSON(r);
      });
      res.csv(resp.rows, 'XRP-distribution.csv');

    // json
    } else {
      res.json({
        result: 'success',
        count: resp.rows.length,
        marker: resp.marker,
        rows: resp.rows
      });
    }
  }
}

module.exports = getXrpDistribution
