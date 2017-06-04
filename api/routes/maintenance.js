'use strict';

var Logger = require('../../lib/logger');
var log = new Logger({scope: 'maintenance'});
var hbase = require('../../lib/hbase')
var domains = ['ripplecharts', 'monitoring_tools'];


/**
 * Stats
 */

var getMaintenance = function(req, res) {
  var table = 'control';
  var rowkey = 'maintenance_' + req.params.domain;

  if (domains.indexOf(req.params.domain) === -1) {
    errorResponse({
      error: 'Invalid domain',
      code: 400
    });
    return;
  }

  hbase.getRow({
    table: table,
    rowkey: rowkey
  }, function(err, resp) {
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
        message: 'unable to get data'
      });
    }
  }

  /**
  * successResponse
  * return a successful response
  * @param {Object} payments
  */

  function successResponse(resp) {
    var result = {
      result: 'success',
      mode: resp && resp.mode ? resp.mode : 'normal'
    };

    if (result.mode === 'maintenance' ||
        result.mode === 'banner') {
      result.html = resp.html;
      result.style = resp.style ? JSON.parse(resp.style) : undefined;
    }

    res.json(result);
  }
};

module.exports = getMaintenance
