'use strict';

var Logger = require('../../../lib/logger');
var log = new Logger({scope: 'metrics'});
var smoment = require('../../../lib/smoment');
var utils = require('../../../lib/utils');
var hbase;

function getRippledVersions(req, res) {

  var options = {
    format: (req.query.format || 'json').toLowerCase()
  };



  hbase.getScan({
    table: 'rippled_versions',
    startRow: ' ',
    stopRow: '~',
    limit: 1,
    descending: true
  },
  function(err, resp) {
   if (err) {
     errorResponse(err);

   } else if (!resp.length) {
     successResponse([]);

   } else {
     var date = smoment(resp[0].date);
    hbase.getScan({
      table: 'rippled_versions',
      startRow: date.hbaseFormatStartRow(),
      stopRow: date.hbaseFormatStopRow(),
    }, function(err, resp) {
      if (err) {
        errorResponse(err);

      } else {
        resp.forEach(function(row) {
          delete row.rowkey;
        });

        successResponse(resp);
      }
    });
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

    // csv
    if (options.format === 'csv') {
      res.csv(resp, 'rippled-versions.csv');

    // json
    } else {
      res.json({
        result: 'success',
        count: resp.length,
        rows: resp
      });
    }
  }
}

module.exports = function(db) {
  hbase = db;
  return getRippledVersions;
};
