'use strict';

var Logger = require('../../../lib/logger');
var log = new Logger({scope: 'get manifests'});
var smoment = require('../../../lib/smoment');
var hbase = require('../../../lib/hbase')

var getManifests = function(req, res) {
  var options = {
    pubkey: req.params.pubkey,
    marker: req.query.marker,
    limit: Number(req.query.limit || 200),
    descending: (/true/i).test(req.query.descending) ? true : false,
    format: (req.query.format || 'json').toLowerCase()
  };

  log.info(options.pubkey || '');

  hbase.getManifests(options)
  .then(function(data) {
    if (!data) {
      errorResponse({
        error: 'Manifest not found',
        code: 404
      });

    } else {
      successResponse(data);
    }
  })
  .catch(errorResponse);

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
        message: 'unable to retrieve manifest(s)'
      });
    }
  }

  /**
  * successResponse
  * return a successful response
  * @param {Object} markets
  * @param {Object} options
  */

  function successResponse(d) {

    if (options.format === 'csv') {
      res.csv(d, 'manifests.csv');

    } else {
      res.json({
        result: 'success',
        count: d.rows.length,
        marker: d.marker,
        manifests: d.rows
      });
    }
  }
};

module.exports = getManifests
