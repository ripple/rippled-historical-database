'use strict';

var Logger = require('../../../lib/logger');
var log = new Logger({scope: 'get validators'});
var smoment = require('../../../lib/smoment');
var hbase = require('../../../lib/hbase')

var getValidators = function(req, res) {
  var options = {
    pubkey: req.params.pubkey,
    format: (req.query.format || 'json').toLowerCase()
  };

  log.info(options.pubkey || '');

  hbase.getValidators(options)
  .then(function(data) {
    if (!data) {
      errorResponse({
        error: 'Validator not found',
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
        message: 'unable to retrieve validator(s)'
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

    if (options.pubkey) {
      d.result = 'success';
      res.json(d);

    } else if (options.format === 'csv') {
      res.csv(d, 'validators.csv');

    } else {
      res.json({
        result: 'success',
        count: d.length,
        validators: d
      });
    }
  }
};

module.exports = getValidators
