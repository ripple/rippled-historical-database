'use strict';

var Logger = require('../../../lib/logger');
var log = new Logger({scope: 'topology nodes'});
var smoment = require('../../../lib/smoment');
var hbase = require('../../../lib/hbase')

var getLinks = function(req, res) {
  var options = {
    date: smoment(req.query.date),
    format: (req.query.format || 'json').toLowerCase()
  };

  if (req.query.date && !options.date) {
    errorResponse({
      error: 'invalid date format',
      code: 400
    });
    return;
  }

  log.info(options.date.format())

  hbase.getTopologyLinks(options)
  .nodeify(function(err, resp) {
    if (err) {
      errorResponse(err);
    } else {
      successResponse(resp, options);
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
        message: 'unable to retrieve topology links'
      });
    }
  }

  /**
  * successResponse
  * return a successful response
  * @param {Object} markets
  * @param {Object} options
  */

  function successResponse(data, options) {
    var filename;

    if (options.format === 'csv') {
      filename = 'topology links - ' + data.date + '.csv';
      res.csv(data.links, filename);

    } else {
      res.json({
        result: 'success',
        date: data.date,
        count: data.links.length,
        nodes: data.links
      });
    }
  }
};

module.exports = getLinks
