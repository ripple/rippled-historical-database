'use strict';

var Logger = require('../../../lib/logger');
var log = new Logger({scope: 'topology nodes'});
var smoment = require('../../../lib/smoment');
var hbase = require('../../../lib/hbase')

var getNodes = function(req, res) {
  var options = {
    pubkey: req.params.pubkey,
    date: smoment(req.query.date),
    details: (/true/i).test(req.query.verbose) ? true : false,
    format: (req.query.format || 'json').toLowerCase()
  };

  if (req.query.date && !options.date) {
    errorResponse({
      error: 'invalid date format',
      code: 400
    });
    return;
  }

  log.info(options.pubkey || options.date.format());

  hbase.getTopologyNodes(options)
  .nodeify(function(err, resp) {
    if (err) {
      errorResponse(err);

    } else if (!resp) {
      errorResponse({
        error: 'node not found',
        code: 404
      });

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
        message: 'unable to retrieve topology node(s)'
      });
    }
  }

  /**
  * successResponse
  * return a successful response
  * @param {Object} markets
  * @param {Object} options
  */

  function successResponse(data) {

    if (options.pubkey) {
      data.result = 'success';
      res.json(data);

    } else {
      var filename;

      if (options.format === 'csv') {
        filename = 'topology nodes - ' + data.date + '.csv';
        res.csv(data.nodes, filename);

      } else {
        res.json({
          result: 'success',
          date: data.date,
          count: data.nodes.length,
          nodes: data.nodes
        });
      }
    }
  }
};

module.exports = getNodes
