'use strict';

var Logger = require('../../../lib/logger');
var log = new Logger({scope: 'topology nodes'});
var smoment = require('../../../lib/smoment');
var hbase = require('../../../lib/hbase')

var getNodes = function(req, res) {
  var options = {
    details: (/true/i).test(req.query.verbose) ? true : false,
    date: smoment(req.query.date),
    links: true,
  };

  if (req.query.date && !options.date) {
    errorResponse({
      error: 'invalid date format',
      code: 400
    });
    return;
  }

  log.info(options.date.format())

  hbase.getTopologyNodes(options)
  .nodeify(function(err, resp) {
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
        message: 'unable to retrieve topology nodes'
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
    res.json({
      result: 'success',
      date: data.date,
      node_count: data.nodes.length,
      link_count: data.links.length,
      nodes: data.nodes,
      links: data.links
    });
  }
};

module.exports = getNodes
