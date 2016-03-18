var Promise = require('bluebird');
var smoment = require('../../smoment');

var methods = {};

/**
 * getTopologyNodes
 */

var getTopologyNodes = function(options) {
  var self = this;
  return self.getTopologyInfo(options.date)
  .then(function(info) {
    return new Promise(function(resolve, reject) {
      if (!info) {
        reject('crawl data not found');
        return;
      }

      self.getScanWithMarker(self, {
        prefix: self._topologyPrefix,
        table: 'rawl_node_stats',
        startRow: info.rowkey,
        stopRow: info.rowkey + 'z',
        limit: options.limit
      }, function(err, resp) {
        if (err) {
          reject(err);
        } else {
          var parts = info.rowkey.split('_');
          var timestamp = Math.floor(Number(parts[0])/1000);
          var result = {
            date: smoment(timestamp).format(),
            marker: resp.marker,
            nodes: formatNodeStats(resp.rows)
          };

          if (options.links) {
            self.getTopologyLinks({
              crawl_key: info.rowkey
            })
            .then(function(data) {
              result.links = data.links;
              resolve(result);
            });
          } else {
            resolve(result);
          }
        }
      });
    });
  });
};

/**
 * formatNodeStats
 */

var formatNodeStats = function(rows) {
  var results = [];

  rows.forEach(function(r) {
    var ipp = r.ipp ? r.ipp.split(':') : null;
    var row = {
      node_public_key: r.pubkey
    };

    if (ipp) {
      row.ip = ipp[0];
      row.port = ipp[1] ? Number(ipp[1]) : undefined;
    }

    row.version = r.version,
    row.uptime = Number(r.uptime || 0);

    if (r.in_count && r.in_count !== '0')
      row.inbound_count = Number(r.in_count);
    if (r.in_add_count  && r.in_add_count !== '0')
      row.inbound_added = Number(r.in_add_count);
    if (r.in_drop_count  && r.in_drop_count !== '0')
      row.inbound_dropped = Number(r.in_drop_count);
    if (r.out_count  && r.out_count !== '0')
      row.outbound_count = Number(r.out_count);
    if (r.out_add_count  && r.out_add_count !== '0')
      row.outbound_added = Number(r.out_add_count);
    if (r.out_drop_count  && r.out_drop_count !== '0')
      row.outbound_dropped = Number(r.out_drop_count);

    results.push(row);
  });

  return results;
};

/**
 * getTopologyLinks
 */

var getTopologyLinks = function(options) {
  var self = this;

  if (options.crawl_key) {
    return getLinks(options.crawl_key);

  } else {
    return self.getTopologyInfo(options.date)
    .then(function(info) {

      if (!info) {
        throw new Error('crawl data not found');
      }

      return getLinks(info.rowkey);
    });
  }

  function getLinks(key) {
    return new Promise(function(resolve, reject) {
      self.getScanWithMarker(self, {
        prefix: self._topologyPrefix,
        table: 'onnections',
        startRow: key,
        stopRow: key + 'z',
        //filterString: 'KeyOnlyFilter()',
        limit: options.limit || Infinity
      }, function(err, resp) {
        if (err) {
          reject(err);
        } else {

          var parts = key.split('_');
          var timestamp = Math.floor(Number(parts[0])/1000);

          resolve({
            date: smoment(timestamp).format(),
            marker: resp.marker,
            links: formatLinks(resp.rows)
          });
        }
      });
    });
  }
};

/**
 * formatLinks
 */

var formatLinks = function(rows) {
  var results = [];

  rows.forEach(function(r) {
    var parts = r.rowkey.split('+');
    results.push({
      source: parts[1],
      target: parts[2]
    });
  });

  return results;
};


/**
 * getTopologyInfo
 */

var getTopologyInfo = function(date) {

  var self = this;
  var stop = date ? date.moment.unix() * 1000 : 'a';

  return new Promise(function(resolve, reject) {
    self.getScan({
      prefix: self._topologyPrefix,
      table: 'rawls',
      startRow: 0,
      stopRow: stop,
      descending: true,
      limit: 1
    }, function(err, resp) {
      if (err) {
        reject(err);
      } else {
        resolve(resp && resp[0] ? resp[0] : null);
      }
    });
  });
};

module.exports = {
  getTopologyNodes: getTopologyNodes,
  getTopologyInfo: getTopologyInfo,
  getTopologyLinks: getTopologyLinks
};
