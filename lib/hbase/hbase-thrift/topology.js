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

/**
 * getValidatorReports
 */

var getValidatorReports = function(options) {
  var self = this;
  var keys = [];

  if (options.pubkey) {
    start = smoment(options.start);
    end = smoment(options.end);
    start.moment.startOf('day');

    while(end.moment.diff(start.moment)>=0) {
      keys.push(start.hbaseFormatStartRow() + '|' + options.pubkey);
      start.moment.add(1, 'day');
    }

    return new Promise (function(resolve, reject) {
      self.getRows({
        table: 'validator_reports',
        rowkeys: keys
      }, function(err, rows) {
        if (err) {
          reject(err);

        } else {
          resolve({
            rows: formatReports(rows)
          });
        }
      });
    });

  } else if (!options.start) {
    return scanHelper(smoment('2013-01-01'), smoment(), true)
    .then(function(resp) {
      if (!resp || !resp.rows || !resp.rows.length) {
        return {
          rows: []
        }
      }

      var start = smoment(resp.rows[0].date);
      start.moment.startOf('day');
      var end = smoment(options.start);
      return scanHelper(start, end);
    });

  } else {
    return scanHelper(options.start, options.end);
  }

  function scanHelper(start, end, latest) {
    return new Promise(function(resolve, reject) {
      self.getScanWithMarker(self, {
        table: 'validator_reports',
        startRow: start.hbaseFormatStartRow(),
        stopRow: end.hbaseFormatStopRow(),
        limit: latest ? 1 : Infinity,
        descending: latest ? true : false
      }, function(err, resp) {
        if (err) {
          reject(err);
        } else {
          resolve({
            marker: resp.marker,
            rows: formatReports(resp.rows)
          });
        }
      });
    });
  }

  function formatReports(rows, sort) {
    var results = [];

    rows.forEach(function(r) {
      results.push({
        validation_public_key: r.validation_public_key,
        date: r.date,
        total_ledgers: Number(r.total_ledgers),
        main_net_agreeement: r.main_net_agreement,
        main_net_ledgers: Number(r.main_net_ledgers),
        alt_net_agreeement: r.alt_net_agreement,
        alt_net_ledgers: Number(r.alt_net_ledgers),
        other_ledgers: Number(r.other_ledgers),
      })
    });

    if (sort) {
      results.sort(function(a, b) {
        return (b.main_net_agreeement - a.main_net_agreeement) ||
          (b.main_net_ledgers - a.main_net_ledgers) ||
          (a.alt_net_agreeement - b.alt_net_agreeement) ||
          (a.alt_net_ledgers - b.alt_net_ledgers)
      });
    }

    return results;
  }
};

/**
 * getValidators
 */

var getValidators = function(options) {
  var self = this;

  if (options.pubkey) {
    return new Promise(function(resolve, reject) {
      self.getRow({
        table: 'validators',
        rowkey: options.pubkey
      }, function(err, row) {
        if (err) {
          reject(err);
        } else {
          resolve(row ? formatRow(row) : undefined);
        }
      });
    });

  } else {
    return self.getAllRows({
      table: 'validators'
    }).then(function(rows) {
      rows.sort(function(a,b) {
        return smoment(b.last_datetime).unix(true) -
          smoment(a.last_datetime).unix(true);
      });

      rows.forEach(formatRow);
      return rows;
    });
  }

  function formatRow(r) {
    delete r.rowkey;
    return r;
  }
};

/**
 * getLedgerValidations
 */

var getLedgerValidations = function(options) {
  var self = this;
  return new Promise(function(resolve, reject) {
    self.getScanWithMarker(self, {
      table: 'validations_by_ledger',
      startRow: options.ledger_hash,
      stopRow: options.ledger_hash + '~',
      marker: options.marker,
      limit: options.limit,
      descending: false
    }, function (err, resp) {
      if (err) {
        reject(err);

      } else {
        resp.rows.forEach(function(r) {
          r.count = Number(r.count);
          delete r.rowkey;
        });

        resolve(resp);
      }
    });
  });
};

module.exports = {
  getTopologyNodes: getTopologyNodes,
  getTopologyInfo: getTopologyInfo,
  getTopologyLinks: getTopologyLinks,
  getValidatorReports: getValidatorReports,
  getLedgerValidations: getLedgerValidations,
  getValidators: getValidators
};
