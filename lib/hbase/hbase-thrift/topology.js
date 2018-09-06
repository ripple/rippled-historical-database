'use strict'

var Promise = require('bluebird')
var smoment = require('../../smoment')

const formatKey = 'YYYYMMDDHHmmss';
const timeInfinity = 99999999999999;

const getInverseTimestamp = date => (timeInfinity - Number(smoment(date).format(formatKey))).toString();


/**
 * formatNodeStats
 */

function formatNodeStats(r) {
  var ipp = r.ipp ? r.ipp.split(':') : null
  var row = {
    node_public_key: r.pubkey
  }

  if (ipp) {
    row.ip = ipp[0]
    row.port = ipp[1] ? Number(ipp[1]) : undefined
  }

  row.version = r.version
  row.uptime = Number(r.uptime || 0)

  if (r.in_count && r.in_count !== '0') {
    row.inbound_count = Number(r.in_count)
  }

  if (r.in_add_count && r.in_add_count !== '0') {
    row.inbound_added = Number(r.in_add_count)
  }

  if (r.in_drop_count && r.in_drop_count !== '0') {
    row.inbound_dropped = Number(r.in_drop_count)
  }

  if (r.out_count && r.out_count !== '0') {
    row.outbound_count = Number(r.out_count)
  }

  if (r.out_add_count && r.out_add_count !== '0') {
    row.outbound_added = Number(r.out_add_count)
  }

  if (r.out_drop_count && r.out_drop_count !== '0') {
    row.outbound_dropped = Number(r.out_drop_count)
  }

  return row
}

/**
 * formatLinks
 */

function formatLinks(rows) {
  var results = []

  rows.forEach(function(r) {
    var parts = r.rowkey.split('+')
    results.push({
      source: parts[1],
      target: parts[2]
    })
  })

  return results
}

/**
 * getTopologyNodes
 */

function getTopologyNodes(options = {}) {
  var self = this

  const formatLinks = (nodes, links) => {
    const nodeMap = {};

    nodes.forEach(d => {
      nodeMap[d.pubkey_node.substr(0,12)] = d.pubkey_node;
    });

    return links.map(d => {
      const parts = d.split('>');
      return {
        source: nodeMap[parts[0]],
        target: nodeMap[parts[1]]
      };
    });
  };

  const getDetails = nodes => {
    const tasks = [];
    nodes.forEach(node => {
      tasks.push(getNodeDetails(node.node_public_key, false)
        .then(details => {
          return Object.assign(node, details);
        }));
    });

    return Promise.all(tasks);
  }

  const getTopology = (date, onlyNodes = false) => {
    return new Promise(function(resolve, reject) {
      self.getScan({
        table: 'network_crawls',
        startRow: getInverseTimestamp(options.date),
        stopRow: '~',
        columns: onlyNodes ? ['d:nodes','d:start'] : undefined,
        limit: 1
      }, function(err, resp) {
        if (err || !resp) {
          reject(err || 'unable to get topology');
        } else if (!resp[0]) {
          reject({
            code: 404,
            message: 'topology not found'
          });
        } else {
          resolve(Object.assign({}, resp[0], {
            nodes: JSON.parse(resp[0].nodes),
            connections: resp[0].connections ? JSON.parse(resp[0].connections) : undefined
          }));
        }
      });
    });
  }

  function getNodeDetails(pubkey, all = true) {
    const columns = all ? undefined : [
      'f:lat',
      'f:long',
      'f:continent',
      'f:country',
      'f:region',
      'f:city',
      'f:postal_code',
      'f:country_code',
      'f:region_code',
      'f:timezone',
      'f:isp',
      'f:org',
      'f:domain'
    ]

    return new Promise(function(resolve, reject) {
      self.getRow({
        table: 'node_state',
        rowkey: pubkey,
        columns
      }, (err, node) => err ? reject(err) : resolve(node))
    })
  }

  if (options.pubkey) {
    return getNodeDetails(options.pubkey)
    .then(node => {
      return Object.assign(node, {
        node_public_key: node.pubkey_node,
        inbound_count: Number(node.in),
        outbound_count: Number(node.out),
        uptime: Number(node.uptime),
        pubkey_node: undefined,
        in: undefined,
        out: undefined
      });
    });
  }

  return getTopology(options.date, !options.links)
  .then(resp => {
    const nodeList = resp.nodes.slice(0, options.limit)
    .map(d => {
      return {
        node_public_key: d.pubkey_node,
        ip: d.host,
        port: d.port ? Number(d.port) : undefined,
        version: `rippled-${d.version}`,
        uptime: Number(d.uptime),
        inbound_count: d.in,
        outbound_count: d.out
      };
    });

    const result = {
      date: resp.start,
      node_count: nodeList.length,
      link_count: resp.connections_count,
      nodes: nodeList,
      links: options.links && formatLinks(resp.nodes, resp.connections)
    }

    if (options.details) {
      return getDetails(nodeList)
      .then(detailedNodes => {
        result.nodes = detailedNodes;
        return result;
      })
    }

    return result;
  })
}

/**
 * getTopologyLinks
 */

function getTopologyLinks(options = {}) {
  var self = this

  return self.getTopologyNodes({
    date: options.date,
    links: true
  });
}

/**
 * getValidatorReports
 */

function getValidatorReports(options) {
  var self = this
  var keys = []
  var start
  var end

  function formatReports(rows, sort) {
    var results = []

    rows.forEach(function(r) {
      results.push({
        validation_public_key: r.validation_public_key,
        date: r.date,
        total_ledgers: Number(r.total_ledgers),
        main_net_agreement: r.main_net_agreement,
        main_net_ledgers: Number(r.main_net_ledgers),
        alt_net_agreement: r.alt_net_agreement,
        alt_net_ledgers: Number(r.alt_net_ledgers),
        other_ledgers: Number(r.other_ledgers)
      })
    })

    if (sort) {
      results.sort(function(a, b) {
        return (b.main_net_agreement - a.main_net_agreement) ||
          (b.main_net_ledgers - a.main_net_ledgers) ||
          (a.alt_net_agreement - b.alt_net_agreement) ||
          (a.alt_net_ledgers - b.alt_net_ledgers)
      })
    }

    return results
  }


  function getDetails(data) {
    data.rows = formatReports(data.rows, true)
    if (!options.details) {
      return data
    }

    return Promise.map(data.rows, function(row) {
      return new Promise(function(resolve) {
        self.getRow({
          table: 'validators',
          rowkey: row.validation_public_key
        }, function(err, resp) {
          if (err) {
            self.log.error(err)
            self.log.error('failed to get report: ' + row.validation_public_key)
            resolve()

          } else if (resp) {
            row.domain = resp.domain
            row.domain_state = resp.domain_state
            row.last_datetime = resp.last_datetime
            resolve(row)
          } else {
            resolve(row)
          }
        })
      })
    }).then(function(rows) {
      data.rows = rows.filter(function(d) {
        return Boolean(d)
      })

      return data
    })
  }

  function scanHelper(startRow, endRow, latest) {
    return new Promise(function(resolve, reject) {
      self.getScanWithMarker(self, {
        table: 'validator_reports',
        startRow: startRow.hbaseFormatStartRow(),
        stopRow: endRow.hbaseFormatStopRow(),
        limit: latest ? 1 : Infinity,
        descending: latest ? true : false
      }, function(err, resp) {
        if (err) {
          reject(err)
        } else {
          resolve({
            marker: resp.marker,
            rows: formatReports(resp.rows)
          })
        }
      })
    })
  }

  if (options.pubkey) {
    start = smoment(options.start)
    end = smoment(options.end)
    start.moment.startOf('day')

    while (end.moment.diff(start.moment) >= 0) {
      keys.push(start.hbaseFormatStartRow() + '|' + options.pubkey)
      start.moment.add(1, 'day')
    }

    if (options.descending) {
      keys.reverse()
    }

    return new Promise(function(resolve, reject) {
      self.getRows({
        table: 'validator_reports',
        rowkeys: keys
      }, function(err, rows) {

        if (err) {
          reject(err)

        } else {
          resolve({
            rows: formatReports(rows)
          })
        }
      })
    })

  } else if (!options.start) {
    return scanHelper(smoment('2013-01-01'), smoment(), true)
    .then(function(resp) {
      if (!resp || !resp.rows || !resp.rows.length) {
        return {
          rows: []
        }
      }

      start = smoment(resp.rows[0].date)
      start.moment.startOf('day')
      end = smoment(options.start)
      return scanHelper(start, end)
      .then(getDetails)
    })

  } else {
    return scanHelper(options.start, options.end)
    .then(getDetails)
  }
}

/**
 * getValidators
 */

function getValidators(options) {
  var self = this

  function formatRow(r) {
    delete r.rowkey
    return r
  }

  if (options && options.pubkey) {
    return new Promise(function(resolve, reject) {
      self.getRow({
        table: 'validators',
        rowkey: options.pubkey
      }, function(err, row) {
        if (err) {
          reject(err)
        } else {
          resolve(row ? formatRow(row) : undefined)
        }
      })
    })

  } else {
    return self.getAllRows({
      table: 'validators'
    }).then(function(rows) {
      rows.sort(function(a, b) {
        return smoment(b.last_datetime).unix(true) -
          smoment(a.last_datetime).unix(true)
      })

      rows.forEach(formatRow)
      return rows
    })
  }
}

/**
 * getLedgerValidations
 */

function getLedgerValidations(options) {
  var self = this
  return new Promise(function(resolve, reject) {
    self.getScanWithMarker(self, {
      table: 'validations_by_ledger',
      startRow: options.ledger_hash,
      stopRow: options.ledger_hash + '~',
      marker: options.marker,
      limit: options.limit,
      descending: false
    },
    function(err, resp) {
      if (err) {
        reject(err)

      } else {
        resp.rows.forEach(function(r) {
          if (r.count) {
            r.count = Number(r.count)
          }

          delete r.rowkey
        })

        resolve(resp)
      }
    })
  })
}


/**
 * getValidations
 */

function getValidations(options) {
  var self = this
  var table
  var startRow
  var stopRow

  if (options.pubkey) {
    table = 'validations_by_validator'
    startRow = options.pubkey + '|' + options.start.hbaseFormatStartRow()
    stopRow = options.pubkey + '|' + options.end.hbaseFormatStopRow()

  } else {
    table = 'validations_by_date'
    startRow = options.start.hbaseFormatStartRow()
    stopRow = options.end.hbaseFormatStopRow()
  }

  return new Promise(function(resolve, reject) {
    self.getScanWithMarker(self, {
      table: table,
      startRow: startRow,
      stopRow: stopRow,
      marker: options.marker,
      limit: options.limit,
      descending: options.descending,
      filterString: 'KeyOnlyFilter()'
    },
    function(err, resp) {
      if (err) {
        reject(err)

      } else {
        var keys = []
        resp.rows.forEach(function(r) {
          var parts = r.rowkey.split('|')
          if (options.pubkey) {
            keys.push([
              parts[2],
              parts[0],
              parts[1].substr(0, 14)
            ].join('|'))

          } else {
            keys.push([
              parts[2],
              parts[1],
              parts[0].substr(0, 14)
            ].join('|'))
          }
        })

        self.getRows({
          table: 'validations_by_ledger',
          rowkeys: keys
        }, function(err2, rows) {
          if (err2) {
            reject(err2)

          } else {
            rows.forEach(function(r) {
              r.count = Number(r.count)
              delete r.rowkey
            })

            resp.rows = rows
            resolve(resp)
          }
        })
      }
    })
  })
}

/**
 * getManifests
 */

function getManifests(options) {
  var self = this

  if (options && options.pubkey) {
    return new Promise(function(resolve, reject) {
      self.getScanWithMarker(self, {
        table: 'manifests_by_validator',
        startRow: options.pubkey,
        stopRow: options.pubkey + '|z',
        marker: options.marker,
        limit: options.limit,
        descending: options.descending
      }, function(err, resp) {
        if (err) {
          reject(err)
        } else {
          resp.rows.forEach(function(r) {
            if (r.count) {
              r.count = Number(r.count)
            }

            delete r.rowkey
          })

          resolve(resp)
        }
      })
    })
  }
}

function getNodeStates() {
  return this.getAllRows({
    table: 'node_state'
  })
}

module.exports = {
  getNodeStates: getNodeStates,
  getTopologyNodes: getTopologyNodes,
  getTopologyLinks: getTopologyLinks,
  getValidatorReports: getValidatorReports,
  getLedgerValidations: getLedgerValidations,
  getValidators: getValidators,
  getValidations: getValidations,
  getManifests: getManifests
}
