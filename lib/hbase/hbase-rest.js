'use strict'

var Promise = require('bluebird')
var HBaseRest = require('hbase')
var Logger = require('../logger')

var tables = {
  ledgers: [
    'ledgers',
    'transactions',
    'exchanges',
    'payments',
    'payments_by_currency',
    'balance_changes',
    'account_exchanges',
    'account_offers',
    'account_payments',
    'accounts_created',
    'network_fees',
    'memos',
    'lu_ledgers_by_index',
    'lu_ledgers_by_time',
    'lu_transactions_by_time',
    'lu_account_transactions',
    'lu_affected_account_transactions',
    'lu_account_offers_by_sequence',
    'lu_account_memos',
    'agg_payments',
    'agg_exchanges',
    'agg_metrics',
    'agg_stats',
    'agg_account_stats',
    'agg_account_balance_changes',
    'agg_account_payments',
    'agg_account_exchanges',
    'xrp_distribution',
    'top_markets',
    'top_currencies',
    'issuer_balance_snapshot',
    'fee_stats',
    'rippled_versions',
    'control',
    'crawl_node_stats',
    'crawls',
    'connections',
    'node_state',
    'nodes',
    'escrows',
    'account_escrows',
    'payment_channels',
    'account_payment_channels'
  ],
  validations: [
    'validations_by_ledger',
    'validations_by_validator',
    'validations_by_date',
    'validator_reports',
    'validators',
    'validator_domain_changes',
    'manifests_by_validator',
    'manifests_by_master_key'
  ]
}

function Client(options) {
  var prefix = options.prefix || ''
  var rest = new HBaseRest(options)
  var log = new Logger({
    scope: 'hbase-rest',
    level: options.logLevel,
    file: options.logFile
  })

  /**
   * initTables
   * create tables and column families
   * if they do not exits
   */

  this.initTables = function(group, done) {
    var self = this

    Promise.map(tables[group], function(table) {
      return self.addTable(table, group === 'validations')
    })
    .nodeify(function(err, resp) {

      if (err) {
        log.error('Error configuring tables:', err)
      } else {
        log.info(group, 'tables configured')
      }

      if (done) {
        done(err, resp)
      }
    })
  }

  this.removeTables = function(group, done) {
    var self = this

    Promise.map(tables[group], function(table) {
      return self.removeTable(table)
    })
    .nodeify(function(err, resp) {

      if (err) {
        log.error('Error removing tables:', err)
      } else {
        log.info(group, 'tables removed')
      }

      if (done) {
        done(err, resp)
      }
    })
  }

  /**
   * addTable
   * add a new table to HBase
   */

  this.addTable = function(table, includeIncrement) {
    var families = ['f', 'd']

    if (table === 'agg_stats' || table === 'agg_account_stats') {
      families.push('type', 'result', 'metric')
    }

    if (includeIncrement) {
      families.push('inc')
    }

    return new Promise(function(resolve, reject) {
      var schema = []
      families.forEach(function(family) {
        schema.push({
          name: family
        })
      })

      rest.table(prefix + table)
      .create({
        ColumnSchema: schema
      },
      function(err, resp) {
        if (err) {
          reject(err)

        } else {
          log.info('created table:', prefix + table)
          resolve(resp)
        }
      })
    })
  }

  /**
   * removeTable
   * remove a table from HBase
   */

  this.removeTable = function(table) {
    return new Promise(function(resolve, reject) {
      rest.table(prefix + table)
      .delete(function(err, resp) {
        if (err && err.code === 404) {
          log.info('table not found:', prefix + table)
          resolve()

        } else if (err) {
          reject(err)

        } else {
          log.info('removed table:', prefix + table)
          resolve(resp)
        }
      })
    })
  }
}

module.exports = function(options) {
  return new Client(options)
}
