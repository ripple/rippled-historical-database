var config = require('../config')
var WebHDFS = require('webhdfs')
var hdfs = WebHDFS.createClient(config.get('hdfs'))
var Logger = require('./logger');
var moment = require('moment')
var EPOCH_OFFSET = 946684800

var log = new Logger({
  scope: 'hdfs',
  level: config.get('logLevel') || 3,
  file: config.get('logFile')
})

var ledgerFields = [
  'new_fields',
  'hbase_key',
  'ledger_hash',
  'ledger_index',
  'executed_time',
  'transaction_hash',
  'hash',
  'parent_hash',
  'accepted',
  'closed',
  'account_hash',
  'close_time_resolution',
  'rowkey',
  'close_time_human',
  'total_coins',
  'totalCoins',
  'seqNum',
  'transactions',
  'parent_close_time',
  'close_flags'
]

var txFields = [
  'new_fields',
  'tx_hash',
  'tx_index',
  'ledger_hash',
  'executed_time',
  'ledger_index',
  'Sequence',
  'TransactionType',
  'tx_result',
  'LastLedgerSequence',
  'Account',
  'Destination',
  'TxnSignature',
  'SigningPubKey',
  'Paths',
  'Expiration',
  'InvoiceID',
  'Flags',
  'DestinationTag',
  'SourceTag',
  'Fee',
  'OfferSequence',
  'Amount',
  'TakerPays',
  'TakerGets',
  'LimitAmount',
  'SendMax',
  'fclient',
  'dclient',
  'hash',
  'raw',
  'metaData',
  'meta',
  'Memos',
  'CancelAfter',
  'Channel',
  'Condition',
  'Fulfillment',
  'Owner',
  'PublicKey',
  'SettleDelay',
  'Signature'
]

module.exports.ingestLedgerHeader = function(ledger) {
  var base = 'prod_ledgers/close_date_human='
  var newFields = {}
  var columns = []
  var closeTime = ledger.close_time
  var parentCloseTime = ledger.parent_close_time
  var filename = ledger.ledger_hash + '.txt'

  if (closeTime < EPOCH_OFFSET) {
    closeTime += EPOCH_OFFSET
  }

  if (parentCloseTime < EPOCH_OFFSET) {
    parentCloseTime += EPOCH_OFFSET
  }

  base += moment.unix(closeTime).utc().format('YYYYMMDD') + '/'

  Object.keys(ledger).forEach(function(key) {
    var index = ledgerFields.indexOf(key)

    if (key === 'close_time') {
      columns[ledgerFields.indexOf('executed_time')] = closeTime
      return
    }

    if (key === 'close_time_human') {
      columns[ledgerFields.indexOf('close_time_human')] =
        moment.unix(closeTime).utc().format()
      return
    }

    if (key === 'parent_close_time') {
      columns[index] = parentCloseTime
      return
    }

    if (index === -1) {
      newFields[key] = ledger[key]
      return
    }

    columns[index] = JSON.stringify(ledger[key])
  })

  columns[ledgerFields.indexOf('hbase_key')] = ledger.hash
  columns[ledgerFields.indexOf('rowkey')] = ledger.hash
  columns[0] = JSON.stringify(newFields)

  columns.forEach(function(d, i) {
    if (!d) {
      columns[i] = ''
    }
  })

  return write(base + filename, columns.join('|'))
}

module.exports.ingestTransaction = function(tx) {
  var base = 'prod_transactions/close_date_human='
  var newFields = {}
  var columns = []
  var executed = tx.executed_time
  var filename = tx.hash + '.txt'

  if (executed < EPOCH_OFFSET) {
    executed += EPOCH_OFFSET
  }

  base += moment.unix(executed).utc().format('YYYYMMDD') + '/'

  Object.keys(tx).forEach(function(key) {
    var index = txFields.indexOf(key)

    if (key === 'executed_time') {
      columns[index] = executed
      return
    }

    if (index === -1) {
      newFields[key] = tx[key]
      return
    }

    columns[index] = JSON.stringify(tx[key])
  })

  columns[txFields.indexOf('tx_hash')] = tx.hash
  columns[0] = JSON.stringify(newFields)

  columns.forEach(function(d, i) {
    if (!d) {
      columns[i] = ''
    }
  })

  return write(base + filename, columns.join('|'))
}

function write(path, data) {
  log.debug('saving \'' + path + '\'')
  return new Promise(function(resolve, reject) {
    hdfs.writeFile(path, data, function(err) {
      if (err) {
        reject(err)
      } else {
        log.info('saved \'' + path + '\'')
        resolve()
      }
    })
  })
}
