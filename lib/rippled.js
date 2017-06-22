var config = require('../config')
var request = require('request-promise')
var url = 'http://' +
    config.get('ripple:server').split('//')[1].split(':')[0] +
    ':51234'

/**
 * getBalances
 */

module.exports.getBalances = function(options) {

  return Promise.all([
    getXrpBalance(options),
    getLines(options)
  ])
  .then(function(resp) {
    resp[1].unshift(resp[0])
    return resp[1]
  })
}

/**
 * getXrpBalance
 */

function getXrpBalance(options) {
  var params = {
    method: 'account_info',
    params: [{
      account: options.account,
      ledger_index: options.ledger
    }]
  }

  return request.post({
    url: url,
    json: params,
    timeout: 5000
  })
  .then(function(resp) {
    if (resp.result.error_message) {
      throw new Error(resp.result.error_message)
    }

    return {
      currency: 'XRP',
      value: resp.result.account_data.Balance / 1000000
    }
  })
}

/**
 * getLines
 */

function getLines(options) {
  var limit
  var params = {
    method: 'account_lines',
    params: [{
      account: options.account,
      ledger_index: options.ledger,
      limit: options.limit - 1,
      peer: options.counterparty
    }]
  }

  return request.post({
    url: url,
    json: params,
    timeout: 5000
  })
  .then(function(resp) {

    if (resp.result.error_message) {
      throw new Error(resp.result.error_message)
    }

    var result = []
    resp.result.lines.forEach(function(d) {
      result.push({
        currency: d.currency,
        counterparty: d.account,
        value: d.balance
      })
    })

    return result
  })
}
