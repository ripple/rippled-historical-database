var config = require('../config')
var BigNumber = require('bignumber.js')
var request = require('request-promise')
var sellFlag = 0x00020000
var url = 'http://' +
    config.get('ripple:server').split('//')[1].split(':')[0] +
    ':51234'

/**
 * getBalances
 */

module.exports.getBalances = function(options) {
  return getXrpBalance(options)
  .then(function(xrpBalance) {
    return getLines(options)
    .then(function(balances) {
      balances.unshift(xrpBalance)
      return balances
    })
  })
}

module.exports.getOrders = function(options) {
  var params = {
    method: 'account_offers',
    params: [{
      account: options.account,
      ledger_index: options.ledger,
      limit: options.limit - 1,
    }]
  }

  return request.post({
    url: url,
    json: params,
    timeout: 5000
  }).then(function(resp) {

    if (resp.result.error_message) {
      throw new Error(resp.result.error_message)
    }

    var orders = []
    resp.result.offers.forEach(function(d) {
      orders.push(formatOrder(options.account, d))
    })

    return orders
  })
}

/**
 * adjustQualityForXRP
 */

function adjustQualityForXRP(options) {
  const numeratorShift = (options.pays === 'XRP' ? -6 : 0)
  const denominatorShift = (options.gets === 'XRP' ? -6 : 0)
  const shift = numeratorShift - denominatorShift
  return shift === 0 ? options.quality :
    (new BigNumber(options.quality)).shift(shift).toString()
}

/**
 * parseAmount
 */

function parseAmount(d) {
  return typeof d === 'object' ? d : {
    currency: 'XRP',
    value: (d / 1000000).toString()
  }
}

/**
 * formatOrder
 */

function formatOrder(account, d) {
  var direction = (d.flags & sellFlag) === 0 ? 'buy' : 'sell'
  var gets = parseAmount(d.taker_gets)
  var pays = parseAmount(d.taker_pays)
  var quantity = direction === 'buy' ? pays : gets
  var price = direction === 'buy' ? gets : pays

  return {
    specification: {
      direction: 'buy',
      quantity: {
        currency: quantity.currency,
        value: quantity.value,
        counterparty: quantity.issuer
      },
      totalPrice: {
        currency: price.currency,
        value: price.value,
        counterparty: price.issuer
      }
    },
    properties: {
      maker: account,
      flags: d.flags,
      sequence: d.seq,
      makerExchangeRate: adjustQualityForXRP({
        quality: d.quality,
        gets: gets.currency,
        pays: pays.currency
      })
    }
  }
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
