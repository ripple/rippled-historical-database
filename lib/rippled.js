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
  .then(function(resp) {

    if (options.currency && options.currency === 'XRP') {
      return {
        ledger_index: resp.ledger_index,
        balances: [{
          currency: 'XRP',
          value: resp.balance
        }]
      }
    }

    options.ledger = resp.ledger_index

    return getLines(options)
    .then(function(balances) {
      balances.unshift({
        currency: 'XRP',
        value: resp.balance
      })

      return {
        ledger_index: resp.ledger_index,
        balances: balances.filter(function(d) {
          if (options.currency &&
             options.currency !== d.currency) {
            return false
          }

          return true
        }).slice(0, options.limit)
       }
    })
  })
}

module.exports.getOrders = function(options) {
  var orders = []
  var count = options.limit || Infinity

  function getOrdersRecursive(marker) {
    var size = 100

    var params = {
      method: 'account_offers',
      params: [{
        account: options.account,
        ledger_index: options.ledger,
        limit: size,
        marker: marker
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

      orders.push.apply(orders, resp.result.offers)
      count -= size

      if (count > 0 && resp.result.marker) {
        return getOrdersRecursive(resp.result.marker)
      } else {
        var result = []
        orders.forEach(function(d) {
          result.push(formatOrder(options.account, d))
        })


        return {
          ledger_index: resp.result.ledger_index || resp.result.ledger_current_index,
          orders: result.slice(0, options.limit)
        }
      }
    })
  }


  return getOrdersRecursive()
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
      ledger_index: resp.result.ledger_index || resp.result.ledger_current_index,
      balance: (resp.result.account_data.Balance / 1000000).toString()
    }
  })
}

/**
 * getLines
 */

function getLines(options) {

  var offset = options.currency ? 0 : 1
  var count = options.limit ?
      options.limit - offset : Infinity
  var lines = []

  function getLinesRecursive(marker) {
    var size = 400

    var params = {
      method: 'account_lines',
      params: [{
        account: options.account,
        ledger_index: options.ledger,
        limit: size,
        peer: options.counterparty,
        marker: marker
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

      lines.push.apply(lines, resp.result.lines)
      count -= size
      count -= size
      if (count > 0 && resp.result.marker) {
        return getLinesRecursive(resp.result.marker)
      } else {
        return lines
      }
    })
  }

  return getLinesRecursive()
  .then(function(resp) {

    var result = []
    resp.forEach(function(d) {
      result.push({
        currency: d.currency,
        counterparty: d.account,
        value: d.balance
      })
    })

    return result
  })
}
