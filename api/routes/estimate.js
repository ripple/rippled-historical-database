'use strict';

const config = require('../../config')
const Logger = require('../../lib/logger')
const log = new Logger({scope : 'estimate'})
const hbase = require('../../lib/hbase')
const moment = require('moment')
const request = require('request-promise')

let cached = undefined

const fees = {
  bitstamp: {
    exchange: .0025,
    transfer: {
      BTC: 0,
      XRP: 0
    }
  },
  bitso: {
    exchange: .008,
    transfer: {
      BTC: .001,
      XRP: 0
    }
  }
}


module.exports = function(req, res) {
  return validate(req.query)
  .then(() => {
    return Promise.all([
      getEstimate(req.query, 'XRP'),
    ])
  })
  .then(resp => {
    return resp.filter(d => {
      return Boolean(d)
    })
  })
  .then(estimates => {
    return getForex({
      base: req.query.source_currency,
      counter: req.query.destination_currency
    })
    .then(rate => {
      res.send({
        result: 'success',
        estimates: estimates,
        fx_rate: rate || 'unavailable'
      })
    })
  })
  .catch(err => {
    log.error(err.error || err)
    res.status(500).json({
      result: 'error',
      message: err.message || err
    })
  })
}

/**
 * getCachedRate
 */

function getCachedRate(base, counter) {
  let key
  let key2
  let inverse = false

  if (!cached || moment().diff(cached.date, 'minutes') > 60) {
    return undefined
  }

  if (base === 'USD') {
    return cached.quotes[base + counter] || null

  } else if (counter === 'USD') {
    return cached.quotes[counter + base] ?
      1 / cached.quotes[counter + base] : null

  } else if (cached.quotes['USD' + counter] &&
            cached.quotes['USD' + base]) {

    return cached.quotes['USD' + counter] /
      cached.quotes['USD' + base]
  }

  return null
}

/**
 * getRate
 */

function getRate(base) {
  return new Promise((resolve, reject) => {
    hbase.getScan({
      table: 'forex_rates',
      startRow: base,
      stopRow: base + '|z',
      descending: true,
      limit: 1
    }, (err, res) => {
      if (err) {
        reject(err)

      } else if (res[0] && moment().diff(res[0].date, 'minutes') < 120) {
        resolve(res[0])

      } else {
        resolve()
      }
    })
  })
}

/**
 * getForex
 */

function getForex(options) {
  const tasks = []

  if (options.base !== 'USD') {
    tasks.push(getRate('USD|' + options.base))
  }

  if (options.counter !== 'USD') {
    tasks.push(getRate('USD|' + options.counter))
  }

  return Promise.all(tasks)
  .then(res => {
    if (res.length === 1 && res[0]) {
      return options.base === 'USD' ?
        Number(res[0].rate) : 1 / Number(res[0].rate)
    } else if (res.length == 2 && res[0] && res[1]) {
      return Number(res[1].rate) / Number(res[0].rate)
    }
  })

}

/**
 * validate
 */

function validate(options) {
  if (!options.source) {
    return Promise.reject('source exchange required')
  }

  if (!options.destination) {
    return Promise.reject('destination exchange required')
  }

  if (!options.source_currency) {
    return Promise.reject('source currency required')
  }

  if (!options.destination_currency) {
    return Promise.reject('destination currency required')
  }

  if (!options.source_amount) {
    return Promise.reject('source amount required')
  }

  if (isNaN(options.source_amount)) {
    return Promise.reject('source amount is not a number')
  }

  options.source = options.source.toLowerCase()
  options.destination = options.destination.toLowerCase()
  options.source_currency = options.source_currency.toUpperCase()
  options.destination_currency = options.destination_currency.toUpperCase()
  return Promise.resolve()
}

/**
 * getEstimate
 */

function getEstimate(options, intermediary) {
  return Promise.all([
    getBook({
      source: options.source,
      base: intermediary,
      counter: options.source_currency
    }),
    getBook({
      source: options.destination,
      base: intermediary,
      counter: options.destination_currency
    })
  ])
  .then(books => {
    const sourceExchangeFee = fees[options.source] &&
          fees[options.source].exchange ? fees[options.source].exchange : 0
    const destinationExchangeFee = fees[options.destination] &&
          fees[options.destination].exchange ? fees[options.destination].exchange : 0
    const transferFee = fees[options.destination] &&
          fees[options.destination].transfer[intermediary] ?
          fees[options.destination].transfer[intermediary] : 0

    const midpoint1 = (Number(books[0].bids[0].price) +
                       Number(books[0].asks[0].price)) / 2

    const midpoint2 = (Number(books[1].bids[0].price) +
                       Number(books[1].asks[0].price)) / 2

    const adjusted1 = options.source_amount * (1 - sourceExchangeFee)
    const res1 = getLeg(books[0].asks, 'buy', adjusted1)
    const adjusted2 = res1.proceeds * (1 - destinationExchangeFee) -
          transferFee
    const res2 = getLeg(books[1].bids, 'sell', adjusted2)

    const result = {
      intermediary: intermediary,
      amount: options.source_amount,
      proceeds: res2.proceeds,
      rate: res2.proceeds / options.source_amount,
      midpoint: midpoint2 / midpoint1
    }

    result.bps = Math.abs(Math.ceil((result.rate /
                                     result.midpoint - 1) * 10000))

    return result
  })
  .catch(err => {
    if (err.message === 'orderbook unavailable' ||
        err.message === 'insufficient liquidity' ||
        err.message === 'stale orderbook') {
      log.info(err.message)
      return undefined
    }

    throw err
  })
}

/**
 * getBook
 */

function getBook(options) {
  return new Promise((resolve, reject) => {
    const start = [
      options.source,
      options.base,
      options.counter
    ].join('|')

    hbase.getScan({
      table: 'external_orderbooks',
      startRow: start,
      stopRow: start + '|z',
      limit: 1
    },
    function(err, resp) {
      if (err) {
        reject(err)
        return
      }

      if (!resp.length) {
        log.info(options.source + ' ' +
                 options.base + options.counter +
                 ' orderbook unavailable')
        reject(new Error('orderbook unavailable'))
        return
      }

      if (moment().diff(resp[0].timestamp, 'minutes') > 5) {
        log.info(options.source + ' ' +
                 options.base + options.counter +
                 ' stale orderbook')
        reject(new Error('stale orderbook'))
        return
      }

      resolve({
        bids: JSON.parse(resp[0].bids),
        asks: JSON.parse(resp[0].asks)
      })
    })
  })
}

/**
 * getLeg
 */

function getLeg(offers, direction, amount) {
    const result = {
      amount: amount,
      proceeds: 0,
    }

    let remaining = amount

    offers.every((offer, i) => {
      const quantity = offer.price * offer.amount

      if (direction === 'buy') {
        if (quantity <= remaining) {
          result.proceeds += offer.amount
          remaining -= quantity
          return true

        } else {
          result.proceeds += remaining / offer.price
          result.avg_rate = amount / result.proceeds
          result.max = offer.price
          result.consumed_offers = i + 1
          remaining = 0
          return false
        }
      } else if (offer.amount <= remaining) {
        result.proceeds += quantity
        remaining -= offer.amount
        return true

      } else {
        result.proceeds += remaining * offer.price
        result.avg_rate = result.proceeds / amount
        result.max = offer.price
        result.consumed_offers = i + 1
        remaining = 0
        return false
      }
    })

  if (remaining) {
    throw Error('insufficient liquidity')
  }

  return result
}
