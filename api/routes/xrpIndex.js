'use strict';

const config = require('../../config')
const Logger = require('../../lib/logger')
const log = new Logger({scope : 'xrp index'})
const hbase = require('../../lib/hbase')
const smoment = require('../../lib/smoment')
const moment = require('moment')
const request = require('request-promise')
const utils = require('../../lib/utils')

const intervals = [
  '5minute',
  '15minute',
  '30minute',
  '1hour',
  '2hour',
  '4hour',
  '1day'
]

const DATE_FORMAT = 'YYYYMMDDHHmmss';
const MAX_TIME = 99999999999999;
const invertTimestamp = timestamp => MAX_TIME - timestamp;
const getInverseTimestamp = date => MAX_TIME - Number(date.format(DATE_FORMAT));

const formatResults = (data, rates) => {
  const rows = [];

  data.rows.forEach((row, i) => {
    const rate = rates[i];
    if (row.midpoint) {
      rows.push({
        price: (row.midpoint * rate).toPrecision(6),
        volume: row.volume,
        counter_volume: (row.volume * row.midpoint * rate).toString(),
        count: Number(row.count || 0),
        date: row.date
      })

    } else {
      rows.push({
        open: row.open && (row.open * rate).toPrecision(6),
        high: row.high && (row.high * rate).toPrecision(6),
        low: row.low && (row.low * rate).toPrecision(6),
        close: row.close && (row.close * rate).toPrecision(6),
        vwap: row.vwap && (row.vwap * rate).toPrecision(6),
        volume: row.volume,
        counter_volume: (row.usd_volume * rate).toString(),
        count: Number(row.count || 0),
        date: row.date
      })
    }
  });

  return {
    rows,
    marker: data.marker
  };
}

const getRates = (data, currency) => {
  const tasks = [];

  data.rows.forEach(row => {
    const { rowkey } = row;
    const timestamp = invertTimestamp(rowkey.split('|')[1]);
    tasks.push(getFXRate({ currency, timestamp }));
  });

  return Promise.all(tasks);
};

const getFXRate = options => {
  const { currency, timestamp } = options;
  const base = `USD|${currency}|`;

  if (currency === 'USD') {
    return Promise.resolve(1);
  }

  return new Promise((resolve, reject) => {
    hbase
      .getScan({
        table: 'forex_rates',
        startRow: `${base}${timestamp}`,
        stopRow: `${base}`,
        columns: ['d:rate'],
        descending: true,
        excludeMarker: true,
        limit: 1
      }, (err, resp) => {
        if (err) {
          reject(err);
        } else {
          resolve((resp[0] ? parseFloat(resp[0].rate) : 0));
        }
      });
  });
};


module.exports = function(req, res) {
  return validate(req.query)
  .then(options => {
    return getIndex(options)
    .then(data => {
      return getRates(data, options.currency)
      .then(rates => {
        return formatResults(data, rates);
      });
    });
  })
  .then(result => {

    if (result.marker) {
      utils.addLinkHeader(req, res, result.marker);
    }

    res.send({
      result: 'success',
      count: result.rows.length,
      rows: result.rows,
      marker: result.marker
    })
  })
  .catch(err => {
    log.error(err.error || err)
    res.status(err.code || 500).json({
      result: 'error',
      message: err.error || err
    })
  })
}

/**
 * validate
 */

function validate(params) {

  const options = {
    start: smoment(params.start || '2013-01-01'),
    end: smoment(params.end),
    interval: (params.interval || '').toLowerCase(),
    currency: (params.currency || 'USD').toUpperCase(),
    limit: params.limit,
    marker: params.marker,
    ascending: (/true/i).test(params.descending) ? false : true,
  }

  if (!options.start) {
    return Promise.reject({
      error: 'invalid start date format',
      code: 400
    })
  }

  if (!options.end) {
    return Promise.reject({
      error: 'invalid end date format',
      code: 400
    })
  }

  if (options.interval && intervals.indexOf(options.interval) === -1) {
    return Promise.reject({
      error: 'invalid interval',
      code: 400
    })
  }

  if (isNaN(options.limit)) {
    options.limit = 200;

  } else if (options.limit > 400) {
    options.limit = 400;
  }

  return Promise.resolve(options)
}

/**
 * getIndex
 */

function getIndex(options, rate) {
  return new Promise((resolve, reject) => {
    const table = options.interval ?
      'agg_xrp_index' : 'xrp_index'

    const base = options.interval ?
      options.interval + '|' : ''
    const stop = getInverseTimestamp(options.start)
    const start = getInverseTimestamp(options.end)

    hbase.getScanWithMarker(hbase, {
      table: table,
      startRow: base + start,
      stopRow: base + stop,
      limit: options.limit,
      marker: options.marker,
      descending: !options.ascending,
      columns: [
        'd:midpoint',
        'd:volume',
        'd:usd_volume',
        'd:count',
        'd:open',
        'd:high',
        'd:low',
        'd:close',
        'd:vwap',
        'd:date',
        'f:date'
      ]
    },
    function(err, resp) {
      if (err) {
        reject(err)
      } else {
        resolve(resp)
      }
    })
  })
}

