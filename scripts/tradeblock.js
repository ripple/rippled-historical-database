'use strict'
var config = require('../config/import.config')
var WebSocket = require('ws')
var WebHDFS = require('webhdfs')
var hdfs = WebHDFS.createClient(config.get('hdfs'))
var moment = require('moment')

var url = 'wss://clientapi.tradeblock.com/json/RPbb09aee6'
var connection
var buffer = {}
var last

/**
 * checkStatus
 */

function checkStatus() {
  var time = moment.utc()
  .startOf('hour')
  .subtract(1, 'hour')

  var filename = 'file_date=' + time.format('YYYYMMDD') +
  '/XBT|USD/STMP/' + time.format('HH') + '.csv'

  console.log('checking for file: ' + filename)

  hdfs.exists(filename, function(exists) {
    console.log(filename + (exists ? ' found' : ' not found'))
    process.exit(exists ? 0 : 1)
  })
}

/**
 * appendFile
 */

function appendFile(pair, market, time, data) {
  var filename = 'file_date=' + time.format('YYYYMMDD') +
  '/' + pair.replace('/', '|') +
  '/' + market +
  '/' + time.format('HH') + '.csv'
  console.log('appending file: ' + filename)

  hdfs.exists(filename, function(exists) {
    var action = exists ? 'appendFile' : 'writeFile'
    hdfs[action](filename, data, {}, function(err) {
      if (err) {
        console.error(err)
        return
      }
    })
  })
}

/**
 * saveBuffer
 */

function saveBuffer(time) {
  var market
  var pair

  console.log('save buffer')

  for (pair in buffer) {
    for (market in buffer[pair]) {
      appendFile(pair, market, time, buffer[pair][market])
      delete buffer[pair][market]
    }
  }

  last = moment(time).startOf('minute')
}

/**
 * bufferOrderbookData
 */

function bufferOrderbookData(json) {

  var time = moment.unix(json.asof).utc()
  var minute = moment(time).startOf('minute')
  var price
  var size
  var i

  if (!buffer[json.pair]) {
    buffer[json.pair] = {}
  }

  if (!buffer[json.pair][json.market]) {
    buffer[json.pair][json.market] = ''
  }

  for (i = 0; i < json.bids.length; i++) {
    price = json.bids[i][0]
    size = json.bids[i][1]

    buffer[json.pair][json.market] += 'bids,' +
      json.asof + ',' +
      price + ',' +
      size + ',' +
      json.type + '\n'
  }

  for (i = 0; i < json.asks.length; i++) {
    price = json.asks[i][0]
    size = json.asks[i][1]

    buffer[json.pair][json.market] += 'asks,' +
      json.asof + ',' +
      price + ',' +
      size + ',' +
      json.type + '\n'
  }

  if (!last) {
    last = minute

  } else if (minute.diff(last)) {
    saveBuffer(time)
  }
}

if (config.get('check')) {
  checkStatus()
  return
}

connection = new WebSocket(url)
connection.onopen = function() {
  console.log('started at: ' + new Date())
  var message = {
    action: 'subscribe',
    channel: 'orderbooks'
  }
  connection.send(JSON.stringify(message))
}

connection.onerror = function(error) {
  console.log(error)
  console.log('Connection error ' + new Date())
  process.exit()
}

connection.onmessage = function(message) {
  var json

  try {
    json = JSON.parse(message.data)
  } catch (e) {
    console.log('invalid JSON: ', message.data)
    return
  }


  if (json.type === 3) {
    bufferOrderbookData(json)
  }
}

