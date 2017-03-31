/* eslint no-unused-vars:0 */
'use strict'

var config = require('../config/api.config')
var Server = require('./server')
var options = {
  hbase: config.get('hbase'),
  ripple: config.get('ripple'),
  port: config.get('port'),
  cacheControl: config.get('cacheControl')
}

var server = new Server(options)

