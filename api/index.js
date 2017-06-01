/* eslint no-unused-vars:0 */
'use strict'

var config = require('../config')
var Server = require('./server')
var options = {
  port: config.get('port'),
  cacheControl: config.get('cacheControl')
}

var server = new Server(options)

