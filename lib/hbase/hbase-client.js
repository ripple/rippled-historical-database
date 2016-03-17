var Promise = require('bluebird');
var ripple = require('ripple-lib');
var moment = require('moment');
var smoment = require('../smoment');
var utils = require('../utils');
var Hbase = require('./hbase-thrift');
var data = require('./hbase-thrift/data');
var Parser = require('../ledgerParser');
var binary = require('ripple-binary-codec');

var isoUTC = 'YYYY-MM-DDTHH:mm:ss[Z]';
var EPOCH_OFFSET = 946684800;
var LI_PAD       = 12;
var I_PAD        = 5;
var E_PAD        = 3;
var S_PAD        = 12;
var method;

var exchangeIntervals = [
  '1minute',
  '5minute',
  '15minute',
  '30minute',
  '1hour',
  '2hour',
  '4hour',
  '1day',
  '3day',
  '7day',
  '1month',
  '1year'
];

function HbaseClient() {
  Hbase.apply(this, arguments);
}

HbaseClient.prototype = Object.create(Hbase.prototype);
HbaseClient.prototype.constructor = HbaseClient;

for (method in data) {
  HbaseClient.prototype[method] = data[method];
}

module.exports = HbaseClient;
