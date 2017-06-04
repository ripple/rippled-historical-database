var Hbase = require('./hbase-thrift')
var data = require('./hbase-thrift/data')
var topology = require('./hbase-thrift/topology')
var config = require('../../config')

function HbaseClient() {
  Hbase.apply(this, arguments)
}

HbaseClient.prototype = Object.create(Hbase.prototype)
HbaseClient.prototype.constructor = HbaseClient

for (method in data) {
  HbaseClient.prototype[method] = data[method]
}

for (method in topology) {
  HbaseClient.prototype[method] = topology[method]
}

module.exports = new HbaseClient(config.get('hbase'))
