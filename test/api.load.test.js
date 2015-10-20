var config   = require('../config/api.config');
var Promise  = require('bluebird');
var request  = require('request');
var Postgres = require('../import/postgres/client');
var Server   = require('../api/server');
var dbConfig = config.get('postgres');
var hbConfig = config.get('hbase');
var moment   = require('moment');
var http     = require('http');
var https    = require('https');

var port     = 7111;
var server;
var db;

http.globalAgent.maxSockets = https.globalAgent.maxSockets = 10000;

db     = new Postgres(dbConfig);
server = new Server({
  postgres : dbConfig,
  hbase    : hbConfig,
  port     : port,
});

var start = moment.utc().startOf('day');
var end   = moment.utc(start).add(1, 'day');

function getPayments (params, callback) {
  var d = Date.now();
  var url = 'http://localhost:' + port + '/v1/accounts/' + params.account + '/payments';
  request({
    url: url,
    json: true,
    qs : {
      limit : params.limit,
      start : params.start,
      end   : params.end,
    }
  },
  function (err, res, body) {
    d = Date.now() - d;
    console.log(err, params.start, body.count, (d/1000)+'s');
    callback(err, body);
  });
}


var count = 500;
var i = 0;
var params = [];
while(i++<count) {

  params.push({
    account : 'rMwjYedjc7qqtKYVLiAccJSmCwih4LnE2q',
    start   : start.format(),
    end     : end.format()
  });

  params.push({
    account : 'rvYAfWj5gh67oV6fW32ZzP3Aw4Eubs59B',
    start   : start.format(),
    end     : end.format()
  });

  start.subtract(1, 'day');
  end.subtract(1, 'day');
}

Promise.map(params, function (p, i) {
  return new Promise(function(resolve, reject) {
    setTimeout(function() {
      getPayments(p, function(err, resp) {
        if (err) reject (err);
        else resolve (true);
      });
    }, 10*i);
  });

}).nodeify(function(err, resp) {
  console.log(err, 'done');
  getPayments(params[0], function(){});
});



