'use strict';

var request = require('request-promise');
var Promise = require('bluebird');
var path = require('path');
var config = require('../config');
var hbase = require('../lib/hbase');
var spawn = require('child_process').spawn;
var nodemailer = require('nodemailer');
var transporter = nodemailer.createTransport();
var to = config.get('recipients');
var interval = 5 * 60 * 1000;

// get topology status
var getTopologyStatus = function() {
  return request({
    url: 'http://localhost:8772/api/v1/topology/summary',
    json: true
  })
  .then(function(resp) {
    var topology;
    resp.topologies.every(function(t) {
      if (t.name === 'ripple-ledger-importer') {
        topology = t;
        return false;
      }

      return true;
    });

    return topology ? topology.status : 'NOT FOUND';
  });
};

// get ledger status
var getLedgerStatus = function() {
  return new Promise(function(resolve, reject) {
    hbase.getLedger({}, function(err, ledger) {
      if (err) {
        reject(err);
        return;
      }

      var now = Date.now();
      var gap = ledger ? (now - ledger.close_time * 1000) / 1000 : Infinity;
      resolve(gap);
    });
  });
};


function checkStatus() {
  Promise.all([
    getTopologyStatus(),
    getLedgerStatus()
  ])
  .then(function(resp) {
    if (resp[0] !== 'ACTIVE' ||
        resp[1] > 60 * 5) {
      notify(resp);
      restartTopology();
    }
  }).catch(function(e) {
    console.log(e);
  });
}

console.log('checking status every ' + interval / 1000 + ' seconds');
setInterval(checkStatus, interval);

/**
 * restartTopology
 */

function restartTopology() {
  var script = path.resolve(__dirname + '/../storm/production/importer.sh');
  var prc = spawn(script, ['restart']);

  prc.stdout.setEncoding('utf8');
  prc.stdout.on('data', function(data) {
    console.log(data);
  });

  prc.on('close', function(code) {
    console.log('process exit code ' + code);
  });
}

/**
 * notify
 */

function notify(data) {
  var message;

  console.log('restarting topology:', data);

  if (data[0] !== 'ACTIVE') {
    message = 'the importer script is not active: ' + data[0];
  } else {
    message = 'the last ledger was imported ' +
      'more than 5 minutes ago: ' + data[1];
  }

  var params = {
    from: 'Storm Import<storm-import@ripple.com>',
    to: to,
    subject: 'restarting topology',
    html: 'The import topology is being restarted: <br /><br />\n' +
      '<blockquote><pre>' + message + '</pre></blockquote><br />\n'
  };

  transporter.sendMail(params, function(err, info) {
    if (err) {
      console.log(err);
    } else {
      console.log('Notification sent: ', info.accepted);
    }
  });
}
