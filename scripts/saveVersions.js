var Promise = require('bluebird');
var smoment = require('../lib/smoment');
var exec = require('child_process').exec;

function run(hbase) {

/**
 * install
 */

function install() {
  return new Promise(function(resolve, reject) {
    exec(__dirname + '/versionsSetup.sh', function callback(err, stdout, stderr) {
      if (err) {
        reject(err);
      } else {
        resolve();
      }
    });
  });
}

/**
 * getVersions
 */

function getVersions() {
  return new Promise(function(resolve, reject) {
    exec(__dirname + '/getVersions.sh', function callback(err, stdout, stderr) {
      if (err) {
        console.log(err)
        console.log(stdout)
        console.log(stderr);
        reject(err);
      } else {
        resolve(JSON.parse(stdout));
      }
    });
  });
}

/**
 * saveVersions
 */

function saveVersions(data) {
  var rows = [];
  var date = smoment();
  date.moment.startOf('day');

  for (var key in data) {
    rows.push(hbase.putRow({
      table: 'rippled_versions',
      rowkey: date.hbaseFormatStartRow() + '|' + key,
      columns: {
        date: date.format(),
        repo: key,
        version: data[key]
      }
    }));
  }

  return Promise.all(rows);
}

  return getVersions()
  .then(saveVersions)
  .then(function() {
    console.log('versions saved');
  });
}

module.exports = run;

if (require.main === module) {

var config = require('../config/import.config');
var Hbase = require('../lib/hbase/hbase-client');
var hbaseOptions = config.get('hbase');
var hbase = new Hbase(hbaseOptions);

  run(hbase)
  .then(function() {
    process.exit();
  })
  .catch(function(e) {
    console.log(e);
    console.log(e.stack);
    process.exit(1);
  });
}
