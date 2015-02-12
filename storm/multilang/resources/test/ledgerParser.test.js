var Parser = require('../src/lib/modules/ledgerParser');
var fs     = require('fs');
var assert = require('assert');

var path         = __dirname + '/transactions/';
var EPOCH_OFFSET = 946684800;

/*
var tx = JSON.parse(fs.readFileSync(path + 'demmurage-IOU.json', "utf8"));
var parsed;

tx.metaData = tx.meta;
tx.executed_time = tx.date + EPOCH_OFFSET;
parsed = Parser.parseTransaction(tx);

console.log(parsed.exchanges);

tx = JSON.parse(fs.readFileSync(path + 'demmurage-XRP.json', "utf8"));
tx.metaData = tx.meta;
tx.executed_time = tx.date + EPOCH_OFFSET;
parsed = Parser.parseTransaction(tx);

console.log(parsed.exchanges);
*/

tx = JSON.parse(fs.readFileSync(path + 'demmurage-XRP2.json', "utf8"));

tx.metaData = tx.meta;
tx.executed_time = tx.date + EPOCH_OFFSET;
parsed = Parser.parseTransaction(tx);

console.log(parsed);

