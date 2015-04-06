'use strict';

var winston = require('winston');
var moment = require('moment');

var Log = function(options) {
  var self = this;
  var level = options.level || 3;
  var scope = options.scope || null;
  var transports;

  /**
   * log
   */

  function log(type, args) {
    args = Array.prototype.slice.call(args);

    if (scope) {
      args.unshift(scope.toUpperCase());
    }

    args.unshift('[' + moment.utc().format('YYYY-MM-DD HH:mm:ss.SSS') + ']');
    args.push('');
    self.winston[type].apply(this, args);
  }

  // for storm we will log everything to a
  // file, including console logging
  if (options.file) {
    transports = [new (winston.transports.File)({filename: options.file})];

    // replace console.log function
    console.log = function() {
      var args;

      if (level) {
        args = Array.prototype.slice.call(arguments);
        args.unshift('CONSOLE');
        log('info', args);
      }
    };

  } else {
    transports = [new (winston.transports.Console)({level: 'debug'})];
  }

  this.winston = new (require('winston').Logger)({
    transports: transports
  });

  this.level = function(l) {
    level = parseInt(l, 10);
  };

  this.debug = function() {
    if (level > 3) {
      log('debug', arguments);
    }
  };

  this.info = function() {
    if (level > 2) {
      log('info', arguments);
    }
  };

  this.warn = function() {
    if (level > 1) {
      log('warn', arguments);
    }
  };

  this.error = function() {
    if (level) {
      log('error', arguments);
    }
  };
};

module.exports = Log;

