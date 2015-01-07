var winston = require('winston');
var config  = require('../config/import.config');

var Log = function (scope) {
  var silent;
  var level = 3;
  var self = this;
  var transports;
  
  //for storm we will log everything to a
  //file, including console logging
  if (config.get('logToFile')) {
    transports = [new (winston.transports.File)({ filename: 'nodejs.log' })];
    
    //replace console.log function
    console.log = function() {
      if (level) {
        log('console', arguments);
      }
    };
    
  } else {
    transports = [new (winston.transports.Console)({level:'debug'})]  
  }

  this.winston = new (require('winston').Logger)({
    transports: transports 
  });
  
  this.level = function (l) {
    level = parseInt(l, 10);
  };
  
  function log(type, args) {
    args = [].concat.apply({},args).slice(1);
    args.unshift(scope.toUpperCase()+":");
    self.winston[type].apply(this, args);    
  }

  this.debug = function () {
    if (level<4) return;
    log('debug', arguments);
  };
    
  this.info = function () {
    if (level<3) return;
    log('info', arguments);
  };

  this.warn = function () {
    if (level<2) return;
    log('warn', arguments);
  };  
  
  this.error = function () {
    if (level<1) return;
    log('error', arguments);
  };  
  
};

module.exports = function (scope) {
  return new Log(scope);
};
