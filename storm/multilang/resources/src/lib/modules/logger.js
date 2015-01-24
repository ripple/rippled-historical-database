var winston = require('winston');

var Log = function (options) {
  var self  = this;  
  var level = options.level || 3;
  var scope = options.scope || null;
  var transports;
  
  //for storm we will log everything to a
  //file, including console logging
  if (options.file) {
    transports = [new (winston.transports.File)({ filename: options.file })];
    
    //replace console.log function
    console.log = function() {
      var args;
      
      if (level) {
        args = Array.prototype.slice.call(arguments);
        args.unshift("CONSOLE");
        log('info', args);
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
    args = Array.prototype.slice.call(args);
    if (scope) args.unshift(scope.toUpperCase()+":");
    args.push('');
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

module.exports = Log
