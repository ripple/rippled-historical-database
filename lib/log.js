var winston = require('winston');

var Log = function (scope) {
  var silent;
  var level = 4;
  
  this.level = function (l) {
    level = parseInt(l, 10);
  };
  
  function log(type, args) {
    args = [].concat.apply({},args).slice(1);
    args.unshift(scope.toUpperCase()+":");
    winston[type](args.join(" "));    
  }
  
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
