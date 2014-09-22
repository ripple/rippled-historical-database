module.exports = function (config) {
  return require('nano')({
    url : config.protocol +
      '://' + config.username + 
      ':'   + config.password + 
      '@'   + config.host + 
      ':'   + config.port + 
      '/'   + config.database,
    request_defaults : {timeout :90 * 1000}, //30 seconds max for couchDB 
  });  
};
