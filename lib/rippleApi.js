var config = require('../config')
var ripple = require('ripple-lib')
var rippleAPI = new ripple.RippleAPI(config.get('ripple'))

rippleAPI.connect()
.then(function() {
  console.log('ripple API connected.')
})
.catch(function(e) {
  console.log(e)
})

rippleAPI.on('error', function(errorCode, errorMessage, data) {
  console.log(errorCode, errorMessage, data)
})

module.exports = rippleAPI
